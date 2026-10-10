"""Read a source file as text and standardise it.

Every column is read as a string so Pandas never converts anything implicitly (money to float,
guessed date formats). Standardisation then adds, next to the untouched raw columns:

* ``std_<column>``  canonical text used for output, keys and payload comparison
* ``_txn_date``, ``_txn_time``, ``_debit``, ``_credit``, ``_balance``  parsed values (None if invalid)
* ``_<amount>_present``  whether the raw amount cell had any value
"""
from __future__ import annotations

from datetime import date, datetime, time
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation

import pandas as pd

from .config import Config
from .ingest import SourceFile

LINEAGE_COLUMNS = ["source_file", "source_row_number", "file_date", "file_sha256"]
AMOUNT_COLUMNS = ("debit", "credit", "balance")


class SchemaError(Exception):
    """The file cannot be parsed or its header does not match the configured schema."""


def read_source_file(sf: SourceFile, cfg: Config) -> pd.DataFrame:
    try:
        df = pd.read_csv(
            sf.path,
            dtype=str,
            keep_default_na=False,
            skip_blank_lines=False,  # a blank line is a received (and rejected) record, not silently dropped
            encoding=cfg.encoding,
            sep=cfg.delimiter,
        )
    except (pd.errors.ParserError, pd.errors.EmptyDataError, UnicodeDecodeError) as exc:
        raise SchemaError(f"cannot parse {sf.name}: {exc}") from exc

    df = df.rename(columns=lambda c: str(c).strip()).rename(columns=cfg.column_map)
    missing = [c for c in cfg.columns if c not in df.columns]
    unexpected = [c for c in df.columns if c not in cfg.columns]
    if missing or unexpected:
        raise SchemaError(f"header mismatch in {sf.name}: missing={missing} unexpected={unexpected}")

    df = df.loc[:, list(cfg.columns)].fillna("").astype(str)
    df["source_file"] = sf.name
    df["source_row_number"] = range(2, len(df) + 2)  # line number in the file; line 1 is the header
    df["file_date"] = sf.file_date.isoformat()
    df["file_sha256"] = sf.sha256
    return df.reset_index(drop=True)


def parse_date(value: str, formats: tuple[str, ...]) -> date | None:
    for fmt in formats:
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    return None


def parse_time(value: str, fmt: str) -> time | None:
    try:
        return datetime.strptime(value, fmt).time()
    except ValueError:
        return None


def parse_amount(value: str, scale: int) -> Decimal | None:
    if not value:
        return None
    try:
        amount = Decimal(value)
    except InvalidOperation:
        return None
    if not amount.is_finite():
        return None
    return amount.quantize(Decimal(1).scaleb(-scale), rounding=ROUND_HALF_UP)


def format_amount(amount: Decimal | None, scale: int) -> str:
    return "" if amount is None else f"{amount:.{scale}f}"


def _objects(values, index) -> pd.Series:
    return pd.Series(list(values), index=index, dtype=object)


def standardise(df: pd.DataFrame, cfg: Config) -> pd.DataFrame:
    out = df.copy()
    stripped = {c: out[c].str.strip() for c in cfg.columns}
    idx = out.index

    for c in cfg.columns:
        out[f"std_{c}"] = stripped[c]
    out["std_account_number"] = stripped["account_number"].str.upper()
    out["std_account_type"] = stripped["account_type"].str.upper()

    txn_dates = [parse_date(v, cfg.date_formats) for v in stripped["txn_date"]]
    txn_times = [parse_time(v, cfg.time_format) for v in stripped["txn_time"]]
    out["_txn_date"] = _objects(txn_dates, idx)
    out["_txn_time"] = _objects(txn_times, idx)
    out["std_txn_date"] = [d.isoformat() if d else "" for d in txn_dates]
    out["std_txn_time"] = [t.strftime("%H:%M:%S") if t else "" for t in txn_times]

    for c in AMOUNT_COLUMNS:
        amounts = [parse_amount(v, cfg.amount_scale) for v in stripped[c]]
        out[f"_{c}_present"] = (stripped[c] != "").to_numpy(dtype=bool)
        out[f"_{c}"] = _objects(amounts, idx)
        out[f"std_{c}"] = [format_amount(a, cfg.amount_scale) for a in amounts]

    out["_file_date"] = _objects((date.fromisoformat(v) for v in out["file_date"]), idx)
    return out
