"""Clean transaction store, partitioned by business date (txn_date).

In memory the store is a DataFrame in "clean format": canonical text values plus lineage.
On disk it is one CSV per txn_date partition plus a combined convenience file. Partitions are
rewritten only when their content changes.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from .config import Config
from .io_utils import atomic_write_bytes, csv_bytes, read_text_csv

STORE_LINEAGE = ["source_file", "source_row_number", "file_date", "file_sha256", "is_late_arrival", "dq_warnings"]
SORT_ORDER = ["txn_date", "account_number", "file_date", "source_file", "source_row_number"]
COMBINED_FILE = "transactions_all.csv"


def clean_columns(cfg: Config) -> list[str]:
    return list(cfg.columns) + STORE_LINEAGE


def partition_path(cfg: Config, txn_date: str) -> Path:
    return cfg.clean_dir / f"txn_date={txn_date}" / "transactions.csv"


def to_clean_format(rows: pd.DataFrame, cfg: Config) -> pd.DataFrame:
    """Convert standardised, accepted rows to the clean output format."""
    out = pd.DataFrame({c: rows[f"std_{c}"].to_numpy(dtype=object) for c in cfg.columns}, index=rows.index)
    out["source_file"] = rows["source_file"].to_numpy(dtype=object)
    out["source_row_number"] = rows["source_row_number"].astype(int).to_numpy()
    out["file_date"] = rows["file_date"].to_numpy(dtype=object)
    out["file_sha256"] = rows["file_sha256"].to_numpy(dtype=object)
    out["is_late_arrival"] = ["true" if d < f else "false" for d, f in zip(rows["_txn_date"], rows["_file_date"])]
    out["dq_warnings"] = ""
    return out


def sort_store(store: pd.DataFrame) -> pd.DataFrame:
    if store.empty:
        return store
    ordered = store.assign(source_row_number=store["source_row_number"].astype(int))
    return ordered.sort_values(SORT_ORDER, kind="stable").reset_index(drop=True)


def load_store(cfg: Config) -> pd.DataFrame:
    columns = clean_columns(cfg)
    frames = [read_text_csv(p) for p in sorted(cfg.clean_dir.glob("txn_date=*/transactions.csv"))]
    frames = [f for f in frames if not f.empty]
    if not frames:
        return pd.DataFrame(columns=columns)
    store = pd.concat(frames, ignore_index=True).loc[:, columns]
    return sort_store(store)


def write_store(store: pd.DataFrame, cfg: Config) -> list[Path]:
    """Write every partition and the combined file; return the paths that actually changed."""
    columns = clean_columns(cfg)
    store = sort_store(store)
    changed: list[Path] = []
    for txn_date, part in store.groupby("txn_date", sort=True):
        path = partition_path(cfg, str(txn_date))
        if atomic_write_bytes(path, csv_bytes(part, columns)):
            changed.append(path)
    combined = cfg.clean_dir / COMBINED_FILE
    if atomic_write_bytes(combined, csv_bytes(store, columns)):
        changed.append(combined)
    return changed
