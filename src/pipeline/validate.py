"""Validation rules.

Hard rules decide whether a row is rejected (quarantined). Every hard rule is evaluated for
every row, so a rejected row carries all of its reasons. Soft rules never reject: they attach
data-quality warnings to rows that are kept.
"""
from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from decimal import Decimal
from typing import TYPE_CHECKING, Callable

import numpy as np
import pandas as pd

if TYPE_CHECKING:
    from .config import Config


@dataclass(frozen=True)
class HardRule:
    code: str
    description: str
    fails: Callable[[pd.DataFrame, "Config"], pd.Series]  # True where the row fails the rule


def _bool(values, df: pd.DataFrame) -> pd.Series:
    return pd.Series(list(values), index=df.index, dtype=bool)


def _account_number_format(df, cfg):
    return _bool((not cfg.account_number_pattern.fullmatch(v) for v in df["std_account_number"]), df)


def _valid_txn_date(df, cfg):
    return _bool((d is None for d in df["_txn_date"]), df)


def _txn_date_window(df, cfg):
    # a transaction cannot be dated after the file that delivers it, nor implausibly far before it
    def outside(d, file_date):
        return d is not None and (d > file_date or (file_date - d).days > cfg.max_backdate_days)
    return _bool((outside(d, f) for d, f in zip(df["_txn_date"], df["_file_date"])), df)


def _valid_txn_time(df, cfg):
    return _bool((t is None for t in df["_txn_time"]), df)


def _debit_credit_exclusive(df, cfg):
    return _bool(df["_debit_present"] & df["_credit_present"], df)


def _debit_credit_present(df, cfg):
    return _bool(~df["_debit_present"] & ~df["_credit_present"], df)


def _positive_amount(df, cfg):
    def bad(present, amount):
        return present and (amount is None or amount <= 0)
    debit = (bad(p, a) for p, a in zip(df["_debit_present"], df["_debit"]))
    credit = (bad(p, a) for p, a in zip(df["_credit_present"], df["_credit"]))
    return _bool((d or c for d, c in zip(debit, credit)), df)


def _ref_no_present(df, cfg):
    return _bool(df["std_ref_no"] == "", df)


def _valid_balance(df, cfg):
    return _bool((b is None for b in df["_balance"]), df)


HARD_RULES: dict[str, HardRule] = {
    "account_number_format": HardRule("INVALID_ACCOUNT_NUMBER", "account number missing or wrong format", _account_number_format),
    "valid_txn_date": HardRule("INVALID_TXN_DATE", "txn_date missing, unparseable or impossible", _valid_txn_date),
    "txn_date_window": HardRule("INVALID_TXN_DATE", "txn_date after the file date or too far before it", _txn_date_window),
    "valid_txn_time": HardRule("INVALID_TXN_TIME", "txn_time missing or invalid", _valid_txn_time),
    "debit_credit_exclusive": HardRule("BOTH_DEBIT_AND_CREDIT", "both debit and credit have a value", _debit_credit_exclusive),
    "debit_credit_present": HardRule("NO_DEBIT_OR_CREDIT", "neither debit nor credit has a value", _debit_credit_present),
    "positive_amount": HardRule("INVALID_AMOUNT", "amount not numeric, zero or negative", _positive_amount),
    "ref_no_present": HardRule("MISSING_REF_NO", "ref_no missing", _ref_no_present),
    "valid_balance": HardRule("INVALID_BALANCE", "balance missing or not numeric", _valid_balance),
}

SOFT_RULES: dict[str, str] = {
    "account_type_consistency": "ACCOUNT_TYPE_MISMATCH",
    "balance_continuity": "BALANCE_DISCONTINUITY",
    "negative_savings_balance": "NEGATIVE_SAVINGS_BALANCE",
}


def apply_hard_rules(df: pd.DataFrame, cfg: "Config") -> tuple[pd.DataFrame, pd.DataFrame]:
    """Split ``df`` into (valid, rejected); rejected rows get a ``rejection_reason`` column."""
    reasons: list[list[str]] = [[] for _ in range(len(df))]
    for name in cfg.hard_rules:
        rule = HARD_RULES[name]
        for i in np.flatnonzero(rule.fails(df, cfg).to_numpy(dtype=bool)):
            if rule.code not in reasons[i]:
                reasons[i].append(rule.code)
    rejected_mask = np.array([bool(r) for r in reasons], dtype=bool)
    rejected = df[rejected_mask].copy()
    rejected["rejection_reason"] = ["; ".join(r) for r in reasons if r]
    rejected["rejection_detail"] = ""
    return df[~rejected_mask].copy(), rejected


def _posting_key(r: dict) -> tuple:
    # posting order: business date, then delivery, then position in the file
    return (r["txn_date"], r["file_date"], r["source_file"], int(r["source_row_number"]))


def soft_rule_warnings(new_rows: pd.DataFrame, history: pd.DataFrame, cfg: "Config") -> pd.Series:
    """Warnings for ``new_rows`` given already-accepted ``history`` (both in clean format)."""
    if new_rows.empty or not cfg.soft_rules:
        return pd.Series([""] * len(new_rows), index=new_rows.index, dtype=object)

    def records(frame: pd.DataFrame, is_new: bool) -> list[dict]:
        out = []
        for idx, row in zip(frame.index, frame.to_dict("records")):
            row["_idx"], row["_new"] = idx, is_new
            for c in ("debit", "credit", "balance"):
                row[c] = Decimal(row[c]) if row[c] else None
            out.append(row)
        return out

    old = records(history, False)
    new = records(new_rows, True)
    warnings: dict[object, list[str]] = {r["_idx"]: [] for r in new}

    if "account_type_consistency" in cfg.soft_rules:
        # established type = most common type in history (or in this batch for a new account);
        # ties go to the type seen first in posting order
        by_account: dict[str, list[dict]] = {}
        for r in sorted(old, key=_posting_key):
            by_account.setdefault(r["account_number"], []).append(r)
        batch_by_account: dict[str, list[dict]] = {}
        for r in sorted(new, key=_posting_key):
            batch_by_account.setdefault(r["account_number"], []).append(r)
        for account, rows in batch_by_account.items():
            basis = by_account.get(account) or rows
            counts = Counter(r["account_type"] for r in basis)
            top = max(counts.values())
            established = next(r["account_type"] for r in basis if counts[r["account_type"]] == top)
            for r in rows:
                if r["account_type"] != established:
                    warnings[r["_idx"]].append(SOFT_RULES["account_type_consistency"])

    if "balance_continuity" in cfg.soft_rules:
        last_balance: dict[str, Decimal] = {}
        for r in sorted(old + new, key=_posting_key):
            account = r["account_number"]
            if r["_new"] and account in last_balance:
                expected = last_balance[account] - (r["debit"] or 0) + (r["credit"] or 0)
                if expected != r["balance"]:
                    warnings[r["_idx"]].append(SOFT_RULES["balance_continuity"])
            last_balance[account] = r["balance"]

    if "negative_savings_balance" in cfg.soft_rules:
        for r in new:
            if r["account_type"] == "SAVINGS" and r["balance"] < 0:
                warnings[r["_idx"]].append(SOFT_RULES["negative_savings_balance"])

    order = list(SOFT_RULES.values())
    return pd.Series(
        ["; ".join(sorted(warnings[i], key=order.index)) for i in new_rows.index],
        index=new_rows.index, dtype=object,
    )
