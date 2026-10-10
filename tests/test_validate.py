import pandas as pd
import pytest

from pipeline.store import to_clean_format
from pipeline.validate import apply_hard_rules, soft_rule_warnings

from conftest import make_row, standardised


def reasons(cfg, **overrides):
    valid, rejected = apply_hard_rules(standardised(cfg, [make_row(**overrides)]), cfg)
    return "" if rejected.empty else rejected["rejection_reason"].iloc[0]


def test_valid_row_passes(cfg):
    assert reasons(cfg) == ""


@pytest.mark.parametrize("value", [
    "31-02-2026",   # impossible date
    "32-08-2026",   # impossible day
    "2026/08/01",   # wrong format
    "2026-08-01",   # ISO is not the source format
    "",             # blank
    "02-08-2026",   # after the file date
    "01-06-2026",   # more than max_backdate_days before the file date
])
def test_invalid_dates_are_rejected(cfg, value):
    assert reasons(cfg, txn_date=value) == "INVALID_TXN_DATE"


@pytest.mark.parametrize("value", ["25:00:00", "10:00", ""])
def test_invalid_times_are_rejected(cfg, value):
    assert reasons(cfg, txn_time=value) == "INVALID_TXN_TIME"


def test_both_debit_and_credit(cfg):
    assert reasons(cfg, debit="1200", credit="1200") == "BOTH_DEBIT_AND_CREDIT"


def test_neither_debit_nor_credit(cfg):
    assert reasons(cfg, debit="", credit="") == "NO_DEBIT_OR_CREDIT"


def test_whitespace_only_amount_counts_as_absent(cfg):
    assert reasons(cfg, debit="  ", credit="") == "NO_DEBIT_OR_CREDIT"


@pytest.mark.parametrize("value", ["abc", "0", "-5.00", "1,200"])
def test_invalid_amounts(cfg, value):
    assert reasons(cfg, debit=value) == "INVALID_AMOUNT"


def test_missing_ref_no(cfg):
    assert reasons(cfg, ref_no="  ") == "MISSING_REF_NO"


def test_bad_account_number(cfg):
    assert reasons(cfg, account_number="AC12") == "INVALID_ACCOUNT_NUMBER"


def test_invalid_balance(cfg):
    assert reasons(cfg, balance="n/a") == "INVALID_BALANCE"


def test_every_reason_is_reported(cfg):
    got = reasons(cfg, txn_date="31-02-2026", debit="10", credit="10", ref_no="")
    assert got == "INVALID_TXN_DATE; BOTH_DEBIT_AND_CREDIT; MISSING_REF_NO"


def test_amounts_are_normalised_to_two_decimals(cfg):
    std = standardised(cfg, [make_row(debit="366050", balance="46887.6")])
    assert std["std_debit"].iloc[0] == "366050.00"
    assert std["std_balance"].iloc[0] == "46887.60"


def _clean(cfg, rows, file_date="2026-08-01"):
    return to_clean_format(standardised(cfg, rows, file_date), cfg)


def test_soft_rules_flag_but_never_reject(cfg):
    history = _clean(cfg, [
        make_row(ref_no="REF1", debit="100.00", balance="900.00", account_type="SALARY"),
        make_row(ref_no="REF2", debit="100.00", balance="800.00", account_type="SALARY"),
    ])
    new = _clean(cfg, [
        make_row(ref_no="REF3", debit="50.00", balance="750.00", account_type="SALARY"),     # continues
        make_row(ref_no="REF4", debit="50.00", balance="999.00", account_type="SAVINGS"),    # breaks both
    ], file_date="2026-08-02")
    warnings = soft_rule_warnings(new, history, cfg).tolist()
    assert warnings == ["", "ACCOUNT_TYPE_MISMATCH; BALANCE_DISCONTINUITY"]


def test_negative_savings_balance_warning(cfg):
    new = _clean(cfg, [make_row(balance="-10.00"), make_row(ref_no="R2", account_type="CURRENT", balance="-20.00")])
    empty = pd.DataFrame(columns=new.columns)
    warnings = soft_rule_warnings(new, empty, cfg).tolist()
    assert warnings[0] == "NEGATIVE_SAVINGS_BALANCE"
    assert "NEGATIVE_SAVINGS_BALANCE" not in warnings[1]
