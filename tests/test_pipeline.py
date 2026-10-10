import json

import pytest

from pipeline.run import main

from conftest import HEADER, csv_text, make_row

DAILY = "batch_2026-08-01.csv"
LATE = "batch_2026-08-04_late_correction.csv"


def summary(project):
    return json.loads((project.root / "reports/reconciliation_summary.json").read_text(encoding="utf-8"))


# ---------------------------------------------------------------- end to end on the provided data

def test_end_to_end_on_provided_files(project):
    project.copy_real_input(DAILY, LATE)
    project.run()
    s = summary(project)

    assert s["totals"] == {"received": 190, "processed": 183, "rejected": 3, "duplicates_removed": 4}
    assert s["control_check"]["passed"]
    assert s["rejections_by_reason"] == {"BOTH_DEBIT_AND_CREDIT": 1, "MISSING_REF_NO": 1, "NO_DEBIT_OR_CREDIT": 1}
    assert s["duplicates_by_type"] == {"EXACT_DUPLICATE_IN_FILE": 4}
    assert s["clean_dataset"] == {"rows": 183, "partitions": {"2026-08-01": 183}}
    assert [m["expected_file"] for m in s["missing_files"]] == ["batch_2026-08-02.csv", "batch_2026-08-03.csv"]
    assert s["dq_warnings"] == {"ACCOUNT_TYPE_MISMATCH": 3, "BALANCE_DISCONTINUITY": 3, "NEGATIVE_SAVINGS_BALANCE": 3}

    (late,) = s["late_corrections"]
    assert late["partitions"] == [{"txn_date": "2026-08-01", "rows_before": 180, "rows_added": 3, "rows_after": 183}]
    assert late["debit_added"] == "18500.00" and late["credit_added"] == "97000.00"
    assert late["accounts_affected"] == ["AC100000002", "AC100000005", "AC100000009"]
    assert late["existing_rows_updated"] == 0

    q = project.quarantine()
    assert q["source_row_number"].tolist() == ["186", "187", "188"]
    assert q["debit"].tolist()[1] == "1200"        # quarantine keeps the original values


# ---------------------------------------------------------------- re-processing

def test_rerun_skips_files_and_changes_nothing(project):
    project.copy_real_input(DAILY, LATE)
    project.run()
    before = project.snapshot()
    result = project.run()
    assert result.skipped == [DAILY, LATE] and not result.processed
    assert result.changed_outputs == []
    assert project.snapshot() == before


def test_force_reprocess_produces_identical_outputs(project):
    project.copy_real_input(DAILY, LATE)
    project.run()
    before = project.snapshot(include_manifest=False)
    result = project.run(force=True)
    assert result.processed == [DAILY, LATE]
    assert project.snapshot(include_manifest=False) == before


def test_lost_manifest_does_not_create_duplicates(project):
    project.copy_real_input(DAILY, LATE)
    project.run()
    before = project.snapshot(include_manifest=False)
    (project.root / "state/manifest.json").unlink()
    project.run()
    assert project.snapshot(include_manifest=False) == before


def test_restated_file_only_adds_new_rows(project):
    project.add_rows(DAILY, [make_row(ref_no="REF1")])
    project.run()
    project.add_rows(DAILY, [make_row(ref_no="REF1"), make_row(ref_no="REF2", balance="900.00")])  # same name, new content
    result = project.run()
    assert result.file_metrics[DAILY] == {"received": 2, "processed": 1, "rejected": 0, "duplicates_removed": 1, "late_rows": 0}
    assert sorted(project.clean()["ref_no"]) == ["REF1", "REF2"]


def test_dry_run_writes_nothing(project):
    project.copy_real_input(DAILY)
    result = project.run(dry_run=True)
    assert result.file_metrics[DAILY]["processed"] == 180
    assert project.snapshot() == {}


# ---------------------------------------------------------------- late corrections

def test_late_file_in_a_later_run_lands_in_history_once(project):
    project.copy_real_input(DAILY)
    project.run()
    assert summary(project)["clean_dataset"]["partitions"] == {"2026-08-01": 180}
    partitions_before = sorted(p.name for p in (project.root / "output/clean").iterdir())

    project.copy_real_input(LATE)
    project.run()
    s = summary(project)
    assert s["clean_dataset"]["partitions"] == {"2026-08-01": 183}
    assert sorted(p.name for p in (project.root / "output/clean").iterdir()) == partitions_before  # no new partition
    late_rows = project.clean().query("source_file == @LATE")
    assert set(late_rows["is_late_arrival"]) == {"true"}

    before = project.snapshot(include_manifest=False)
    project.run(force=True)
    assert project.snapshot(include_manifest=False) == before


def test_late_correction_does_not_duplicate_existing_rows(project):
    project.add_rows(DAILY, [make_row(ref_no="REF1")])
    project.add_rows("batch_2026-08-04_late_correction.csv",
                     [make_row(ref_no="REF1"), make_row(ref_no="REF9", balance="900.00", txn_time="23:00:00")])
    project.run()
    clean = project.clean()
    assert sorted(clean["ref_no"]) == ["REF1", "REF9"]
    assert clean.set_index("ref_no").loc["REF9", "is_late_arrival"] == "true"


# ---------------------------------------------------------------- robustness

def test_schema_drift_fails_only_that_file(project):
    project.add_rows(DAILY, [make_row()])
    project.add_file("batch_2026-08-02.csv", csv_text([make_row(ref_no="REF2")], header=HEADER.replace("ref_no", "reference")))
    result = project.run()
    assert result.failed == ["batch_2026-08-02.csv"]
    s = summary(project)
    assert s["totals"]["received"] == 1
    assert s["failed_files"][0]["file"] == "batch_2026-08-02.csv"
    assert "missing=['ref_no']" in s["failed_files"][0]["error"]


def test_blank_line_is_counted_and_rejected(project):
    project.add_file(DAILY, csv_text([make_row()]).rstrip("\n") + "\n\n" + ",".join(make_row(ref_no="REF2").values()) + "\n")
    result = project.run()
    assert result.file_metrics[DAILY]["received"] == 3
    assert result.file_metrics[DAILY]["rejected"] == 1


def test_missing_dates_follow_business_calendar(project):
    cfg_path = project.root / "config/pipeline.yaml"
    cfg_path.write_text(cfg_path.read_text(encoding="utf-8").replace("frequency: daily", "frequency: business_days"),
                        encoding="utf-8")
    project.copy_real_input(DAILY, LATE)
    project.run()
    # 2026-08-01 is a Saturday: the weekend is not expected, Monday 08-03 is
    assert [m["date"] for m in summary(project)["missing_files"]] == ["2026-08-03"]


# ---------------------------------------------------------------- CLI

def test_cli_success_and_config_error(project, capsys):
    project.copy_real_input(DAILY, LATE)
    assert main(["--config", str(project.root / "config/pipeline.yaml")]) == 0
    assert main(["--config", str(project.root / "config/missing.yaml")]) == 2


def test_unknown_rule_in_config_is_rejected(project):
    cfg_path = project.root / "config/pipeline.yaml"
    cfg_path.write_text(cfg_path.read_text(encoding="utf-8").replace("- ref_no_present", "- no_such_rule"),
                        encoding="utf-8")
    from pipeline.config import ConfigError
    with pytest.raises(ConfigError, match="no_such_rule"):
        project.config
