from conftest import make_row


def test_exact_duplicates_in_file_are_removed_and_audited(project):
    rows = [make_row(ref_no="REF1"), make_row(ref_no="REF2", debit="50.00", balance="950.00"), make_row(ref_no="REF1")]
    project.add_rows("batch_2026-08-01.csv", rows)
    result = project.run()

    assert result.file_metrics["batch_2026-08-01.csv"] == {
        "received": 3, "processed": 2, "rejected": 0, "duplicates_removed": 1, "late_rows": 0}
    dups = project.duplicates()
    assert dups[["ref_no", "source_row_number", "duplicate_type", "duplicate_of_row"]].values.tolist() == [
        ["REF1", "4", "EXACT_DUPLICATE_IN_FILE", "2"]]


def test_genuine_repeats_with_different_ref_no_are_kept(project):
    # identical in every business field except the bank reference: three real interest credits
    rows = [make_row(ref_no=f"REF{i}", debit="", credit="10.00", description="INTEREST CREDIT") for i in range(3)]
    project.add_rows("batch_2026-08-01.csv", rows)
    project.run()
    assert len(project.clean()) == 3
    assert not (project.root / "output/audit/duplicates_removed.csv").exists() or project.duplicates().empty


def test_duplicate_arriving_in_a_later_file_is_removed(project):
    project.add_rows("batch_2026-08-01.csv", [make_row(ref_no="REF1")])
    project.add_rows("batch_2026-08-02.csv", [make_row(ref_no="REF1"), make_row(ref_no="REF2", balance="900.00")])
    result = project.run()

    assert result.file_metrics["batch_2026-08-02.csv"]["duplicates_removed"] == 1
    assert project.duplicates()["duplicate_type"].tolist() == ["EXACT_DUPLICATE_OF_LOADED_ROW"]
    assert sorted(project.clean()["ref_no"]) == ["REF1", "REF2"]


def test_key_conflict_is_quarantined_not_overwritten(project):
    project.add_rows("batch_2026-08-01.csv", [make_row(ref_no="REF1", debit="100.00")])
    project.add_rows("batch_2026-08-02.csv", [make_row(ref_no="REF1", debit="999.00")])
    project.run()

    clean = project.clean()
    assert clean["debit"].tolist() == ["100.00"]          # original kept
    q = project.quarantine()
    assert q["rejection_reason"].tolist() == ["DUPLICATE_KEY_CONFLICT"]
    assert "batch_2026-08-01.csv line 2" in q["rejection_detail"].iloc[0]


def test_formatting_differences_are_still_duplicates(project):
    project.add_rows("batch_2026-08-01.csv", [make_row(ref_no="REF1", debit="100", account_type="savings")])
    project.add_rows("batch_2026-08-02.csv", [make_row(ref_no=" REF1 ", debit="100.00", account_type="SAVINGS")])
    project.run()
    assert len(project.clean()) == 1
    assert len(project.duplicates()) == 1
