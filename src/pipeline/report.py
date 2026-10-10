"""Reconciliation summary (Markdown for people, JSON for automated checks).

The summary is cumulative and is derived only from persisted state (manifest, clean store,
quarantine and duplicates audit). It contains no run timestamps, so re-running the pipeline
on the same inputs reproduces it byte for byte.
"""
from __future__ import annotations

import json
from collections import Counter
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any

import pandas as pd

from .config import Config
from .dedup import CONFLICT_CODE
from .io_utils import atomic_write_bytes
from .manifest import FAILED, SUCCESS, Manifest

METRICS = ("received", "processed", "rejected", "duplicates_removed")


def _split_codes(values: pd.Series) -> Counter:
    counts: Counter = Counter()
    for value in values:
        counts.update(code.strip() for code in str(value).split(";") if code.strip())
    return counts


def _money(values: pd.Series) -> str:
    total = sum((Decimal(v) for v in values if v), Decimal("0"))
    return f"{total:.2f}"


def build_summary(manifest: Manifest, store: pd.DataFrame, quarantine: pd.DataFrame,
                  duplicates: pd.DataFrame, missing_dates: list[date], cfg: Config) -> dict[str, Any]:
    files = []
    for e in manifest.entries(SUCCESS):
        files.append({"file": e["file_name"], "file_date": e["file_date"], "file_type": e["file_type"],
                      **{m: e["metrics"][m] for m in METRICS}})
    totals = {m: sum(f[m] for f in files) for m in METRICS}
    balanced = totals["received"] == totals["processed"] + totals["rejected"] + totals["duplicates_removed"]

    ok_hashes = {e["sha256"] for e in manifest.entries(SUCCESS)}
    q = quarantine[quarantine["file_sha256"].isin(ok_hashes)]
    d = duplicates[duplicates["file_sha256"].isin(ok_hashes)]
    reason_counts = _split_codes(q["rejection_reason"])

    partitions = store.groupby("txn_date").size() if not store.empty else pd.Series(dtype=int)

    late = []
    for e in manifest.entries(SUCCESS):
        rows = store[(store["file_sha256"] == e["sha256"]) & (store["is_late_arrival"] == "true")]
        if rows.empty and e["file_type"] == "daily":
            continue
        earlier = store[(store["file_date"] < e["file_date"])
                        | ((store["file_date"] == e["file_date"]) & (store["source_file"] < e["file_name"]))]
        parts = []
        for txn_date, n in rows.groupby("txn_date").size().items():
            before = int((earlier["txn_date"] == txn_date).sum())
            parts.append({"txn_date": txn_date, "rows_before": before, "rows_added": int(n), "rows_after": before + int(n)})
        file_q = q[q["file_sha256"] == e["sha256"]]
        late.append({
            "file": e["file_name"],
            "file_type": e["file_type"],
            "late_rows_loaded": int(len(rows)),
            "partitions": parts,
            "existing_rows_updated": 0,
            "key_conflicts_quarantined": int((file_q["rejection_reason"] == CONFLICT_CODE).sum()),
            "rejected": e["metrics"]["rejected"],
            "duplicates_removed": e["metrics"]["duplicates_removed"],
            "debit_added": _money(rows["debit"]),
            "credit_added": _money(rows["credit"]),
            "accounts_affected": sorted(rows["account_number"].unique().tolist()),
            "dq_warnings": dict(sorted(_split_codes(rows["dq_warnings"]).items())),
        })

    return {
        "source": cfg.source_name,
        "files": files,
        "totals": totals,
        "control_check": {"equation": "received = processed + rejected + duplicates_removed", "passed": balanced},
        "clean_dataset": {"rows": int(len(store)), "partitions": {str(k): int(v) for k, v in partitions.items()}},
        "rejections_by_reason": dict(sorted(reason_counts.items())),
        "rejected_records_with_multiple_reasons": int(sum(";" in str(r) for r in q["rejection_reason"])),
        "duplicates_by_type": dict(sorted(Counter(d["duplicate_type"]).items())),
        "late_corrections": late,
        "missing_files": [{"date": m.isoformat(), "expected_file": cfg.expected_file_name.format(date=m.isoformat())}
                          for m in missing_dates],
        "dq_warnings": dict(sorted(_split_codes(store["dq_warnings"]).items())) if not store.empty else {},
        "failed_files": [{"file": e["file_name"], "sha256": e["sha256"], "error": e["error"]}
                         for e in manifest.entries(FAILED)],
    }


def _table(headers: list[str], rows: list[list[Any]]) -> list[str]:
    lines = ["| " + " | ".join(headers) + " |", "|" + "|".join("---" for _ in headers) + "|"]
    lines += ["| " + " | ".join(str(c) for c in row) + " |" for row in rows]
    return lines


def render_markdown(s: dict[str, Any]) -> str:
    t = s["totals"]
    out = [f"# Reconciliation Summary: {s['source']}", ""]
    out += ["## Overview", ""]
    out += _table(["Measure", "Count"], [
        ["Total records received", t["received"]],
        ["Total records successfully processed", t["processed"]],
        ["Total records rejected (quarantined)", t["rejected"]],
        ["Duplicates removed", t["duplicates_removed"]],
        ["Rows in clean dataset", s["clean_dataset"]["rows"]],
    ])
    check = s["control_check"]
    status = "PASSED" if check["passed"] else "FAILED"
    out += ["", f"Control check `{check['equation']}`: **{status}** "
            f"({t['received']} = {t['processed']} + {t['rejected']} + {t['duplicates_removed']})", ""]

    out += ["## By file", ""]
    out += _table(["File", "File date", "Type", "Received", "Processed", "Rejected", "Duplicates removed"],
                  [[f["file"], f["file_date"], f["file_type"], f["received"], f["processed"], f["rejected"],
                    f["duplicates_removed"]] for f in s["files"]])

    out += ["", "## Rejected records by reason", ""]
    if s["rejections_by_reason"]:
        out += _table(["Reason", "Records"], [[k, v] for k, v in s["rejections_by_reason"].items()])
        out += ["", f"Records with more than one reason: {s['rejected_records_with_multiple_reasons']}. "
                "Details: `output/quarantine/quarantine.csv`."]
    else:
        out += ["No records were rejected."]

    out += ["", "## Duplicates removed", ""]
    if s["duplicates_by_type"]:
        out += _table(["Type", "Records"], [[k, v] for k, v in s["duplicates_by_type"].items()])
        out += ["", "Duplicate key: " + "`account_number + ref_no`. Details: `output/audit/duplicates_removed.csv`."]
    else:
        out += ["No duplicates were found."]

    out += ["", "## Late correction impact", ""]
    if not s["late_corrections"]:
        out += ["No late-arriving records."]
    for lc in s["late_corrections"]:
        out += [f"**{lc['file']}** ({lc['file_type']})", ""]
        out += [f"- Late rows loaded into earlier dates: {lc['late_rows_loaded']}"]
        for p in lc["partitions"]:
            out += [f"- Partition `txn_date={p['txn_date']}`: {p['rows_before']} -> {p['rows_after']} rows "
                    f"(+{p['rows_added']})"]
        out += [f"- Existing rows updated: {lc['existing_rows_updated']} (corrections never overwrite loaded rows)",
                f"- Key conflicts quarantined: {lc['key_conflicts_quarantined']}; other rejections: "
                f"{lc['rejected'] - lc['key_conflicts_quarantined']}; duplicates removed: {lc['duplicates_removed']}",
                f"- Amounts added: debit {lc['debit_added']}, credit {lc['credit_added']}",
                f"- Accounts affected: {', '.join(lc['accounts_affected']) or 'none'}"]
        warnings = ", ".join(f"{k} x{v}" for k, v in lc["dq_warnings"].items()) or "none"
        out += [f"- Data-quality warnings on these rows: {warnings}", ""]

    out += ["## Missing files", ""]
    if s["missing_files"]:
        out += _table(["Date", "Expected file"], [[m["date"], m["expected_file"]] for m in s["missing_files"]])
    else:
        out += ["No gaps in the delivery calendar."]

    out += ["", "## Data-quality warnings (rows kept)", ""]
    if s["dq_warnings"]:
        out += _table(["Warning", "Rows"], [[k, v] for k, v in s["dq_warnings"].items()])
    else:
        out += ["None."]

    if s["failed_files"]:
        out += ["", "## Files that failed to load", ""]
        out += _table(["File", "Error"], [[f["file"], f["error"]] for f in s["failed_files"]])
    return "\n".join(out) + "\n"


def write_reports(summary: dict[str, Any], cfg: Config) -> list[Path]:
    changed = []
    md = cfg.reports_dir / "reconciliation_summary.md"
    js = cfg.reports_dir / "reconciliation_summary.json"
    if atomic_write_bytes(md, render_markdown(summary).encode("utf-8")):
        changed.append(md)
    if atomic_write_bytes(js, (json.dumps(summary, indent=2) + "\n").encode("utf-8")):
        changed.append(js)
    return changed
