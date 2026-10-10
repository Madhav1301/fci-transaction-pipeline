"""Pipeline runner and command-line entry point.

    python -m pipeline.run --config config/pipeline.yaml [--force-reprocess] [--dry-run]
"""
from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd

from .config import Config, ConfigError, load_config
from .dedup import deduplicate
from .ingest import SourceFile, discover_files, find_missing_dates
from .io_utils import concat_frames
from .manifest import FAILED, SUCCESS, Manifest
from .quarantine import duplicates_columns, merge_records, quarantine_columns
from .reader import SchemaError, read_source_file, standardise
from .report import build_summary, write_reports
from .store import clean_columns, load_store, sort_store, to_clean_format, write_store
from .validate import apply_hard_rules, soft_rule_warnings

log = logging.getLogger("pipeline")


class ControlCheckError(RuntimeError):
    """A file's counts do not reconcile; nothing is written."""


@dataclass
class RunResult:
    processed: list[str] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)
    failed: list[str] = field(default_factory=list)
    file_metrics: dict[str, dict[str, int]] = field(default_factory=dict)
    changed_outputs: list[Path] = field(default_factory=list)
    summary: dict | None = None


def _process_file(sf: SourceFile, store: pd.DataFrame, cfg: Config):
    raw = read_source_file(sf, cfg)
    std = standardise(raw, cfg)
    valid, rejected = apply_hard_rules(std, cfg)
    result = deduplicate(valid, store, cfg)

    accepted = to_clean_format(result.accepted, cfg)
    accepted["dq_warnings"] = soft_rule_warnings(accepted, store, cfg)

    quarantined = concat_frames([rejected, result.conflicts], list(rejected.columns))
    metrics = {
        "received": len(raw),
        "processed": len(result.accepted) + len(result.already_loaded),
        "rejected": len(quarantined),
        "duplicates_removed": len(result.duplicates),
        "late_rows": int((accepted["is_late_arrival"] == "true").sum()),
    }
    if metrics["received"] != metrics["processed"] + metrics["rejected"] + metrics["duplicates_removed"]:
        raise ControlCheckError(f"{sf.name}: counts do not reconcile: {metrics}")
    return accepted, quarantined, result.duplicates, metrics


def run_pipeline(cfg: Config, *, force: bool = False, dry_run: bool = False) -> RunResult:
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    result = RunResult()
    manifest = Manifest.load(cfg.manifest_file)
    store = load_store(cfg)
    files = discover_files(cfg)
    log.info("found %d source file(s) in %s; clean store holds %d rows", len(files), cfg.input_dir, len(store))

    new_quarantine, new_duplicates = [], []
    for sf in files:
        if manifest.is_processed(sf) and not force:
            log.info("skip %s: already processed (sha256 %s...)", sf.name, sf.sha256[:12])
            result.skipped.append(sf.name)
            continue
        for old in manifest.earlier_versions(sf):
            log.warning("%s was delivered before with different content (sha256 %s...); "
                        "processing it as a restated file", sf.name, old["sha256"][:12])
        try:
            accepted, quarantined, duplicates, metrics = _process_file(sf, store, cfg)
        except SchemaError as exc:
            log.error("FAILED %s: %s", sf.name, exc)
            manifest.record(sf, FAILED, now=now, error=str(exc))
            result.failed.append(sf.name)
            continue
        store = sort_store(concat_frames([store, accepted], clean_columns(cfg)))
        new_quarantine.append(quarantined)
        new_duplicates.append(duplicates)
        manifest.record(sf, SUCCESS, now=now, metrics=metrics)
        result.processed.append(sf.name)
        result.file_metrics[sf.name] = metrics
        log.info("processed %s: received=%d processed=%d rejected=%d duplicates=%d late=%d",
                 sf.name, metrics["received"], metrics["processed"], metrics["rejected"],
                 metrics["duplicates_removed"], metrics["late_rows"])

    if dry_run:
        log.info("dry run: no outputs written")
        return result

    # data first, manifest last: if anything fails before this point the next run simply redoes it
    result.changed_outputs += write_store(store, cfg)
    quarantine, q_changed = merge_records(cfg.quarantine_file, new_quarantine, quarantine_columns(cfg))
    duplicates, d_changed = merge_records(cfg.duplicates_file, new_duplicates, duplicates_columns(cfg))
    result.changed_outputs += [p for p, c in ((cfg.quarantine_file, q_changed), (cfg.duplicates_file, d_changed)) if c]
    if manifest.save():
        result.changed_outputs.append(cfg.manifest_file)

    delivered = [date.fromisoformat(e["file_date"]) for e in manifest.entries()]
    missing = find_missing_dates(delivered, cfg)
    for day in missing:
        log.warning("no file received for %s (expected %s)", day, cfg.expected_file_name.format(date=day.isoformat()))
    result.summary = build_summary(manifest, store, quarantine, duplicates, missing, cfg)
    result.changed_outputs += write_reports(result.summary, cfg)

    if result.changed_outputs:
        for path in result.changed_outputs:
            log.info("wrote %s", path.relative_to(cfg.project_root) if path.is_relative_to(cfg.project_root) else path)
    else:
        log.info("no outputs changed")
    return result


def _configure_logging(log_file: Path, level: str) -> None:
    log_file.parent.mkdir(parents=True, exist_ok=True)
    fmt = logging.Formatter("%(asctime)s %(levelname)-7s %(name)s: %(message)s")
    root = logging.getLogger()
    root.setLevel(level)
    for handler in [h for h in root.handlers if getattr(h, "_pipeline", False)]:
        root.removeHandler(handler)
        handler.close()
    for handler in (logging.StreamHandler(sys.stdout), logging.FileHandler(log_file, encoding="utf-8")):
        handler.setFormatter(fmt)
        handler._pipeline = True  # so a second main() call in the same process replaces, not duplicates
        root.addHandler(handler)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Ingest bank transaction files with data-quality checks.")
    parser.add_argument("--config", default="config/pipeline.yaml", help="path to the YAML configuration")
    parser.add_argument("--force-reprocess", action="store_true",
                        help="re-read files even if already processed (output is unchanged by design)")
    parser.add_argument("--dry-run", action="store_true", help="process and report counts without writing")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    args = parser.parse_args(argv)

    try:
        cfg = load_config(args.config)
    except ConfigError as exc:
        print(f"configuration error: {exc}", file=sys.stderr)
        return 2
    _configure_logging(cfg.log_file, args.log_level)
    log.info("run started: source=%s force=%s dry_run=%s", cfg.source_name, args.force_reprocess, args.dry_run)
    try:
        result = run_pipeline(cfg, force=args.force_reprocess, dry_run=args.dry_run)
    except (ControlCheckError, FileNotFoundError) as exc:
        log.error("run aborted, nothing written: %s", exc)
        return 1

    if result.summary:
        t = result.summary["totals"]
        log.info("totals: received=%d processed=%d rejected=%d duplicates_removed=%d (control check %s)",
                 t["received"], t["processed"], t["rejected"], t["duplicates_removed"],
                 "passed" if result.summary["control_check"]["passed"] else "FAILED")
    log.info("run finished: processed=%d skipped=%d failed=%d",
             len(result.processed), len(result.skipped), len(result.failed))
    return 1 if result.failed else 0


if __name__ == "__main__":
    sys.exit(main())
