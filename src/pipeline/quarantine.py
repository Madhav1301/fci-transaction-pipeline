"""Quarantine and duplicates-audit files.

Both keep the original (raw) source values plus lineage, and are keyed on file hash + line
number, so re-processing a file never adds the same record twice.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from .config import Config
from .io_utils import atomic_write_bytes, concat_frames, csv_bytes, read_text_csv

RECORD_KEY = ["file_sha256", "source_row_number"]
LINEAGE = ["source_file", "source_row_number", "file_date", "file_sha256"]


def quarantine_columns(cfg: Config) -> list[str]:
    return list(cfg.columns) + ["rejection_reason", "rejection_detail"] + LINEAGE


def duplicates_columns(cfg: Config) -> list[str]:
    return list(cfg.columns) + ["duplicate_type", "duplicate_of_file", "duplicate_of_row"] + LINEAGE


def load_records(path: Path, columns: list[str]) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=columns)
    return read_text_csv(path).reindex(columns=columns, fill_value="")


def merge_records(path: Path, new_rows: list[pd.DataFrame], columns: list[str]) -> tuple[pd.DataFrame, bool]:
    """Upsert ``new_rows`` into the file at ``path``; return (all records, whether the file changed)."""
    existing = load_records(path, columns)
    new = concat_frames([f.loc[:, columns] for f in new_rows], columns)
    merged = concat_frames([existing.astype(str), new.astype(str)], columns)
    if merged.empty:
        return merged, False
    merged["_line"] = merged["source_row_number"].astype(int)
    merged = (
        merged.drop_duplicates(subset=RECORD_KEY, keep="last")
        .sort_values(["file_date", "source_file", "_line"], kind="stable")
        .drop(columns="_line")
        .reset_index(drop=True)
    )
    return merged, atomic_write_bytes(path, csv_bytes(merged, columns))
