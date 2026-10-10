"""Duplicate detection on the business key (default: account_number + ref_no).

For each valid incoming row, in file order:

* key not seen before                        -> accepted
* key seen, same values, same file + line     -> already loaded (a re-run; no change)
* key seen, same values, different source     -> exact duplicate (removed, written to audit)
* key seen, different values                  -> key conflict (quarantined for review)

Rows with a different ref_no are never compared, so genuine repeated transactions survive.
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Iterable

import pandas as pd

from .config import Config

CONFLICT_CODE = "DUPLICATE_KEY_CONFLICT"
DUP_IN_FILE = "EXACT_DUPLICATE_IN_FILE"
DUP_OF_LOADED = "EXACT_DUPLICATE_OF_LOADED_ROW"


@dataclass
class DedupResult:
    accepted: pd.DataFrame
    already_loaded: pd.DataFrame
    duplicates: pd.DataFrame
    conflicts: pd.DataFrame


def payload_hashes(values: Iterable[tuple[str, ...]]) -> list[str]:
    return [hashlib.sha256("\x1f".join(v).encode("utf-8")).hexdigest() for v in values]


def deduplicate(valid: pd.DataFrame, store: pd.DataFrame, cfg: Config) -> DedupResult:
    columns = list(cfg.columns)
    key_positions = [columns.index(k) for k in cfg.dedup_key]

    # key -> (payload hash, file sha, line, file name) for everything already accepted
    known: dict[tuple[str, ...], tuple[str, str, int, str]] = {}
    store_values = list(zip(*(store[c].astype(str) for c in columns))) if not store.empty else []
    for values, ph, sha, line, name in zip(
        store_values, payload_hashes(store_values),
        store.get("file_sha256", []), store.get("source_row_number", []), store.get("source_file", []),
    ):
        known[tuple(values[i] for i in key_positions)] = (ph, sha, int(line), name)

    valid = valid.sort_values("source_row_number", kind="stable")
    incoming = list(zip(*(valid[f"std_{c}"] for c in columns))) if not valid.empty else []

    accepted, loaded, duplicates, conflicts = [], [], [], []
    dup_type, dup_of_file, dup_of_row, conflict_detail = [], [], [], []
    for idx, values, ph, sha, line, name in zip(
        valid.index, incoming, payload_hashes(incoming),
        valid["file_sha256"], valid["source_row_number"], valid["source_file"],
    ):
        key = tuple(values[i] for i in key_positions)
        seen = known.get(key)
        if seen is None:
            known[key] = (ph, sha, int(line), name)
            accepted.append(idx)
        elif seen[0] == ph and (seen[1], seen[2]) == (sha, int(line)):
            loaded.append(idx)
        elif seen[0] == ph:
            duplicates.append(idx)
            dup_type.append(DUP_IN_FILE if seen[1] == sha else DUP_OF_LOADED)
            dup_of_file.append(seen[3])
            dup_of_row.append(seen[2])
        else:
            conflicts.append(idx)
            conflict_detail.append(f"key already loaded from {seen[3]} line {seen[2]} with different values")

    dup_frame = valid.loc[duplicates].copy()
    dup_frame["duplicate_type"] = dup_type
    dup_frame["duplicate_of_file"] = dup_of_file
    dup_frame["duplicate_of_row"] = dup_of_row
    conflict_frame = valid.loc[conflicts].copy()
    conflict_frame["rejection_reason"] = CONFLICT_CODE
    conflict_frame["rejection_detail"] = conflict_detail
    return DedupResult(valid.loc[accepted].copy(), valid.loc[loaded].copy(), dup_frame, conflict_frame)
