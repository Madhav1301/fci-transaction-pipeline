"""Small file helpers shared by the writers: atomic writes and deterministic CSV output."""
from __future__ import annotations

import os
import tempfile
from pathlib import Path
from typing import Iterable, Sequence

import pandas as pd


def atomic_write_bytes(path: Path, data: bytes) -> bool:
    """Write ``data`` to ``path`` via a temp file + rename.

    Returns False (and leaves the file untouched) when the content is already identical,
    so re-runs never rewrite unchanged outputs.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and path.read_bytes() == data:
        return False
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.", suffix=".tmp")
    try:
        with os.fdopen(fd, "wb") as fh:
            fh.write(data)
        os.replace(tmp, path)
    except BaseException:
        Path(tmp).unlink(missing_ok=True)
        raise
    return True


def csv_bytes(df: pd.DataFrame, columns: Sequence[str]) -> bytes:
    """Serialise ``df`` with a fixed column order, LF line endings and UTF-8."""
    return df.loc[:, list(columns)].to_csv(index=False, lineterminator="\n").encode("utf-8")


def read_text_csv(path: Path) -> pd.DataFrame:
    """Read a CSV produced by this pipeline, keeping every value as text."""
    return pd.read_csv(path, dtype=str, keep_default_na=False)


def concat_frames(frames: Iterable[pd.DataFrame], columns: Sequence[str]) -> pd.DataFrame:
    """Concatenate non-empty frames; return an empty frame with ``columns`` if there are none."""
    non_empty = [f for f in frames if not f.empty]
    if not non_empty:
        return pd.DataFrame(columns=list(columns))
    return pd.concat(non_empty, ignore_index=True)
