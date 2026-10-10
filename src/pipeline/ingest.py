"""File discovery: find source files, fingerprint them and detect gaps in the delivery calendar."""
from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Iterable

from .config import Config

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class SourceFile:
    path: Path
    name: str
    file_date: date
    file_type: str  # "daily" unless the file name carries a suffix such as "late_correction"
    sha256: str


def sha256_of(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def discover_files(cfg: Config) -> list[SourceFile]:
    """Return matching files in the input folder, ordered by file date then name.

    Originals therefore always run before later corrections.
    """
    if not cfg.input_dir.is_dir():
        raise FileNotFoundError(f"input folder not found: {cfg.input_dir}")
    found: list[SourceFile] = []
    for path in sorted(cfg.input_dir.iterdir()):
        if not path.is_file():
            continue
        match = cfg.file_pattern.match(path.name)
        if not match:
            log.info("ignoring %s: name does not match the source file pattern", path.name)
            continue
        try:
            file_date = date.fromisoformat(match.group("file_date"))
        except ValueError:
            log.warning("ignoring %s: file name contains an invalid date", path.name)
            continue
        file_type = match.groupdict().get("file_type") or "daily"
        found.append(SourceFile(path, path.name, file_date, file_type, sha256_of(path)))
    return sorted(found, key=lambda f: (f.file_date, f.name))


def find_missing_dates(file_dates: Iterable[date], cfg: Config) -> list[date]:
    """Dates in the expected calendar (start date .. latest file date) with no file delivered."""
    seen = set(file_dates)
    if not seen:
        return []
    day = cfg.calendar_start or min(seen)
    end = max(seen)
    missing = []
    while day <= end:
        expected = cfg.calendar_frequency == "daily" or day.weekday() < 5
        if expected and day not in seen:
            missing.append(day)
        day += timedelta(days=1)
    return missing
