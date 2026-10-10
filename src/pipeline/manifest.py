"""Processing manifest: which files (by content hash) have been processed, and with what result."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .ingest import SourceFile
from .io_utils import atomic_write_bytes

SUCCESS = "SUCCESS"
FAILED = "FAILED"


class Manifest:
    def __init__(self, path: Path, files: dict[str, dict[str, Any]] | None = None):
        self.path = path
        self.files: dict[str, dict[str, Any]] = files or {}

    @classmethod
    def load(cls, path: Path) -> "Manifest":
        if not path.exists():
            return cls(path)
        data = json.loads(path.read_text(encoding="utf-8"))
        return cls(path, data.get("files", {}))

    def is_processed(self, sf: SourceFile) -> bool:
        entry = self.files.get(sf.sha256)
        return bool(entry) and entry["status"] == SUCCESS

    def earlier_versions(self, sf: SourceFile) -> list[dict[str, Any]]:
        """Entries with the same file name but different content (a restated delivery)."""
        return [e for sha, e in self.files.items() if e["file_name"] == sf.name and sha != sf.sha256]

    def record(self, sf: SourceFile, status: str, *, now: str,
               metrics: dict[str, int] | None = None, error: str | None = None) -> None:
        previous = self.files.get(sf.sha256, {})
        self.files[sf.sha256] = {
            "file_name": sf.name,
            "file_date": sf.file_date.isoformat(),
            "file_type": sf.file_type,
            "sha256": sf.sha256,
            "status": status,
            "metrics": metrics,
            "error": error,
            "first_processed_at": previous.get("first_processed_at", now),
            "last_processed_at": now,
        }

    def entries(self, status: str | None = None) -> list[dict[str, Any]]:
        rows = [e for e in self.files.values() if status is None or e["status"] == status]
        return sorted(rows, key=lambda e: (e["file_date"], e["file_name"], e["sha256"]))

    def to_bytes(self) -> bytes:
        return (json.dumps({"files": self.files}, indent=2, sort_keys=True) + "\n").encode("utf-8")

    def save(self) -> bool:
        return atomic_write_bytes(self.path, self.to_bytes())
