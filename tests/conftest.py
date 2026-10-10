"""Shared fixtures: an isolated project folder that uses the real config/pipeline.yaml."""
from __future__ import annotations

import shutil
from pathlib import Path

import pandas as pd
import pytest

from pipeline.config import Config, load_config
from pipeline.reader import standardise
from pipeline.run import RunResult, run_pipeline

REPO_ROOT = Path(__file__).resolve().parents[1]
HEADER = "account_number,txn_date,txn_time,description,ref_no,debit,credit,balance,account_type"
OUTPUT_DIRS = ("output", "reports", "state")


def make_row(**overrides: str) -> dict[str, str]:
    row = {
        "account_number": "AC100000001", "txn_date": "01-08-2026", "txn_time": "10:00:00",
        "description": "TEST PAYMENT", "ref_no": "REF000001", "debit": "100.00", "credit": "",
        "balance": "1000.00", "account_type": "SAVINGS",
    }
    row.update(overrides)
    return row


def csv_text(rows: list[dict[str, str]], header: str = HEADER) -> str:
    cols = header.split(",")
    return "\n".join([header] + [",".join(r.get(c, "") for c in cols) for r in rows]) + "\n"


class Project:
    def __init__(self, root: Path):
        self.root = root
        (root / "config").mkdir(parents=True)
        shutil.copy(REPO_ROOT / "config" / "pipeline.yaml", root / "config" / "pipeline.yaml")
        (root / "input").mkdir()

    @property
    def config(self) -> Config:
        return load_config(self.root / "config" / "pipeline.yaml")

    def add_file(self, name: str, text: str) -> Path:
        path = self.root / "input" / name
        path.write_bytes(text.encode("utf-8"))
        return path

    def add_rows(self, name: str, rows: list[dict[str, str]]) -> Path:
        return self.add_file(name, csv_text(rows))

    def copy_real_input(self, *names: str) -> None:
        for name in names:
            shutil.copy(REPO_ROOT / "input" / name, self.root / "input" / name)

    def run(self, **kwargs) -> RunResult:
        return run_pipeline(self.config, **kwargs)

    def snapshot(self, include_manifest: bool = True) -> dict[str, bytes]:
        files = {}
        for d in OUTPUT_DIRS:
            for p in sorted((self.root / d).rglob("*")):
                if p.is_file() and (include_manifest or p.name != "manifest.json"):
                    files[p.relative_to(self.root).as_posix()] = p.read_bytes()
        return files

    def clean(self) -> pd.DataFrame:
        return pd.read_csv(self.root / "output/clean/transactions_all.csv", dtype=str, keep_default_na=False)

    def quarantine(self) -> pd.DataFrame:
        return pd.read_csv(self.root / "output/quarantine/quarantine.csv", dtype=str, keep_default_na=False)

    def duplicates(self) -> pd.DataFrame:
        return pd.read_csv(self.root / "output/audit/duplicates_removed.csv", dtype=str, keep_default_na=False)


@pytest.fixture
def project(tmp_path: Path) -> Project:
    return Project(tmp_path / "project")


@pytest.fixture
def cfg(project: Project) -> Config:
    return project.config


def standardised(cfg: Config, rows: list[dict[str, str]], file_date: str = "2026-08-01") -> pd.DataFrame:
    """Build a standardised frame directly, as the reader would produce it."""
    df = pd.DataFrame(rows, columns=list(cfg.columns)).fillna("").astype(str)
    df["source_file"] = f"batch_{file_date}.csv"
    df["source_row_number"] = range(2, len(df) + 2)
    df["file_date"] = file_date
    df["file_sha256"] = "test"
    return standardise(df, cfg)
