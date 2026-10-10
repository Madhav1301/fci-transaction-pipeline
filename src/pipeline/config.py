"""Load and validate the YAML configuration that describes one source feed."""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import yaml


class ConfigError(ValueError):
    """The configuration file is missing a setting or contains an invalid value."""


@dataclass(frozen=True)
class Config:
    project_root: Path
    source_name: str
    input_dir: Path
    file_pattern: re.Pattern[str]
    encoding: str
    delimiter: str
    column_map: dict[str, str]
    columns: tuple[str, ...]
    date_formats: tuple[str, ...]
    time_format: str
    amount_scale: int
    account_number_pattern: re.Pattern[str]
    dedup_key: tuple[str, ...]
    on_conflict: str
    hard_rules: tuple[str, ...]
    soft_rules: tuple[str, ...]
    max_backdate_days: int
    calendar_frequency: str
    calendar_start: date | None
    expected_file_name: str
    clean_dir: Path
    quarantine_file: Path
    duplicates_file: Path
    reports_dir: Path
    manifest_file: Path
    log_file: Path


_MISSING = object()


def _get(raw: dict[str, Any], dotted: str, default: Any = _MISSING) -> Any:
    node: Any = raw
    for part in dotted.split("."):
        if not isinstance(node, dict) or part not in node:
            if default is _MISSING:
                raise ConfigError(f"missing required setting '{dotted}'")
            return default
        node = node[part]
    return node


def load_config(path: str | Path) -> Config:
    """Load a config file. Relative paths resolve from the folder above ``config/``."""
    path = Path(path).resolve()
    if not path.is_file():
        raise ConfigError(f"config file not found: {path}")
    with path.open(encoding="utf-8") as fh:
        raw = yaml.safe_load(fh) or {}
    root_setting = raw.get("project_root")
    root = (path.parent / root_setting).resolve() if root_setting else path.parent.parent
    return config_from_dict(raw, root)


def config_from_dict(raw: dict[str, Any], project_root: Path) -> Config:
    # imported here to avoid a circular import (validate uses Config for type hints)
    from .validate import HARD_RULES, SOFT_RULES

    def path_of(dotted: str) -> Path:
        p = Path(_get(raw, dotted))
        return p if p.is_absolute() else (project_root / p)

    try:
        file_pattern = re.compile(_get(raw, "source.file_pattern"))
        account_pattern = re.compile(_get(raw, "schema.account_number_pattern"))
    except re.error as exc:
        raise ConfigError(f"invalid regular expression: {exc}") from exc
    if "file_date" not in file_pattern.groupindex:
        raise ConfigError("source.file_pattern must define a named group 'file_date'")

    columns = tuple(_get(raw, "schema.columns"))
    dedup_key = tuple(_get(raw, "dedup.key"))
    if not dedup_key or any(c not in columns for c in dedup_key):
        raise ConfigError(f"dedup.key {list(dedup_key)} must be a non-empty subset of schema.columns")
    required = {"account_number", "txn_date", "txn_time", "ref_no", "debit", "credit", "balance", "account_type"}
    if missing := sorted(required - set(columns)):
        raise ConfigError(f"schema.columns is missing columns the rules depend on: {missing}")

    on_conflict = _get(raw, "dedup.on_conflict", "quarantine")
    if on_conflict != "quarantine":
        raise ConfigError("dedup.on_conflict currently supports only 'quarantine'")

    hard = tuple(_get(raw, "validation.hard"))
    soft = tuple(_get(raw, "validation.soft", []))
    if unknown := [r for r in hard if r not in HARD_RULES]:
        raise ConfigError(f"unknown hard rule(s): {unknown}; available: {sorted(HARD_RULES)}")
    if unknown := [r for r in soft if r not in SOFT_RULES]:
        raise ConfigError(f"unknown soft rule(s): {unknown}; available: {sorted(SOFT_RULES)}")

    frequency = _get(raw, "calendar.frequency", "daily")
    if frequency not in ("daily", "business_days"):
        raise ConfigError("calendar.frequency must be 'daily' or 'business_days'")
    start = _get(raw, "calendar.start_date", None)
    if isinstance(start, str):
        start = date.fromisoformat(start)

    return Config(
        project_root=project_root,
        source_name=str(_get(raw, "source.name")),
        input_dir=path_of("source.input_dir"),
        file_pattern=file_pattern,
        encoding=_get(raw, "source.encoding", "utf-8"),
        delimiter=_get(raw, "source.delimiter", ","),
        column_map=dict(_get(raw, "source.column_map", {}) or {}),
        columns=columns,
        date_formats=tuple(_get(raw, "schema.date_formats")),
        time_format=_get(raw, "schema.time_format"),
        amount_scale=int(_get(raw, "schema.amount_scale", 2)),
        account_number_pattern=account_pattern,
        dedup_key=dedup_key,
        on_conflict=on_conflict,
        hard_rules=hard,
        soft_rules=soft,
        max_backdate_days=int(_get(raw, "validation.max_backdate_days", 30)),
        calendar_frequency=frequency,
        calendar_start=start,
        expected_file_name=_get(raw, "calendar.expected_file_name", "{date}"),
        clean_dir=path_of("output.clean_dir"),
        quarantine_file=path_of("output.quarantine_file"),
        duplicates_file=path_of("output.duplicates_file"),
        reports_dir=path_of("output.reports_dir"),
        manifest_file=path_of("output.manifest_file"),
        log_file=path_of("output.log_file"),
    )
