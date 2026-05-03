"""Configuration and recent-settings helpers for the GUI."""

from __future__ import annotations

from pathlib import Path
from typing import Any
import json

import yaml

from src.app_paths import get_base_dir, get_config_path, get_output_dir

def project_root() -> Path:
    return get_base_dir()


def default_config_path() -> Path:
    return get_config_path()


def default_output_dir() -> Path:
    return get_output_dir()


def settings_path() -> Path:
    return get_base_dir() / "gui_settings.json"


def load_config(path: str | Path) -> dict[str, Any]:
    with Path(path).open("r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}
    if not isinstance(data, dict):
        raise ValueError("Config file must contain a YAML mapping.")
    return data


def save_config(config: dict[str, Any], path: str | Path) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("w", encoding="utf-8") as fh:
        yaml.safe_dump(config, fh, sort_keys=False, allow_unicode=True)


def load_recent_settings() -> dict[str, Any]:
    path = settings_path()
    if not path.exists():
        return {}
    try:
        with path.open("r", encoding="utf-8") as fh:
            data = json.load(fh)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def save_recent_settings(settings: dict[str, Any]) -> None:
    path = settings_path()
    with path.open("w", encoding="utf-8") as fh:
        json.dump(settings, fh, indent=2, ensure_ascii=False)
