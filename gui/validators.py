"""User-facing validation helpers for GUI inputs and runtime settings."""

from __future__ import annotations

from pathlib import Path
from typing import Any


REQUIRED_COLUMNS = ["client_id", "client_name", "sales_rep", "lat", "lon", "visit_frequency"]


def validate_excel_file(path: str) -> list[str]:
    errors: list[str] = []
    if not path or not str(path).strip():
        return ["Изберете входен Excel файл."]
    file_path = Path(path)
    if not file_path.exists():
        errors.append(f"Файлът не съществува: {file_path}")
    elif file_path.suffix.lower() not in {".xlsx", ".xlsm", ".xls", ".csv"}:
        errors.append("Поддържат се само Excel файлове (.xlsx, .xlsm, .xls) или CSV.")
    return errors


def validate_config_values(config: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    daily = config.get("daily_route", {})
    osrm = config.get("osrm", {})
    candidate = config.get("candidate_routes", {})
    optimization = config.get("optimization", {})

    try:
        target = int(daily.get("target_clients", 0))
        min_clients = int(daily.get("min_clients", 0))
        max_clients = int(daily.get("max_clients", 0))
    except (TypeError, ValueError):
        errors.append("target_clients, min_clients и max_clients трябва да са числа.")
        return errors

    if min_clients <= 0:
        errors.append("min_clients трябва да бъде по-голямо от 0.")
    if target < min_clients or target > max_clients:
        errors.append("target_clients трябва да бъде между min_clients и max_clients.")
    if max_clients < target:
        errors.append("max_clients трябва да бъде по-голямо или равно на target_clients.")

    if bool(osrm.get("use_osrm", False)) and not str(osrm.get("url", "")).strip():
        errors.append("OSRM URL не може да бъде празен, когато use_osrm е включен.")

    for key in ["candidates_per_rep", "keep_top_n_per_rep", "random_seed"]:
        try:
            int(candidate.get(key, 0))
        except (TypeError, ValueError):
            errors.append(f"{key} трябва да бъде цяло число.")

    for key in ["time_limit_seconds", "num_workers"]:
        try:
            value = int(optimization.get(key, 0))
            if value <= 0:
                errors.append(f"{key} трябва да бъде по-голямо от 0.")
        except (TypeError, ValueError):
            errors.append(f"{key} трябва да бъде цяло число.")

    return errors


def validate_output_dir(path: str) -> list[str]:
    if not path or not str(path).strip():
        return ["Изберете output папка."]
    try:
        Path(path).mkdir(parents=True, exist_ok=True)
    except Exception as exc:
        return [f"Output папката не може да бъде създадена: {exc}"]
    return []


def format_missing_column_message(column: str) -> str:
    return f"Липсва задължителна колона: {column}"
