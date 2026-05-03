"""Runtime path helpers for development and PyInstaller builds."""

from __future__ import annotations

from pathlib import Path
import sys


def _is_frozen() -> bool:
    return bool(getattr(sys, "frozen", False))


def get_base_dir() -> Path:
    """Return the writable application base directory.

    In development this is the repository root. In a PyInstaller executable this
    is the folder containing the executable, not the internal bundle directory.
    """
    if _is_frozen():
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parents[1]


def get_project_root() -> Path:
    """Return the project/runtime root used for relative app files."""
    return get_base_dir()


def get_config_path() -> Path:
    return get_base_dir() / "config.yaml"


def get_data_dir() -> Path:
    return get_base_dir() / "data"


def get_cache_dir() -> Path:
    return get_base_dir() / "cache"


def get_output_dir() -> Path:
    return get_base_dir() / "output"


def get_logs_dir() -> Path:
    return get_base_dir() / "logs"


def get_resource_path(relative_path: str) -> Path:
    """Resolve a resource from the writable app dir or bundled PyInstaller dir."""
    normalized = relative_path.replace("\\", "/").lstrip("/")
    external_path = get_base_dir() / normalized
    if external_path.exists():
        return external_path

    bundle_dir = getattr(sys, "_MEIPASS", None)
    if bundle_dir:
        bundled_path = Path(bundle_dir) / normalized
        if bundled_path.exists():
            return bundled_path
    return external_path


def ensure_runtime_dirs() -> None:
    """Create runtime folders that should be writable next to the app."""
    for path in [
        get_data_dir(),
        get_cache_dir(),
        get_cache_dir() / "osrm_matrices",
        get_cache_dir() / "candidate_routes",
        get_cache_dir() / "route_costs",
        get_output_dir(),
        get_output_dir() / "maps",
        get_output_dir() / "logs",
        get_output_dir() / "runs",
        get_logs_dir(),
    ]:
        path.mkdir(parents=True, exist_ok=True)
