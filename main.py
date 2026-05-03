"""Command-line entry point for the route-first sales PVRP scheduler."""

from __future__ import annotations

from pathlib import Path
from typing import Any
import argparse
from copy import deepcopy

import yaml

from src.app_paths import ensure_runtime_dirs, get_base_dir, get_config_path, get_data_dir, get_output_dir
from src.osrm_status import check_osrm_status
from src.pipeline import run_pipeline
from src.version import APP_BUILD, APP_NAME, APP_VERSION


def _resolve_runtime_path(path: str) -> str:
    candidate = Path(path).expanduser()
    if candidate.is_absolute():
        return str(candidate)
    return str((get_base_dir() / candidate).resolve())


def load_config(path: str | None = None) -> dict[str, Any]:
    """Load YAML configuration."""
    config_path = Path(path) if path else get_config_path()
    if not config_path.exists():
        raise FileNotFoundError(f"Config file is missing: {config_path}")
    with config_path.open("r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}
    if not isinstance(data, dict):
        raise ValueError("Config file must contain a YAML mapping.")
    return data


def _default_input() -> Path:
    production_input = get_data_dir() / "input_clients.xlsx"
    if production_input.exists():
        return production_input
    return get_data_dir() / "sample_clients.xlsx"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the route-first Sales PVRP scheduler.")
    parser.add_argument("--version", action="version", version=f"{APP_NAME} v{APP_VERSION} ({APP_BUILD})")
    parser.add_argument("--input", default=str(_default_input()), help="Input Excel/CSV file.")
    parser.add_argument("--config", default=str(get_config_path()), help="YAML config file.")
    parser.add_argument("--output", default=str(get_output_dir()), help="Output directory.")
    parser.add_argument("--no-osrm", action="store_true", help="Disable OSRM and use haversine distances.")
    parser.add_argument("--check-osrm", action="store_true", help="Check OSRM availability and exit.")
    parser.add_argument("--time-limit", type=int, help="Override optimization.time_limit_seconds.")
    parser.add_argument("--num-workers", type=int, help="Override optimization.num_workers.")
    parser.add_argument("--quiet-solver", action="store_true", help="Disable verbose CP-SAT search logging.")
    parser.add_argument("--target-clients", type=int, help="Override daily_route.target_clients.")
    parser.add_argument("--min-clients", type=int, help="Override daily_route.min_clients.")
    parser.add_argument("--max-clients", type=int, help="Override daily_route.max_clients.")
    parser.add_argument("--candidates-per-rep", type=int, help="Override candidate_routes.candidates_per_rep.")
    parser.add_argument("--keep-top-n-per-rep", type=int, help="Override candidate_routes.keep_top_n_per_rep.")
    parser.add_argument("--no-cache", action="store_true", help="Disable OSRM and candidate-route caches for this run.")
    return parser.parse_args()


def apply_cli_overrides(config: dict[str, Any], args: argparse.Namespace) -> dict[str, Any]:
    updated = deepcopy(config)
    updated.setdefault("daily_route", {})
    updated.setdefault("candidate_routes", {})
    updated.setdefault("optimization", {})
    updated.setdefault("osrm", {})

    if args.no_osrm:
        updated["osrm"]["use_osrm"] = False
        updated["osrm"]["fallback_to_haversine"] = True
    if args.no_cache:
        updated["osrm"]["use_cache"] = False
        updated["candidate_routes"]["cache"] = False
    for attr, section, key in [
        ("time_limit", "optimization", "time_limit_seconds"),
        ("num_workers", "optimization", "num_workers"),
        ("target_clients", "daily_route", "target_clients"),
        ("min_clients", "daily_route", "min_clients"),
        ("max_clients", "daily_route", "max_clients"),
        ("candidates_per_rep", "candidate_routes", "candidates_per_rep"),
        ("keep_top_n_per_rep", "candidate_routes", "keep_top_n_per_rep"),
    ]:
        value = getattr(args, attr)
        if value is not None:
            updated[section][key] = value
    if args.quiet_solver:
        updated["optimization"]["log_search_progress"] = False
    return updated


def main() -> int:
    """Run the public pipeline contract from the command line."""
    ensure_runtime_dirs()
    args = parse_args()
    args.input = _resolve_runtime_path(args.input)
    args.config = _resolve_runtime_path(args.config)
    args.output = _resolve_runtime_path(args.output)

    config = apply_cli_overrides(load_config(args.config), args)
    osrm_config = config.get("osrm", {})
    if args.check_osrm:
        status = check_osrm_status(str(osrm_config.get("url", "")))
        print(status["message"])
        return 0 if status["available"] else 1

    if bool(osrm_config.get("use_osrm", True)):
        status = check_osrm_status(str(osrm_config.get("url", "")))
        if not status["available"]:
            if bool(osrm_config.get("fallback_to_haversine", True)):
                print(f"WARNING: {status['message']} Using haversine fallback.")
                config["osrm"]["use_osrm"] = False
            else:
                print(f"WARNING: {status['message']} OSRM fallback is disabled.")

    result = run_pipeline(
        input_path=args.input,
        config=config,
        output_dir=args.output,
        progress_callback=lambda percent, message: print(f"[{percent:3d}%] {message}"),
        log_callback=print,
    )

    print(f"Status: {result['status']}")
    print(f"Message: {result['message']}")
    if result.get("solver_status"):
        print(f"Solver status: {result['solver_status']}")
    if result.get("excel_path"):
        print(f"Output Excel: {result['excel_path']}")
    if result.get("map_path"):
        print(f"Output map: {result['map_path']}")
    if result.get("total_route_km") is not None:
        print(f"Total route km: {result['total_route_km']:.1f}")
    for warning in result.get("warnings", []):
        print(f"DIAGNOSTIC: {warning}")

    if result["status"] == "success":
        return 0
    if result["status"] == "infeasible":
        return 2
    if result["status"] == "cancelled":
        return 130
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
