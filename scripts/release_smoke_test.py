"""Release smoke test for imports, config, sample data, and pipeline output."""

from __future__ import annotations

from pathlib import Path
import sys

import yaml


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def load_release_smoke_config() -> dict:
    config_path = ROOT / "config.yaml"
    if not config_path.exists():
        raise FileNotFoundError(f"Missing config.yaml: {config_path}")

    with config_path.open("r", encoding="utf-8") as fh:
        config = yaml.safe_load(fh) or {}
    if not isinstance(config, dict):
        raise ValueError("config.yaml must contain a YAML mapping.")

    config.setdefault("osrm", {})
    config["osrm"].update({"use_osrm": False, "use_cache": False, "fallback_to_haversine": True})
    config.setdefault("candidate_routes", {})
    config["candidate_routes"].update({"cache": False, "candidates_per_rep": 250, "keep_top_n_per_rep": 250})
    config.setdefault("daily_route", {})
    config["daily_route"].update({"target_clients": 6, "min_clients": 1, "max_clients": 8, "allow_underfilled": True})
    config.setdefault("optimization", {})
    config["optimization"].update({"time_limit_seconds": 20, "num_workers": 4, "log_search_progress": False})
    return config


def main() -> int:
    from src.app_paths import ensure_runtime_dirs
    from src.pipeline import run_pipeline

    ensure_runtime_dirs()
    sample_path = ROOT / "data" / "sample_clients.xlsx"
    if not sample_path.exists():
        raise FileNotFoundError(f"Missing sample workbook: {sample_path}")

    result = run_pipeline(
        input_path=str(sample_path),
        config=load_release_smoke_config(),
        output_dir=str(ROOT / "output" / "release_smoke_test"),
        progress_callback=lambda percent, message: print(f"[{percent:3d}%] {message}"),
        log_callback=print,
    )

    print(f"Release smoke status: {result['status']}")
    print(f"Message: {result['message']}")
    if result["status"] == "success":
        excel_path = Path(str(result["excel_path"]))
        if not excel_path.exists():
            raise AssertionError(f"Expected Excel output was not created: {excel_path}")
        print(f"Excel output created: {excel_path}")
        return 0
    if result["status"] == "infeasible":
        print("Controlled infeasible result returned.")
        for warning in result.get("warnings", []):
            print(f"DIAGNOSTIC: {warning}")
        return 0
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
