"""End-to-end smoke test for the public run_pipeline contract."""

from __future__ import annotations

from pathlib import Path
import sys

import openpyxl
import pandas as pd
import yaml

ROOT = Path(__file__).resolve().parents[1]
EXPECTED_SHEETS = {
    "Final_Schedule",
    "Daily_Routes",
    "Summary_By_Sales_Rep",
    "Summary_By_Day",
    "Validation",
    "Candidate_Routes_Selected",
    "Candidate_Coverage",
    "Parameters",
}


def load_smoke_config() -> dict:
    with (ROOT / "config.yaml").open("r", encoding="utf-8") as fh:
        config = yaml.safe_load(fh) or {}
    config["osrm"]["use_osrm"] = False
    config["osrm"]["use_cache"] = False
    config["candidate_routes"]["cache"] = False
    config["daily_route"].update(
        {
            "target_clients": 6,
            "min_clients": 1,
            "max_clients": 8,
            "allow_underfilled": True,
        }
    )
    config["candidate_routes"].update(
        {
            "candidates_per_rep": 250,
            "keep_top_n_per_rep": 250,
        }
    )
    config["optimization"].update(
        {
            "time_limit_seconds": 20,
            "num_workers": 4,
            "log_search_progress": False,
        }
    )
    return config


def assert_excel_output(excel_path: Path) -> None:
    if not excel_path.exists():
        raise AssertionError(f"Expected Excel output does not exist: {excel_path}")
    workbook = openpyxl.load_workbook(excel_path, read_only=True, data_only=True)
    missing = EXPECTED_SHEETS - set(workbook.sheetnames)
    if missing:
        raise AssertionError(f"Missing expected sheets: {sorted(missing)}")

    schedule = pd.read_excel(excel_path, sheet_name="Final_Schedule")
    selected = pd.read_excel(excel_path, sheet_name="Candidate_Routes_Selected")
    if schedule.empty:
        raise AssertionError("Final_Schedule is empty.")
    if schedule["route_km_total"].isna().any():
        raise AssertionError("route_km_total is missing for at least one scheduled stop.")
    if selected["route_km"].isna().any():
        raise AssertionError("route_km is missing for at least one selected candidate route.")
    if schedule.duplicated(["client_id", "day_index"]).any():
        raise AssertionError("A client is visited more than once on the same day.")
    if selected.groupby(["sales_rep", "day_index"]).size().max() > 1:
        raise AssertionError("A sales rep has more than one selected route on a day.")

    freq2 = schedule[schedule["visit_frequency"].eq(2)].groupby("client_id").size()
    if not freq2.empty and not freq2.eq(2).all():
        raise AssertionError("At least one frequency 2 client does not have exactly 2 visits.")

    for client_id, visits in schedule[schedule["visit_frequency"].eq(4)].groupby("client_id"):
        weekly = visits.groupby("week_index").size().reindex([1, 2, 3, 4], fill_value=0)
        if not weekly.eq(1).all():
            raise AssertionError(f"Frequency 4 weekly rule failed for client {client_id}.")

    for client_id, visits in schedule[schedule["visit_frequency"].eq(8)].groupby("client_id"):
        weekly = visits.groupby("week_index").size().reindex([1, 2, 3, 4], fill_value=0)
        if not weekly.eq(2).all():
            raise AssertionError(f"Frequency 8 weekly rule failed for client {client_id}.")


def main() -> int:
    sys.path.insert(0, str(ROOT))
    from src.pipeline import run_pipeline

    result = run_pipeline(
        input_path=str(ROOT / "data" / "sample_clients.xlsx"),
        config=load_smoke_config(),
        output_dir=str(ROOT / "output" / "smoke_test"),
        progress_callback=lambda percent, message: print(f"[{percent:3d}%] {message}"),
        log_callback=print,
    )

    print(f"Smoke status: {result['status']}")
    print(f"Message: {result['message']}")
    if result["status"] == "success":
        excel_path = Path(str(result["excel_path"]))
        assert_excel_output(excel_path)
        if result["total_route_km"] is None:
            raise AssertionError("total_route_km must not be None for a successful smoke run.")
        print(f"Excel output verified: {excel_path}")
        return 0
    if result["status"] == "infeasible":
        print("Controlled infeasible result returned. Diagnostics:")
        for warning in result.get("warnings", []):
            print(f"- {warning}")
        return 0
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
