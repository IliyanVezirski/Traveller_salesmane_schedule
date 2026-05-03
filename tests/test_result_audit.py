from pathlib import Path

import pandas as pd

from scripts.generate_synthetic_clients import generate_synthetic_clients
from src.result_audit import audit_final_schedule, audit_schedule, validate_input_clients_for_audit


def _config() -> dict:
    return {
        "daily_route": {
            "target_clients": 2,
            "min_clients": 1,
            "max_clients": 3,
            "allow_overfilled": False,
        }
    }


def _clients() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"client_id": "C1", "client_name": "A", "sales_rep": "TP_01", "lat": 42.70, "lon": 23.32, "visit_frequency": 2},
            {"client_id": "C2", "client_name": "B", "sales_rep": "TP_01", "lat": 42.71, "lon": 23.33, "visit_frequency": 2},
        ]
    )


def _schedule() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "day_index": 0,
                "week_index": 1,
                "weekday": "Monday",
                "sales_rep": "TP_01",
                "route_order": 1,
                "client_id": "C1",
                "client_name": "A",
                "lat": 42.70,
                "lon": 23.32,
                "visit_frequency": 2,
                "cluster_id": "Center",
                "route_km_total": 12.5,
            },
            {
                "day_index": 1,
                "week_index": 1,
                "weekday": "Tuesday",
                "sales_rep": "TP_01",
                "route_order": 1,
                "client_id": "C2",
                "client_name": "B",
                "lat": 42.71,
                "lon": 23.33,
                "visit_frequency": 2,
                "cluster_id": "Center",
                "route_km_total": 9.0,
            },
            {
                "day_index": 10,
                "week_index": 3,
                "weekday": "Monday",
                "sales_rep": "TP_01",
                "route_order": 1,
                "client_id": "C1",
                "client_name": "A",
                "lat": 42.70,
                "lon": 23.32,
                "visit_frequency": 2,
                "cluster_id": "Center",
                "route_km_total": 11.0,
            },
            {
                "day_index": 11,
                "week_index": 3,
                "weekday": "Tuesday",
                "sales_rep": "TP_01",
                "route_order": 1,
                "client_id": "C2",
                "client_name": "B",
                "lat": 42.71,
                "lon": 23.33,
                "visit_frequency": 2,
                "cluster_id": "Center",
                "route_km_total": 8.5,
            },
        ]
    )


def test_audit_final_schedule_reads_exported_workbook(tmp_path: Path) -> None:
    input_path = tmp_path / "clients.xlsx"
    output_path = tmp_path / "final_schedule.xlsx"
    clients = _clients()
    schedule = _schedule()
    clients.to_excel(input_path, index=False)
    selected = schedule[["day_index", "week_index", "weekday", "sales_rep", "route_km_total"]].copy()
    selected["candidate_id"] = [f"R{i}" for i in range(len(selected))]
    selected["selected_candidate_id"] = selected["candidate_id"]
    selected["number_of_clients"] = 1
    selected["route_km"] = selected["route_km_total"]
    summary = pd.DataFrame(
        [
            {
                "sales_rep": "TP_01",
                "required_monthly_visits": 4,
                "planned_monthly_visits": 4,
            }
        ]
    )
    coverage = clients[["sales_rep", "client_id", "client_name", "visit_frequency"]].copy()
    coverage["number_of_candidates_containing_client"] = 3
    coverage["severity"] = "OK"

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        schedule.to_excel(writer, sheet_name="Final_Schedule", index=False)
        summary.to_excel(writer, sheet_name="Summary_By_Sales_Rep", index=False)
        selected.to_excel(writer, sheet_name="Candidate_Routes_Selected", index=False)
        coverage.to_excel(writer, sheet_name="Candidate_Coverage", index=False)

    audit = audit_final_schedule(str(output_path), str(input_path))

    assert audit["passed"] is True
    assert audit["checks"]["frequency_correctness"]["status"] == "PASS"
    assert audit["summary"]["planned_monthly_visits"] == 4


def test_wrong_sales_rep_is_fail() -> None:
    schedule = _schedule()
    schedule.loc[0, "sales_rep"] = "TP_02"

    audit = audit_schedule(schedule, _clients(), config=_config())

    assert audit["status"] == "FAIL"
    assert audit["checks"]["sales_rep_consistency"]["status"] == "FAIL"


def test_missing_route_km_is_fail() -> None:
    schedule = _schedule()
    schedule.loc[0, "route_km_total"] = None

    audit = audit_schedule(schedule, _clients(), config=_config())

    assert audit["status"] == "FAIL"
    assert audit["checks"]["route_km"]["status"] == "FAIL"


def test_bad_coordinates_dataset_is_caught_by_input_audit() -> None:
    df = generate_synthetic_clients(3, 60, "bad_coordinates", "", random_seed=123)

    issues = validate_input_clients_for_audit(df)
    fields = {issue.get("field") for issue in issues if issue["severity"] == "ERROR"}

    assert {"lat", "lon", "lat_lon", "client_id", "visit_frequency"}.issubset(fields)
