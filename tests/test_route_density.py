import pandas as pd

from src.result_audit import audit_schedule


def test_long_route_relative_to_rep_median_is_warning() -> None:
    clients = pd.DataFrame(
        [
            {"client_id": "C1", "client_name": "A", "sales_rep": "TP_01", "lat": 42.70, "lon": 23.32, "visit_frequency": 2},
            {"client_id": "C2", "client_name": "B", "sales_rep": "TP_01", "lat": 42.71, "lon": 23.33, "visit_frequency": 2},
            {"client_id": "C3", "client_name": "C", "sales_rep": "TP_01", "lat": 42.72, "lon": 23.34, "visit_frequency": 2},
        ]
    )
    rows = [
        ("C1", 0, 1, 10.0),
        ("C2", 0, 1, 10.0),
        ("C2", 5, 2, 11.0),
        ("C3", 5, 2, 11.0),
        ("C1", 10, 3, 40.0),
        ("C3", 10, 3, 40.0),
    ]
    schedule = pd.DataFrame(
        [
            {
                "client_id": client_id,
                "client_name": client_id,
                "sales_rep": "TP_01",
                "day_index": day,
                "week_index": week,
                "weekday": "Monday",
                "route_order": index + 1,
                "lat": 42.70,
                "lon": 23.32,
                "visit_frequency": 2,
                "cluster_id": "Center",
                "route_km_total": route_km,
            }
            for index, (client_id, day, week, route_km) in enumerate(rows)
        ]
    )
    config = {"daily_route": {"target_clients": 2, "min_clients": 1, "max_clients": 3}}

    audit = audit_schedule(schedule, clients, config=config)

    assert audit["status"] == "WARNING"
    assert audit["passed"] is True
    assert audit["checks"]["route_density"]["status"] == "WARNING"
