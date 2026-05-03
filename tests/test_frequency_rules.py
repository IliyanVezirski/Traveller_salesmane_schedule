import pandas as pd

from src.result_audit import audit_schedule


def _calendar() -> pd.DataFrame:
    weekdays = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]
    rows = []
    for week in range(1, 5):
        for weekday_index, weekday in enumerate(weekdays):
            rows.append(
                {
                    "day_index": len(rows),
                    "week_index": week,
                    "weekday": weekday,
                    "weekday_index": weekday_index,
                }
            )
    return pd.DataFrame(rows)


def _audit_config() -> dict:
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
            {
                "client_id": "C1",
                "client_name": "Freq 2",
                "sales_rep": "TP_01",
                "lat": 42.70,
                "lon": 23.32,
                "visit_frequency": 2,
            },
            {
                "client_id": "C2",
                "client_name": "Freq 4",
                "sales_rep": "TP_01",
                "lat": 42.71,
                "lon": 23.33,
                "visit_frequency": 4,
            },
            {
                "client_id": "C3",
                "client_name": "Freq 8",
                "sales_rep": "TP_01",
                "lat": 42.72,
                "lon": 23.34,
                "visit_frequency": 8,
            },
        ]
    )


def _schedule() -> pd.DataFrame:
    rows = []
    visits = {
        "C1": [0, 10],
        "C2": [1, 6, 11, 16],
        "C3": [2, 3, 7, 8, 12, 13, 17, 18],
    }
    names = {"C1": "Freq 2", "C2": "Freq 4", "C3": "Freq 8"}
    freqs = {"C1": 2, "C2": 4, "C3": 8}
    calendar = _calendar().set_index("day_index").to_dict("index")
    for client_id, days in visits.items():
        for day in days:
            rows.append(
                {
                    "day_index": day,
                    "week_index": calendar[day]["week_index"],
                    "weekday": calendar[day]["weekday"],
                    "sales_rep": "TP_01",
                    "route_order": 1,
                    "client_id": client_id,
                    "client_name": names[client_id],
                    "lat": 42.70,
                    "lon": 23.32,
                    "visit_frequency": freqs[client_id],
                    "cluster_id": "Center",
                    "route_km_total": 10.0,
                }
            )
    return pd.DataFrame(rows)


def test_frequency_rules_pass_for_valid_schedule() -> None:
    audit = audit_schedule(_schedule(), _clients(), _calendar(), _audit_config())

    assert audit["status"] == "PASS"
    assert audit["passed"] is True
    assert audit["checks"]["frequency_correctness"]["status"] == "PASS"


def test_frequency_rules_fail_when_weekly_visit_is_missing() -> None:
    schedule = _schedule()
    schedule = schedule[~((schedule["client_id"] == "C2") & (schedule["week_index"] == 3))].copy()

    audit = audit_schedule(schedule, _clients(), _calendar(), _audit_config())

    assert audit["status"] == "FAIL"
    assert audit["checks"]["frequency_correctness"]["status"] == "FAIL"
    assert any("Expected 1 visits in week 3" in issue["message"] for issue in audit["errors"])


def test_duplicate_same_day_visit_is_fail() -> None:
    schedule = pd.concat([_schedule(), _schedule().head(1)], ignore_index=True)

    audit = audit_schedule(schedule, _clients(), _calendar(), _audit_config())

    assert audit["status"] == "FAIL"
    assert audit["checks"]["duplicate_same_day"]["status"] == "FAIL"
