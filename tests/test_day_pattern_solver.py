import pandas as pd

from src.calendar_builder import build_calendar
from src.day_pattern_solver import solve_day_pattern_master


def _calendar() -> pd.DataFrame:
    return build_calendar(
        {
            "working_days": {
                "weeks": 4,
                "weekdays": ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"],
            }
        }
    )


def _config() -> dict:
    return {
        "working_days": {
            "weeks": 4,
            "weekdays": ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"],
        },
        "weekday_consistency": {"frequency_4_same_weekday": True},
        "daily_route": {
            "target_clients": 2,
            "min_clients": 1,
            "max_clients": 3,
            "allow_underfilled": True,
            "allow_overfilled": False,
        },
        "candidate_routes": {"random_seed": 42},
        "optimization": {
            "time_limit_seconds": 10,
            "num_workers": 1,
            "log_search_progress": False,
            "stop_after_first_solution": False,
        },
        "weights": {
            "route_km": 1,
            "underfilled_route": 0,
            "over_target_clients": 0,
            "bad_spacing_frequency_8": 0,
            "bad_spacing_frequency_2": 0,
            "cluster_mixing": 0,
            "territory_weekday_violation": 1000,
            "territory_client_weekday_violation": 1000,
            "preferred_weekday_violation": 100,
        },
    }


def _clients() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "client_id": "C1",
                "client_name": "Fixed Friday",
                "sales_rep": "Rep A",
                "lat": 42.70,
                "lon": 23.32,
                "visit_frequency": 4,
                "fixed_weekday": "Friday",
                "forbidden_weekdays": pd.NA,
                "preferred_weekdays": pd.NA,
                "cluster_id": "A",
                "territory_weekday_index": 4,
            },
            {
                "client_id": "C2",
                "client_name": "Territory Monday",
                "sales_rep": "Rep A",
                "lat": 42.71,
                "lon": 23.33,
                "visit_frequency": 4,
                "fixed_weekday": pd.NA,
                "forbidden_weekdays": pd.NA,
                "preferred_weekdays": pd.NA,
                "cluster_id": "B",
                "territory_weekday_index": 0,
            },
        ]
    )


def test_day_pattern_solver_assigns_clients_to_days_before_routing() -> None:
    result = solve_day_pattern_master(_clients(), _calendar(), _config())

    assert result["status"] == "OPTIMAL"
    selected = result["selected_candidates"]
    assert not selected.empty
    visits = selected.explode("client_ids")
    friday_visits = visits[visits["client_ids"].eq("C1")]
    monday_visits = visits[visits["client_ids"].eq("C2")]

    assert len(friday_visits) == 4
    assert set(friday_visits["weekday"]) == {"Friday"}
    assert len(monday_visits) == 4
    assert set(monday_visits["weekday"]) == {"Monday"}


def test_day_pattern_solver_keeps_frequency2_on_same_weekday() -> None:
    config = _config()
    config["weekday_consistency"]["frequency_2_same_weekday"] = True
    clients = pd.DataFrame(
        [
            {
                "client_id": "C1",
                "client_name": "Twice monthly",
                "sales_rep": "Rep A",
                "lat": 42.70,
                "lon": 23.32,
                "visit_frequency": 2,
                "fixed_weekday": pd.NA,
                "forbidden_weekdays": pd.NA,
                "preferred_weekdays": pd.NA,
                "cluster_id": "A",
                "territory_weekday_index": 0,
            }
        ]
    )

    result = solve_day_pattern_master(clients, _calendar(), config)

    assert result["status"] == "OPTIMAL"
    visits = result["selected_candidates"].explode("client_ids")
    assert visits["weekday"].nunique() == 1
