import pandas as pd

from src.calendar_builder import build_calendar
from src.pvrp_master_solver import solve_pvrp_master


def _calendar() -> pd.DataFrame:
    return build_calendar(
        {
            "working_days": {
                "weeks": 4,
                "weekdays": ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"],
            }
        }
    )


def _clients() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "client_id": "C1",
                "client_name": "Client 1",
                "sales_rep": "Rep A",
                "lat": 42.70,
                "lon": 23.32,
                "visit_frequency": 2,
                "fixed_weekday": pd.NA,
                "forbidden_weekdays": pd.NA,
                "preferred_weekdays": pd.NA,
            },
            {
                "client_id": "C2",
                "client_name": "Client 2",
                "sales_rep": "Rep A",
                "lat": 42.71,
                "lon": 23.33,
                "visit_frequency": 2,
                "fixed_weekday": pd.NA,
                "forbidden_weekdays": pd.NA,
                "preferred_weekdays": pd.NA,
            },
        ]
    )


def _config(weights: dict[str, int]) -> dict:
    base_weights = {
        "route_km": 1000,
        "underfilled_route": 0,
        "over_target_clients": 0,
        "bad_spacing_frequency_8": 0,
        "bad_spacing_frequency_2": 0,
        "cluster_mixing": 0,
        "unused_day": 0,
        "fixed_weekday_violation": 0,
        "preferred_weekday_violation": 0,
    }
    base_weights.update(weights)
    return {
        "working_days": {
            "weeks": 4,
            "weekdays": ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"],
        },
        "daily_route": {
            "target_clients": 2,
            "min_clients": 1,
            "max_clients": 3,
            "allow_underfilled": True,
            "allow_overfilled": False,
        },
        "optimization": {
            "time_limit_seconds": 10,
            "num_workers": 1,
            "log_search_progress": False,
            "stop_after_first_solution": False,
            "decompose_by_sales_rep": False,
        },
        "weights": base_weights,
    }


def _candidate(
    candidate_id: str,
    route_km: float,
    *,
    cluster_mixing_penalty: int = 0,
    underfilled_penalty: int = 0,
    overfilled_penalty: int = 0,
) -> dict:
    return {
        "candidate_id": candidate_id,
        "sales_rep": "Rep A",
        "client_ids": ["C1", "C2"],
        "number_of_clients": 2,
        "route_km": route_km,
        "route_duration_min": None,
        "main_cluster": "A",
        "clusters_used": "A",
        "cluster_count": 1 + cluster_mixing_penalty,
        "generation_method": "test_fixture",
        "underfilled_penalty": underfilled_penalty,
        "overfilled_penalty": overfilled_penalty,
        "cluster_mixing_penalty": cluster_mixing_penalty,
    }


def _selected_ids(result: dict) -> set[str]:
    selected = result["selected_candidates"]
    assert not selected.empty
    return set(selected["selected_candidate_id"].astype(str))


def test_master_solver_prefers_lower_route_km_when_candidates_are_equivalent() -> None:
    candidates = pd.DataFrame(
        [
            _candidate("short_route", route_km=10.0),
            _candidate("long_route", route_km=50.0),
        ]
    )

    result = solve_pvrp_master(_clients(), _calendar(), candidates, _config({"route_km": 1000}))

    assert result["status"] == "OPTIMAL"
    assert _selected_ids(result) == {"short_route"}
    assert result["selected_candidates"]["route_km"].eq(10.0).all()


def test_master_solver_uses_total_objective_not_route_km_alone() -> None:
    candidates = pd.DataFrame(
        [
            _candidate("short_but_mixed", route_km=1.0, cluster_mixing_penalty=10),
            _candidate("longer_but_compact", route_km=5.0, cluster_mixing_penalty=0),
        ]
    )

    result = solve_pvrp_master(
        _clients(),
        _calendar(),
        candidates,
        _config({"route_km": 1, "cluster_mixing": 100}),
    )

    assert result["status"] == "OPTIMAL"
    assert _selected_ids(result) == {"longer_but_compact"}
    assert result["selected_candidates"]["route_km"].eq(5.0).all()
