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


def _single_client_config() -> dict:
    return _config(
        {
            "route_km": 1,
            "underfilled_route": 0,
            "over_target_clients": 0,
            "bad_spacing_frequency_8": 0,
            "bad_spacing_frequency_2": 0,
            "cluster_mixing": 0,
            "fixed_weekday_violation": 0,
            "preferred_weekday_violation": 0,
        }
    ) | {"weekday_consistency": {"frequency_4_same_weekday": True}}


def _single_client(freq: int = 4, fixed_weekday: str | None = None) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "client_id": "C1",
                "client_name": "Client 1",
                "sales_rep": "Rep A",
                "lat": 42.70,
                "lon": 23.32,
                "visit_frequency": freq,
                "fixed_weekday": fixed_weekday if fixed_weekday is not None else pd.NA,
                "forbidden_weekdays": pd.NA,
                "preferred_weekdays": pd.NA,
            }
        ]
    )


def _single_client_candidate() -> pd.DataFrame:
    candidate = _candidate("single_client_route", route_km=1.0)
    candidate["client_ids"] = ["C1"]
    candidate["number_of_clients"] = 1
    return pd.DataFrame([candidate])


def test_frequency_4_uses_same_weekday_across_all_weeks() -> None:
    result = solve_pvrp_master(_single_client(), _calendar(), _single_client_candidate(), _single_client_config())

    assert result["status"] == "OPTIMAL"
    selected = result["selected_candidates"]
    assert len(selected) == 4
    assert selected["week_index"].nunique() == 4
    assert selected["weekday"].nunique() == 1


def test_fixed_weekday_is_a_hard_constraint() -> None:
    result = solve_pvrp_master(_single_client(fixed_weekday="Friday"), _calendar(), _single_client_candidate(), _single_client_config())

    assert result["status"] == "OPTIMAL"
    selected = result["selected_candidates"]
    assert len(selected) == 4
    assert set(selected["weekday"]) == {"Friday"}


def test_territory_weekday_is_strongly_preferred() -> None:
    config = _single_client_config()
    config["weights"]["territory_weekday_violation"] = 200_000
    candidate = _single_client_candidate()
    candidate["territory_weekday_index"] = 0
    candidate["territory_mixing_penalty"] = 0

    result = solve_pvrp_master(_single_client(), _calendar(), candidate, config)

    assert result["status"] == "OPTIMAL"
    selected = result["selected_candidates"]
    assert len(selected) == 4
    assert set(selected["weekday"]) == {"Monday"}


def test_client_territory_weekday_is_strongly_preferred() -> None:
    config = _single_client_config()
    config["weights"]["territory_client_weekday_violation"] = 200_000
    client = _single_client()
    client["territory_weekday_index"] = 2
    client["territory_weekday"] = "Wednesday"

    result = solve_pvrp_master(client, _calendar(), _single_client_candidate(), config)

    assert result["status"] == "OPTIMAL"
    selected = result["selected_candidates"]
    assert len(selected) == 4
    assert set(selected["weekday"]) == {"Wednesday"}


def test_solver_falls_back_to_periodic_seed_when_cp_sat_finds_no_solution() -> None:
    config = _single_client_config()
    config["optimization"]["time_limit_seconds"] = 0.0
    seed_candidates = []
    for day in [0, 5, 10, 15]:
        candidate = _candidate(f"seed_{day}", route_km=1.0)
        candidate["client_ids"] = ["C1"]
        candidate["number_of_clients"] = 1
        candidate["generation_method"] = "periodic_seed"
        candidate["intended_day_index"] = day
        seed_candidates.append(candidate)

    result = solve_pvrp_master(_single_client(), _calendar(), pd.DataFrame(seed_candidates), config)

    assert result["status"] == "FEASIBLE_SEED"
    selected = result["selected_candidates"]
    assert len(selected) == 4
    assert set(selected["day_index"]) == {0, 5, 10, 15}
    assert set(selected["weekday"]) == {"Monday"}
