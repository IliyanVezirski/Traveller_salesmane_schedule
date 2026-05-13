import numpy as np
import pandas as pd

from src.calendar_builder import build_calendar
from src.scoring import validate_solution
from src.selective_day_scheduler import _client_priority, _compactness_strength, _effective_pool_size, solve_selective_day_schedule


def _config() -> dict:
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
        "candidate_routes": {"random_seed": 42},
        "route_costing": {
            "method": "nearest_neighbor_2opt",
            "route_type": "open",
            "pyvrp_max_iterations": 50,
            "pyvrp_time_limit_seconds": 1,
        },
        "selective_day_routing": {
            "enabled": True,
            "pool_size": 6,
            "prize_base": 100000,
            "urgency_bonus": 5000,
            "territory_bonus": 2000,
            "preferred_bonus": 1000,
            "distance_penalty": 250,
            "pyvrp_max_iterations": 50,
        },
        "weights": {
            "route_km": 1,
            "underfilled_route": 0,
            "over_target_clients": 0,
            "preferred_weekday_violation": 0,
        },
    }


def _clients() -> pd.DataFrame:
    rows = []
    for idx in range(4):
        rows.append(
            {
                "client_id": f"C{idx}",
                "client_name": f"Client {idx}",
                "sales_rep": "Rep A",
                "lat": 42.70 + idx * 0.001,
                "lon": 23.30 + idx * 0.001,
                "visit_frequency": 4,
                "fixed_weekday": "Monday" if idx < 2 else "Tuesday",
                "forbidden_weekdays": pd.NA,
                "preferred_weekdays": pd.NA,
                "cluster_id": "A" if idx < 2 else "B",
                "territory_weekday_index": 0 if idx < 2 else 1,
            }
        )
    return pd.DataFrame(rows)


def _matrix(client_ids: list[str]) -> dict:
    coords = np.arange(len(client_ids), dtype=float)
    matrix = np.abs(coords[:, None] - coords[None, :]) * 1000
    return {
        "client_ids": client_ids,
        "distance_matrix_m": matrix,
        "duration_matrix_s": matrix,
        "source": "test",
    }


def test_selective_day_scheduler_lets_pyvrp_pick_from_daily_pool() -> None:
    clients = _clients()
    calendar = build_calendar(_config())
    result = solve_selective_day_schedule(clients, calendar, {"Rep A": _matrix(clients["client_id"].tolist())}, _config())

    assert result["status"] == "FEASIBLE"
    selected = result["selected_candidates"]
    assert not selected.empty
    assert selected["generation_method"].eq("pyvrp_optional_select").any()

    exploded = selected.explode("client_ids")
    assert exploded.groupby("client_ids").size().to_dict() == {"C0": 4, "C1": 4, "C2": 4, "C3": 4}
    assert set(exploded[exploded["client_ids"].isin(["C0", "C1"])]["weekday"]) == {"Monday"}
    assert set(exploded[exploded["client_ids"].isin(["C2", "C3"])]["weekday"]) == {"Tuesday"}


def test_frequency4_weekdays_are_balanced_before_daily_pyvrp_selection() -> None:
    config = _config()
    config["daily_route"].update({"target_clients": 8, "max_clients": 10})
    config["selective_day_routing"]["pool_size"] = 40
    clients = pd.DataFrame(
        [
            {
                "client_id": f"C{idx}",
                "client_name": f"Client {idx}",
                "sales_rep": "Rep A",
                "lat": 42.70 + idx * 0.0001,
                "lon": 23.30 + idx * 0.0001,
                "visit_frequency": 4,
                "fixed_weekday": pd.NA,
                "forbidden_weekdays": pd.NA,
                "preferred_weekdays": pd.NA,
                "cluster_id": "A",
                "territory_weekday_index": 4,
            }
            for idx in range(30)
        ]
    )

    result = solve_selective_day_schedule(clients, build_calendar(config), {"Rep A": _matrix(clients["client_id"].tolist())}, config)

    assert result["status"] == "FEASIBLE"
    selected = result["selected_candidates"].explode("client_ids")
    week3 = selected[selected["week_index"].eq(3)]
    assert int(week3.groupby("day_index").size().max()) <= 10
    assert int(week3[week3["weekday"].eq("Friday")].shape[0]) <= 10


def test_client_priority_strongly_prefers_own_territory_day() -> None:
    config = _config()
    config["selective_day_routing"]["territory_mismatch_penalty"] = 25_000
    config["selective_day_routing"]["distance_penalty"] = 1_200
    client = {
        "client_id": "C0",
        "lat": 42.70,
        "lon": 23.30,
        "visit_frequency": 2,
        "preferred_weekdays": pd.NA,
        "territory_weekday_index": 0,
    }
    state = {"remaining_total": 2, "selected_days": []}
    centers = {0: (42.70, 23.30), 1: (42.70, 23.30)}

    own_day = _client_priority(client, {"day_index": 0, "weekday": "Monday", "weekday_index": 0}, state, centers, config)
    other_day = _client_priority(client, {"day_index": 1, "weekday": "Tuesday", "weekday_index": 1}, state, centers, config)

    assert own_day > other_day + 20_000


def test_compactness_strength_scales_pool_and_geography_penalty() -> None:
    config = _config()
    config["daily_route"].update({"target_clients": 23, "max_clients": 27})
    config["selective_day_routing"].update(
        {
            "compactness_strength": 2.0,
            "pool_size": 45,
            "territory_mismatch_penalty": 40_000,
            "distance_penalty": 2_000,
        }
    )
    client = {
        "client_id": "C0",
        "lat": 42.70,
        "lon": 23.30,
        "visit_frequency": 2,
        "preferred_weekdays": pd.NA,
        "territory_weekday_index": 0,
    }
    state = {"remaining_total": 2, "selected_days": []}
    centers = {0: (42.70, 23.30), 1: (42.70, 23.30)}

    other_day = _client_priority(client, {"day_index": 1, "weekday": "Tuesday", "weekday_index": 1}, state, centers, config)
    config["selective_day_routing"]["compactness_strength"] = 1.0
    baseline_other_day = _client_priority(client, {"day_index": 1, "weekday": "Tuesday", "weekday_index": 1}, state, centers, config)

    config["selective_day_routing"]["compactness_strength"] = 2.0
    assert _compactness_strength(config) == 2.0
    assert _effective_pool_size(config) == 28
    assert baseline_other_day > other_day + 35_000


def test_frequency2_keeps_same_weekday_after_first_visit() -> None:
    config = _config()
    config["weekday_consistency"] = {"frequency_2_same_weekday": True}
    config["daily_route"].update({"target_clients": 1, "min_clients": 1, "max_clients": 1})
    clients = pd.DataFrame(
        [
            {
                "client_id": "C1",
                "client_name": "Twice monthly",
                "sales_rep": "Rep A",
                "lat": 42.70,
                "lon": 23.30,
                "visit_frequency": 2,
                "fixed_weekday": pd.NA,
                "forbidden_weekdays": pd.NA,
                "preferred_weekdays": pd.NA,
                "cluster_id": "A",
                "territory_weekday_index": 0,
            }
        ]
    )

    result = solve_selective_day_schedule(clients, build_calendar(config), {"Rep A": _matrix(["C1"])}, config)

    assert result["status"] == "FEASIBLE"
    visits = result["selected_candidates"].explode("client_ids")
    assert visits["client_ids"].eq("C1").sum() == 2
    assert visits.loc[visits["client_ids"].eq("C1"), "weekday"].nunique() == 1


def test_frequency8_keeps_same_weekday_pair_across_weeks() -> None:
    config = _config()
    config["weekday_consistency"] = {"frequency_8_same_weekday_pair": True}
    config["daily_route"].update({"target_clients": 1, "min_clients": 1, "max_clients": 1})
    clients = pd.DataFrame(
        [
            {
                "client_id": "C1",
                "client_name": "Twice weekly",
                "sales_rep": "Rep A",
                "lat": 42.70,
                "lon": 23.30,
                "visit_frequency": 8,
                "fixed_weekday": pd.NA,
                "forbidden_weekdays": pd.NA,
                "preferred_weekdays": pd.NA,
                "cluster_id": "A",
                "territory_weekday_index": 0,
            }
        ]
    )

    result = solve_selective_day_schedule(clients, build_calendar(config), {"Rep A": _matrix(["C1"])}, config)

    assert result["status"] == "FEASIBLE"
    visits = result["selected_candidates"].explode("client_ids")
    weekly_patterns = visits.groupby("week_index")["weekday"].apply(lambda series: tuple(sorted(series.astype(str)))).to_dict()
    assert len(weekly_patterns) == 4
    assert len(set(weekly_patterns.values())) == 1
