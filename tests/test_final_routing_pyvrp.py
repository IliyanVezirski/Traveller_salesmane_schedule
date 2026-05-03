import numpy as np
import pandas as pd
import pytest

from src.final_routing import optimize_selected_daily_routes


pytest.importorskip("pyvrp")


def test_final_routing_uses_pyvrp_when_configured() -> None:
    config = {
        "candidate_routes": {"random_seed": 42},
        "route_costing": {
            "final_method": "pyvrp",
            "pyvrp_time_limit_seconds": 1,
            "route_type": "open",
        },
    }
    clients = pd.DataFrame(
        [
            {"client_id": "A", "client_name": "A", "sales_rep": "Rep", "lat": 0.0, "lon": 0.0, "visit_frequency": 4, "cluster_id": "1"},
            {"client_id": "B", "client_name": "B", "sales_rep": "Rep", "lat": 0.0, "lon": 1.0, "visit_frequency": 4, "cluster_id": "1"},
            {"client_id": "C", "client_name": "C", "sales_rep": "Rep", "lat": 0.0, "lon": 2.0, "visit_frequency": 4, "cluster_id": "1"},
            {"client_id": "D", "client_name": "D", "sales_rep": "Rep", "lat": 0.0, "lon": 3.0, "visit_frequency": 4, "cluster_id": "1"},
        ]
    )
    selected = pd.DataFrame(
        [
            {
                "sales_rep": "Rep",
                "client_ids": ["A", "B", "C", "D"],
                "day_index": 0,
                "week_index": 1,
                "weekday": "Monday",
                "selected_candidate_id": "cand",
            }
        ]
    )
    matrix = np.array(
        [
            [0, 10, 50, 60],
            [10, 0, 10, 50],
            [50, 10, 0, 10],
            [60, 50, 10, 0],
        ],
        dtype=float,
    )
    matrix_data_by_rep = {
        "Rep": {
            "client_ids": ["A", "B", "C", "D"],
            "distance_matrix_m": matrix,
            "duration_matrix_s": matrix,
        }
    }

    result = optimize_selected_daily_routes(selected, clients, matrix_data_by_rep, config)

    assert set(result["final_route_method"]) == {"pyvrp"}
    assert result["route_km_total"].iloc[0] == pytest.approx(0.03)
