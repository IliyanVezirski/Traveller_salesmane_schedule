import numpy as np
import pandas as pd

from src.clustering import _cluster_order_by_matrix, assign_global_weekday_territories, cluster_clients


def _config() -> dict:
    return {
        "working_days": {"weeks": 4, "weekdays": ["Monday", "Tuesday", "Wednesday"]},
        "daily_route": {"target_clients": 23, "max_clients": 27},
        "candidate_routes": {"random_seed": 42},
        "clustering": {"use_distance_matrix": True, "k_medoids_max_iterations": 20},
        "territory_days": {"enabled": True, "use_distance_matrix": True},
    }


def test_cluster_clients_uses_aligned_distance_matrix_not_lat_lon() -> None:
    clients = pd.DataFrame(
        [
            {"client_id": "A1", "client_name": "A1", "sales_rep": "Rep", "lat": 0.0, "lon": 0.0, "visit_frequency": 4},
            {"client_id": "B1", "client_name": "B1", "sales_rep": "Rep", "lat": 0.0, "lon": 1.0, "visit_frequency": 4},
            {"client_id": "C1", "client_name": "C1", "sales_rep": "Rep", "lat": 0.0, "lon": 2.0, "visit_frequency": 4},
            {"client_id": "A2", "client_name": "A2", "sales_rep": "Rep", "lat": 10.0, "lon": 0.0, "visit_frequency": 4},
            {"client_id": "B2", "client_name": "B2", "sales_rep": "Rep", "lat": 10.0, "lon": 1.0, "visit_frequency": 4},
            {"client_id": "C2", "client_name": "C2", "sales_rep": "Rep", "lat": 10.0, "lon": 2.0, "visit_frequency": 4},
        ]
    )
    matrix_ids = ["A1", "A2", "B1", "B2", "C1", "C2"]
    matrix = np.full((6, 6), 100_000.0)
    np.fill_diagonal(matrix, 0.0)
    for left, right in [("A1", "A2"), ("B1", "B2"), ("C1", "C2")]:
        i, j = matrix_ids.index(left), matrix_ids.index(right)
        matrix[i, j] = matrix[j, i] = 100.0

    clustered = cluster_clients(clients, matrix, _config(), matrix_ids)
    cluster_lookup = clustered.set_index("client_id")["cluster_id"].astype(str).to_dict()

    assert cluster_lookup["A1"] == cluster_lookup["A2"]
    assert cluster_lookup["B1"] == cluster_lookup["B2"]
    assert cluster_lookup["C1"] == cluster_lookup["C2"]
    assert clustered["cluster_id"].nunique() == 3


def test_cluster_order_uses_road_distance_between_cluster_medoids() -> None:
    out = pd.DataFrame(
        [
            {"client_id": "A", "cluster_id": "A", "lat": 0.0, "lon": 0.0, "visit_frequency": 4},
            {"client_id": "B", "cluster_id": "B", "lat": 0.0, "lon": 1.0, "visit_frequency": 4},
            {"client_id": "C", "cluster_id": "C", "lat": 0.0, "lon": 2.0, "visit_frequency": 4},
        ]
    )
    matrix = np.array(
        [
            [0.0, 100.0, 10.0],
            [100.0, 0.0, 10.0],
            [10.0, 10.0, 0.0],
        ]
    )

    assert _cluster_order_by_matrix(out, matrix) in (["A", "C", "B"], ["B", "C", "A"])


def test_global_weekday_territories_are_shared_across_sales_reps() -> None:
    config = {
        "working_days": {"weeks": 4, "weekdays": ["Monday", "Tuesday"]},
        "daily_route": {"target_clients": 10, "max_clients": 20},
        "candidate_routes": {"random_seed": 42},
        "clustering": {"use_distance_matrix": True, "k_medoids_max_iterations": 20},
        "territory_days": {
            "enabled": True,
            "scope": "global",
            "use_distance_matrix": True,
            "global_cluster_count": 2,
        },
    }
    clients = pd.DataFrame(
        [
            {"client_id": "A_west", "client_name": "A_west", "sales_rep": "A", "lat": 0.0, "lon": 0.0, "visit_frequency": 4},
            {"client_id": "B_west", "client_name": "B_west", "sales_rep": "B", "lat": 0.0, "lon": 0.1, "visit_frequency": 4},
            {"client_id": "A_east", "client_name": "A_east", "sales_rep": "A", "lat": 0.0, "lon": 10.0, "visit_frequency": 4},
            {"client_id": "B_east", "client_name": "B_east", "sales_rep": "B", "lat": 0.0, "lon": 10.1, "visit_frequency": 4},
        ]
    )

    global_clients = assign_global_weekday_territories(clients, config)
    global_lookup = global_clients.set_index("client_id")["global_territory_weekday_index"].astype(int).to_dict()

    assert global_lookup["A_west"] == global_lookup["B_west"]
    assert global_lookup["A_east"] == global_lookup["B_east"]
    assert global_lookup["A_west"] != global_lookup["A_east"]

    for _, rep_df in global_clients.groupby("sales_rep"):
        matrix = np.ones((len(rep_df), len(rep_df))) * 100.0
        np.fill_diagonal(matrix, 0.0)
        clustered = cluster_clients(rep_df, matrix, config, rep_df["client_id"].astype(str).tolist())
        for row in clustered.itertuples(index=False):
            assert int(row.territory_weekday_index) == int(row.global_territory_weekday_index)
