"""Final daily route ordering for selected candidate routes."""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from .route_costing import calculate_route_cost, distances_for_order


def _duration_for_order(route_order: list[str], matrix_data: dict[str, Any], route_type: str) -> float | None:
    """Calculate route duration in minutes for a chosen order, when available."""
    duration_matrix = matrix_data.get("duration_matrix_s")
    if duration_matrix is None or len(route_order) <= 1:
        return None
    id_to_idx = {cid: idx for idx, cid in enumerate(matrix_data["client_ids"])}
    duration = np.asarray(duration_matrix, dtype=float)
    order_idx = [id_to_idx[cid] for cid in route_order]
    duration_s = sum(float(duration[order_idx[i], order_idx[i + 1]]) for i in range(len(order_idx) - 1))
    if route_type == "closed":
        duration_s += float(duration[order_idx[-1], order_idx[0]])
    return duration_s / 60.0


def _open_route_km(route_order: list[str], matrix_data: dict[str, Any]) -> float:
    """Calculate open route distance in kilometers for a chosen order."""
    return float(sum(distances_for_order(route_order, matrix_data)))


def _calculate_pyvrp_open_route_cost(client_ids: list[str], matrix_data: dict[str, Any], config: dict) -> dict[str, Any]:
    """Use PyVRP to optimize an open daily route with a zero-cost dummy depot."""
    try:
        from pyvrp import Model
        from pyvrp.stop import MaxIterations, MaxRuntime
    except ImportError as exc:
        raise RuntimeError("PyVRP is not installed.") from exc

    if len(client_ids) <= 2:
        return calculate_route_cost(client_ids, matrix_data, "nearest_neighbor_2opt", "open") | {"final_route_method": "nearest_neighbor_2opt"}

    id_to_idx = {cid: idx for idx, cid in enumerate(matrix_data["client_ids"])}
    distance_matrix = np.asarray(matrix_data["distance_matrix_m"], dtype=float)

    model = Model()
    depot = model.add_depot(0, 0, name="dummy_open_route_depot")
    clients = [model.add_client(i + 1, 0, name=str(client_id)) for i, client_id in enumerate(client_ids)]
    model.add_vehicle_type(num_available=1, start_depot=depot, end_depot=depot)

    locations = [depot] + clients
    for i, frm in enumerate(locations):
        for j, to in enumerate(locations):
            if i == j:
                continue
            if i == 0 or j == 0:
                distance = 0
            else:
                from_client = client_ids[i - 1]
                to_client = client_ids[j - 1]
                distance = int(round(float(distance_matrix[id_to_idx[from_client], id_to_idx[to_client]])))
            model.add_edge(frm, to, distance=distance, duration=distance)

    max_iterations = int(config["route_costing"].get("pyvrp_max_iterations", 0) or 0)
    stop = MaxIterations(max_iterations) if max_iterations > 0 else MaxRuntime(float(config["route_costing"].get("pyvrp_time_limit_seconds", 3)))
    result = model.solve(stop, seed=int(config["candidate_routes"].get("random_seed", 42)), collect_stats=False, display=False)
    if not result.is_feasible():
        raise RuntimeError("PyVRP did not find a feasible final route.")

    routes = result.best.routes()
    if len(routes) != 1:
        raise RuntimeError(f"PyVRP returned {len(routes)} routes for one selected daily candidate.")

    visits = routes[0].visits()
    route_order = [client_ids[int(visit_idx) - 1] for visit_idx in visits]
    return {
        "route_order": route_order,
        "route_km": _open_route_km(route_order, matrix_data),
        "route_duration_min": _duration_for_order(route_order, matrix_data, "open"),
        "final_route_method": "pyvrp",
    }


def _calculate_final_route_cost(client_ids: list[str], matrix_data: dict[str, Any], config: dict) -> dict[str, Any]:
    """Calculate final route cost according to route_costing.final_method."""
    final_method = str(config["route_costing"].get("final_method", "nearest_neighbor_2opt")).lower()
    route_type = str(config["route_costing"].get("route_type", "open")).lower()

    if final_method == "pyvrp" and route_type == "open":
        try:
            return _calculate_pyvrp_open_route_cost(client_ids, matrix_data, config)
        except Exception:
            fallback = calculate_route_cost(client_ids, matrix_data, "nearest_neighbor_2opt", route_type)
            fallback["final_route_method"] = "nearest_neighbor_2opt_fallback_from_pyvrp"
            return fallback

    fallback = calculate_route_cost(client_ids, matrix_data, "nearest_neighbor_2opt", route_type)
    fallback["final_route_method"] = "nearest_neighbor_2opt" if final_method != "pyvrp" else "nearest_neighbor_2opt_fallback_closed_route"
    return fallback


def optimize_selected_daily_routes(selected_candidates_df: pd.DataFrame, clients_df: pd.DataFrame, matrix_data_by_rep: dict[str, dict[str, Any]], config: dict) -> pd.DataFrame:
    """Optimize stop order for every selected candidate/day and return stop-level rows."""
    rows: list[dict[str, Any]] = []
    client_lookup = clients_df.set_index("client_id").to_dict("index")
    for selected in selected_candidates_df.itertuples(index=False):
        sales_rep = str(selected.sales_rep)
        matrix_data = matrix_data_by_rep[sales_rep]
        cost = _calculate_final_route_cost(list(selected.client_ids), matrix_data, config)
        distances = distances_for_order(cost["route_order"], matrix_data)
        cumulative = 0.0
        for order_num, (client_id, leg_km) in enumerate(zip(cost["route_order"], distances), start=1):
            cumulative += leg_km
            client = client_lookup[str(client_id)]
            rows.append(
                {
                    "day_index": int(selected.day_index),
                    "week_index": int(selected.week_index),
                    "weekday": selected.weekday,
                    "sales_rep": sales_rep,
                    "selected_candidate_id": getattr(selected, "selected_candidate_id", getattr(selected, "candidate_id", None)),
                    "route_order": order_num,
                    "client_id": str(client_id),
                    "client_name": client["client_name"],
                    "lat": float(client["lat"]),
                    "lon": float(client["lon"]),
                    "visit_frequency": int(client["visit_frequency"]),
                    "cluster_id": client.get("cluster_id"),
                    "distance_from_previous_km": leg_km,
                    "cumulative_km": cumulative,
                    "route_km_total": float(cost["route_km"]),
                    "final_route_method": cost["final_route_method"],
                }
            )
    return pd.DataFrame(rows)
