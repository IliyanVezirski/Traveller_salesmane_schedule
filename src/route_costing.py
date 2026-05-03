"""Route construction and cost calculation for candidate routes."""

from __future__ import annotations

from typing import Any

import numpy as np


def _route_distance(order: list[int], matrix: np.ndarray, route_type: str = "open") -> float:
    if len(order) <= 1:
        return 0.0
    total = sum(float(matrix[order[i], order[i + 1]]) for i in range(len(order) - 1))
    if route_type == "closed":
        total += float(matrix[order[-1], order[0]])
    return total


def _nearest_neighbor(indices: list[int], matrix: np.ndarray) -> list[int]:
    if not indices:
        return []
    remaining = set(indices)
    current = min(indices)
    order = [current]
    remaining.remove(current)
    while remaining:
        nxt = min(remaining, key=lambda j: float(matrix[current, j]))
        order.append(nxt)
        remaining.remove(nxt)
        current = nxt
    return order


def _two_opt(order: list[int], matrix: np.ndarray, route_type: str) -> list[int]:
    if len(order) < 4:
        return order
    best = order[:]
    best_dist = _route_distance(best, matrix, route_type)
    improved = True
    while improved:
        improved = False
        for i in range(1, len(best) - 2):
            for j in range(i + 1, len(best)):
                if j - i == 1:
                    continue
                candidate = best[:i] + best[i:j][::-1] + best[j:]
                dist = _route_distance(candidate, matrix, route_type)
                if dist + 1e-6 < best_dist:
                    best, best_dist, improved = candidate, dist, True
    return best


def calculate_route_cost(client_ids: list[str], matrix_data: dict[str, Any], method: str = "nearest_neighbor_2opt", route_type: str = "open") -> dict[str, Any]:
    """Calculate route order, kilometers and optional duration for a set of clients."""
    id_to_idx = {cid: idx for idx, cid in enumerate(matrix_data["client_ids"])}
    indices = [id_to_idx[cid] for cid in client_ids]
    matrix = np.asarray(matrix_data["distance_matrix_m"], dtype=float)
    order_idx = _nearest_neighbor(indices, matrix)
    if method == "nearest_neighbor_2opt":
        order_idx = _two_opt(order_idx, matrix, route_type)

    distance_m = _route_distance(order_idx, matrix, route_type)
    duration_min = None
    duration_matrix = matrix_data.get("duration_matrix_s")
    if duration_matrix is not None and len(order_idx) > 1:
        duration = np.asarray(duration_matrix, dtype=float)
        duration_s = sum(float(duration[order_idx[i], order_idx[i + 1]]) for i in range(len(order_idx) - 1))
        if route_type == "closed":
            duration_s += float(duration[order_idx[-1], order_idx[0]])
        duration_min = duration_s / 60.0

    idx_to_id = matrix_data["client_ids"]
    return {"route_order": [idx_to_id[i] for i in order_idx], "route_km": distance_m / 1000.0, "route_duration_min": duration_min}


def distances_for_order(route_order: list[str], matrix_data: dict[str, Any]) -> list[float]:
    """Return per-leg distances in km with zero for the first stop."""
    id_to_idx = {cid: idx for idx, cid in enumerate(matrix_data["client_ids"])}
    matrix = np.asarray(matrix_data["distance_matrix_m"], dtype=float)
    distances = [0.0]
    for prev, cur in zip(route_order, route_order[1:]):
        distances.append(float(matrix[id_to_idx[prev], id_to_idx[cur]]) / 1000.0)
    return distances
