"""Client clustering for compact candidate route generation."""

from __future__ import annotations

import math
from typing import Any

import numpy as np
import pandas as pd
from sklearn.cluster import KMeans


def _geo_distance_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    mean_lat = math.radians((lat1 + lat2) / 2.0)
    dx = (lon2 - lon1) * 111.320 * math.cos(mean_lat)
    dy = (lat2 - lat1) * 110.574
    return math.hypot(dx, dy)


def _aligned_distance_matrix(
    df_rep: pd.DataFrame,
    distance_matrix: np.ndarray | None,
    matrix_client_ids: list[str] | None = None,
) -> np.ndarray | None:
    if distance_matrix is None:
        return None
    matrix = np.asarray(distance_matrix, dtype=float)
    n = len(df_rep)
    if matrix.shape != (n, n):
        return None

    if matrix_client_ids is not None:
        if len(matrix_client_ids) != n:
            return None
        matrix_index = {str(client_id): idx for idx, client_id in enumerate(matrix_client_ids)}
        out_ids = df_rep["client_id"].astype(str).tolist()
        if any(client_id not in matrix_index for client_id in out_ids):
            return None
        order = [matrix_index[client_id] for client_id in out_ids]
        matrix = matrix[np.ix_(order, order)]

    matrix = np.nan_to_num(matrix, nan=1e12, posinf=1e12, neginf=1e12)
    matrix = np.maximum(matrix, 0.0)
    matrix = (matrix + matrix.T) / 2.0
    np.fill_diagonal(matrix, 0.0)
    return matrix


def _k_medoids_labels(distance_matrix: np.ndarray, n_clusters: int, random_seed: int = 42, max_iterations: int = 30) -> np.ndarray:
    """Cluster points by a precomputed road-distance matrix."""
    matrix = np.asarray(distance_matrix, dtype=float)
    n = matrix.shape[0]
    if n_clusters <= 1:
        return np.zeros(n, dtype=int)
    if n_clusters >= n:
        return np.arange(n, dtype=int)

    rng = np.random.default_rng(int(random_seed))
    first = int(rng.integers(0, n))
    medoids = [first]
    while len(medoids) < n_clusters:
        nearest = np.min(matrix[:, medoids], axis=1)
        nearest[medoids] = -1.0
        medoids.append(int(np.argmax(nearest)))

    labels = np.zeros(n, dtype=int)
    for _ in range(max(1, int(max_iterations))):
        labels = np.argmin(matrix[:, medoids], axis=1).astype(int)
        new_medoids = medoids.copy()
        for cluster_idx in range(n_clusters):
            members = np.where(labels == cluster_idx)[0]
            if len(members) == 0:
                nearest = np.min(matrix[:, new_medoids], axis=1)
                nearest[new_medoids] = -1.0
                new_medoids[cluster_idx] = int(np.argmax(nearest))
                continue
            within = matrix[np.ix_(members, members)]
            new_medoids[cluster_idx] = int(members[np.argmin(within.sum(axis=1))])
        if new_medoids == medoids:
            break
        medoids = new_medoids

    labels = np.argmin(matrix[:, medoids], axis=1).astype(int)
    return labels


def _auto_cluster_count(n_clients: int, config: dict | None = None) -> int:
    if n_clients <= 3:
        return max(1, n_clients)
    clustering_cfg = (config or {}).get("clustering", {})
    target_clients = int((config or {}).get("daily_route", {}).get("target_clients", 20))
    configured_target_size = clustering_cfg.get("target_cluster_size")
    if configured_target_size is None:
        target_cluster_size = max(3, int(round(target_clients / 4)))
    else:
        target_cluster_size = max(2, int(configured_target_size))
    max_clusters = max(3, int(clustering_cfg.get("max_clusters_per_rep", 30)))
    min_clusters = max(1, int(clustering_cfg.get("min_clusters_per_rep", 3)))
    by_size = math.ceil(n_clients / target_cluster_size)
    return int(min(n_clients, max_clusters, max(min_clusters, round(math.sqrt(n_clients)), by_size)))


def _cluster_medoid_indices(out: pd.DataFrame, distance_matrix: np.ndarray) -> dict[str, int]:
    labels = out["cluster_id"].astype(str).to_numpy()
    medoid_indices: dict[str, int] = {}
    for cluster_id in sorted(pd.unique(labels)):
        members = np.where(labels == str(cluster_id))[0]
        within = distance_matrix[np.ix_(members, members)]
        medoid_indices[str(cluster_id)] = int(members[np.argmin(within.sum(axis=1))])
    return medoid_indices


def _cluster_distance_lookup_km(
    out: pd.DataFrame,
    clusters: pd.DataFrame,
    distance_matrix: np.ndarray | None,
) -> dict[str, dict[str, float]]:
    cluster_ids = sorted(clusters["cluster_id"].astype(str).unique())
    lookup: dict[str, dict[str, float]] = {cluster_id: {} for cluster_id in cluster_ids}
    if distance_matrix is not None:
        medoid_indices = _cluster_medoid_indices(out, distance_matrix)
        for cluster_id_i in cluster_ids:
            for cluster_id_j in cluster_ids:
                meters = float(distance_matrix[medoid_indices[cluster_id_i], medoid_indices[cluster_id_j]])
                lookup[cluster_id_i][cluster_id_j] = max(0.0, meters / 1000.0)
        return lookup

    centers = clusters.set_index("cluster_id")[["lat", "lon"]].astype(float)
    for cluster_id_i in cluster_ids:
        for cluster_id_j in cluster_ids:
            left = centers.loc[cluster_id_i]
            right = centers.loc[cluster_id_j]
            lookup[cluster_id_i][cluster_id_j] = _geo_distance_km(
                float(left["lat"]),
                float(left["lon"]),
                float(right["lat"]),
                float(right["lon"]),
            )
    return lookup


def _cluster_order_by_matrix(out: pd.DataFrame, distance_matrix: np.ndarray) -> list[str]:
    cluster_ids = sorted(out["cluster_id"].astype(str).unique())
    if len(cluster_ids) <= 1:
        return cluster_ids

    medoid_indices = _cluster_medoid_indices(out, distance_matrix)

    cluster_matrix = np.zeros((len(cluster_ids), len(cluster_ids)), dtype=float)
    for i, cluster_id_i in enumerate(cluster_ids):
        for j, cluster_id_j in enumerate(cluster_ids):
            cluster_matrix[i, j] = float(distance_matrix[medoid_indices[cluster_id_i], medoid_indices[cluster_id_j]])

    def path_score(order: list[int]) -> tuple[float, float, tuple[int, ...]]:
        jumps = [float(cluster_matrix[a, b]) for a, b in zip(order, order[1:])]
        return (max(jumps) if jumps else 0.0, sum(jumps), tuple(order))

    def two_opt(order: list[int]) -> list[int]:
        if len(order) < 4:
            return order
        best = order
        best_score_local = path_score(best)
        improved = True
        while improved:
            improved = False
            for start in range(1, len(best) - 1):
                for end in range(start + 1, len(best)):
                    candidate = best[:start] + list(reversed(best[start : end + 1])) + best[end + 1 :]
                    candidate_score = path_score(candidate)
                    if candidate_score < best_score_local:
                        best = candidate
                        best_score_local = candidate_score
                        improved = True
                        break
                if improved:
                    break
        return best

    best_order: list[int] | None = None
    best_score: tuple[float, float, tuple[int, ...]] | None = None
    for start in range(len(cluster_ids)):
        unused = set(range(len(cluster_ids)))
        order = [start]
        unused.remove(start)
        while unused:
            current = order[-1]
            nxt = min(unused, key=lambda idx: (cluster_matrix[current, idx], idx))
            order.append(nxt)
            unused.remove(nxt)
        order = two_opt(order)
        score = path_score(order)
        if best_score is None or score < best_score:
            best_score = score
            best_order = order

    return [cluster_ids[idx] for idx in (best_order or list(range(len(cluster_ids))))]


def _partition_weekday_territories(
    rows: list[Any],
    weekdays_count: int,
    target_per_weekday: float,
    max_clients: float,
    distance_lookup_km: dict[str, dict[str, float]],
    config: dict,
) -> tuple[dict[str, int], list[float], list[float], float]:
    if not rows:
        return {}, [0.0 for _ in range(weekdays_count)], [0.0 for _ in range(weekdays_count)], 0.0

    n = len(rows)
    if n < weekdays_count:
        assignments = {str(row.cluster_id): min(index, weekdays_count - 1) for index, row in enumerate(rows)}
        loads = [0.0 for _ in range(weekdays_count)]
        spans = [0.0 for _ in range(weekdays_count)]
        for index, row in enumerate(rows):
            loads[min(index, weekdays_count - 1)] += float(row.weekly_demand)
        return assignments, loads, spans, 0.0

    territory_cfg = config.get("territory_days", {})
    span_soft_limit = float(territory_cfg.get("max_daily_territory_km", 75.0))
    span_weight = float(territory_cfg.get("route_span_weight", 25.0))
    span_over_weight = float(territory_cfg.get("route_span_over_limit_weight", 5_000.0))
    load_balance_weight = float(territory_cfg.get("load_balance_weight", 250.0))
    overload_weight = float(territory_cfg.get("overload_weight", 1_000_000.0))

    cluster_ids = [str(row.cluster_id) for row in rows]
    demands = [float(row.weekly_demand) for row in rows]
    prefix_load = [0.0]
    for demand in demands:
        prefix_load.append(prefix_load[-1] + demand)

    segment_span = np.zeros((n + 1, n + 1), dtype=float)
    for start in range(n):
        running = 0.0
        for end in range(start + 1, n + 1):
            if end - start > 1:
                left = cluster_ids[end - 2]
                right = cluster_ids[end - 1]
                running += float(distance_lookup_km.get(left, {}).get(right, 0.0))
            segment_span[start, end] = running

    def segment_load(start: int, end: int) -> float:
        return float(prefix_load[end] - prefix_load[start])

    def segment_cost(start: int, end: int) -> float:
        load = segment_load(start, end)
        span = float(segment_span[start, end])
        overload = max(0.0, load - max_clients)
        span_over = max(0.0, span - span_soft_limit)
        return (
            overload * overload_weight
            + abs(load - target_per_weekday) * load_balance_weight
            + span * span_weight
            + span_over * span_over_weight
        )

    inf = float("inf")
    dp = [[inf for _ in range(n + 1)] for _ in range(weekdays_count + 1)]
    prev: list[list[int | None]] = [[None for _ in range(n + 1)] for _ in range(weekdays_count + 1)]
    dp[0][0] = 0.0
    for territory_index in range(1, weekdays_count + 1):
        min_end = territory_index
        max_end = n - (weekdays_count - territory_index)
        for end in range(min_end, max_end + 1):
            best_value = inf
            best_cut: int | None = None
            for cut in range(territory_index - 1, end):
                if dp[territory_index - 1][cut] == inf:
                    continue
                value = dp[territory_index - 1][cut] + segment_cost(cut, end)
                if value < best_value:
                    best_value = value
                    best_cut = cut
            dp[territory_index][end] = best_value
            prev[territory_index][end] = best_cut

    cuts: list[tuple[int, int]] = []
    end = n
    for territory_index in range(weekdays_count, 0, -1):
        cut = prev[territory_index][end]
        if cut is None:
            return {}, [0.0 for _ in range(weekdays_count)], [0.0 for _ in range(weekdays_count)], inf
        cuts.append((cut, end))
        end = cut
    cuts.reverse()

    assignments: dict[str, int] = {}
    loads: list[float] = []
    spans: list[float] = []
    for territory_index, (start, end) in enumerate(cuts):
        loads.append(segment_load(start, end))
        spans.append(float(segment_span[start, end]))
        for row in rows[start:end]:
            assignments[str(row.cluster_id)] = territory_index

    return assignments, loads, spans, float(dp[weekdays_count][n])


def _territory_route_span_km(cluster_ids: list[str], distance_lookup_km: dict[str, dict[str, float]]) -> float:
    if len(cluster_ids) <= 1:
        return 0.0

    best = float("inf")
    for start in cluster_ids:
        unused = set(cluster_ids)
        unused.remove(start)
        current = start
        total = 0.0
        while unused:
            nxt = min(unused, key=lambda cluster_id: (distance_lookup_km.get(current, {}).get(cluster_id, 0.0), cluster_id))
            total += float(distance_lookup_km.get(current, {}).get(nxt, 0.0))
            current = nxt
            unused.remove(nxt)
        best = min(best, total)
    return best


def _assignment_score(
    assignments: dict[str, int],
    rows: list[Any],
    weekdays_count: int,
    target_per_weekday: float,
    max_clients: float,
    distance_lookup_km: dict[str, dict[str, float]],
    config: dict,
) -> tuple[tuple[float, float, float, float, float, float, float], list[float], list[float]]:
    territory_cfg = config.get("territory_days", {})
    span_soft_limit = float(territory_cfg.get("max_daily_territory_km", 75.0))
    span_weight = float(territory_cfg.get("route_span_weight", 25.0))
    span_over_weight = float(territory_cfg.get("route_span_over_limit_weight", 5_000.0))
    load_balance_weight = float(territory_cfg.get("load_balance_weight", 250.0))
    overload_weight = float(territory_cfg.get("overload_weight", 1_000_000.0))

    row_by_cluster = {str(row.cluster_id): row for row in rows}
    groups = [[] for _ in range(weekdays_count)]
    loads = [0.0 for _ in range(weekdays_count)]
    for cluster_id, weekday_index in assignments.items():
        idx = max(0, min(weekdays_count - 1, int(weekday_index)))
        groups[idx].append(str(cluster_id))
        loads[idx] += float(row_by_cluster[str(cluster_id)].weekly_demand)

    spans = [_territory_route_span_km(group, distance_lookup_km) for group in groups]
    overloads = [max(0.0, load - max_clients) for load in loads]
    span_overs = [max(0.0, span - span_soft_limit) for span in spans]
    total_cost = 0.0
    for load, span, overload, span_over in zip(loads, spans, overloads, span_overs):
        total_cost += (
            overload * overload_weight
            + abs(load - target_per_weekday) * load_balance_weight
            + span * span_weight
            + span_over * span_over_weight
        )
    score = (
        max(overloads) if overloads else 0.0,
        max(span_overs) if span_overs else 0.0,
        sum(span_overs),
        total_cost,
        max(spans) if spans else 0.0,
        max(abs(load - target_per_weekday) for load in loads) if loads else 0.0,
        -min(loads) if loads else 0.0,
    )
    return score, loads, spans


def _refine_weekday_territories(
    assignments: dict[str, int],
    rows: list[Any],
    weekdays_count: int,
    target_per_weekday: float,
    max_clients: float,
    distance_lookup_km: dict[str, dict[str, float]],
    config: dict,
) -> dict[str, int]:
    """Move or swap small cluster groups when that reduces road-span without exceeding daily capacity."""
    if not assignments:
        return assignments

    territory_cfg = config.get("territory_days", {})
    max_iterations = max(0, int(territory_cfg.get("local_refinement_iterations", 25)))
    if max_iterations == 0:
        return assignments

    cluster_ids = sorted(assignments)
    current = dict(assignments)
    best_score, _, _ = _assignment_score(current, rows, weekdays_count, target_per_weekday, max_clients, distance_lookup_km, config)

    for _ in range(max_iterations):
        improved = False
        best_candidate: dict[str, int] | None = None
        best_candidate_score = best_score

        for cluster_id in cluster_ids:
            original_weekday = current[cluster_id]
            for weekday_index in range(weekdays_count):
                if weekday_index == original_weekday:
                    continue
                candidate = dict(current)
                candidate[cluster_id] = weekday_index
                candidate_score, _, _ = _assignment_score(
                    candidate,
                    rows,
                    weekdays_count,
                    target_per_weekday,
                    max_clients,
                    distance_lookup_km,
                    config,
                )
                if candidate_score < best_candidate_score:
                    best_candidate = candidate
                    best_candidate_score = candidate_score

        for left_index, left in enumerate(cluster_ids):
            for right in cluster_ids[left_index + 1 :]:
                if current[left] == current[right]:
                    continue
                candidate = dict(current)
                candidate[left], candidate[right] = candidate[right], candidate[left]
                candidate_score, _, _ = _assignment_score(
                    candidate,
                    rows,
                    weekdays_count,
                    target_per_weekday,
                    max_clients,
                    distance_lookup_km,
                    config,
                )
                if candidate_score < best_candidate_score:
                    best_candidate = candidate
                    best_candidate_score = candidate_score

        if best_candidate is not None:
            current = best_candidate
            best_score = best_candidate_score
            improved = True
        if not improved:
            break

    return current


def _assign_weekday_territories(out: pd.DataFrame, config: dict, distance_matrix: np.ndarray | None = None) -> pd.DataFrame:
    """Assign whole clusters to compact road-distance weekday territories."""
    weekdays = list(config.get("working_days", {}).get("weekdays", []))
    if not weekdays:
        out["territory_weekday_index"] = 0
        out["territory_weekday"] = ""
        return out

    weeks = max(1, int(config.get("working_days", {}).get("weeks", 4)))
    clusters = (
        out.groupby("cluster_id", as_index=False)
        .agg(
            lat=("lat", "mean"),
            lon=("lon", "mean"),
            weekly_demand=("visit_frequency", lambda series: float(pd.to_numeric(series, errors="coerce").fillna(0).sum()) / weeks),
        )
    )
    clusters["cluster_id"] = clusters["cluster_id"].astype(str)
    use_matrix = bool(config.get("territory_days", {}).get("use_distance_matrix", True))
    if use_matrix and distance_matrix is not None:
        ordered_ids = _cluster_order_by_matrix(out, distance_matrix)
        ordered = clusters.set_index("cluster_id").loc[ordered_ids].reset_index()
    else:
        center_lat = float(clusters["lat"].mean())
        center_lon = float(clusters["lon"].mean())
        ordered = (
            clusters.assign(_angle=np.arctan2(clusters["lat"].astype(float) - center_lat, clusters["lon"].astype(float) - center_lon))
            .sort_values(["_angle", "cluster_id"])
            .copy()
        )
    total_weekly_demand = float(ordered["weekly_demand"].sum())
    target_per_weekday = total_weekly_demand / max(1, len(weekdays))
    distance_lookup_km = _cluster_distance_lookup_km(out, clusters, distance_matrix if use_matrix else None)
    max_clients = float(config.get("daily_route", {}).get("max_clients", target_per_weekday))
    rows = list(ordered.itertuples(index=False))
    best_assignments: dict[str, int] | None = None
    best_score: tuple[float, float, float, float, float, float, float] | None = None
    span_soft_limit = float(config.get("territory_days", {}).get("max_daily_territory_km", 75.0))
    for start in range(len(rows)):
        rotated = rows[start:] + rows[:start]
        assignments, loads, spans, total_cost = _partition_weekday_territories(
            rotated,
            len(weekdays),
            target_per_weekday,
            max_clients,
            distance_lookup_km,
            config,
        )
        if not assignments:
            continue
        score = (
            max(0.0, max(loads) - max_clients),
            max(0.0, max(spans) - span_soft_limit),
            total_cost,
            max(spans),
            max(abs(load - target_per_weekday) for load in loads),
            max(loads),
            -min(loads),
        )
        if best_score is None or score < best_score:
            best_score = score
            best_assignments = assignments

    if best_assignments:
        best_assignments = _refine_weekday_territories(
            best_assignments,
            rows,
            len(weekdays),
            target_per_weekday,
            max_clients,
            distance_lookup_km,
            config,
        )

    out["territory_weekday_index"] = out["cluster_id"].astype(str).map(best_assignments or {}).fillna(0).astype(int)
    out["territory_weekday"] = out["territory_weekday_index"].map(lambda index: str(weekdays[int(index)]))
    return out


def assign_global_weekday_territories(clients_df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """Assign a shared weekday territory map across all sales reps without moving clients."""
    out = clients_df.copy().reset_index(drop=True)
    weekdays = list(config.get("working_days", {}).get("weekdays", []))
    if not bool(config.get("territory_days", {}).get("enabled", True)) or not weekdays or out.empty:
        return out

    configured_clusters = config.get("global_geography", {}).get("global_cluster_count")
    if configured_clusters is None:
        configured_clusters = config.get("territory_days", {}).get("global_cluster_count")
    if configured_clusters is None:
        n_clusters = _auto_cluster_count(len(out), config)
    else:
        n_clusters = max(1, min(len(out), int(configured_clusters)))

    if n_clusters <= 1:
        labels = np.zeros(len(out), dtype=int)
    else:
        model = KMeans(n_clusters=n_clusters, random_state=int(config["candidate_routes"].get("random_seed", 42)), n_init=10)
        labels = model.fit_predict(out[["lat", "lon"]].to_numpy(dtype=float))

    territory_input = out.copy()
    territory_input["cluster_id"] = labels.astype(str)
    territory_out = _assign_weekday_territories(territory_input, config, None)

    out["global_territory_cluster_id"] = territory_input["cluster_id"].astype(str)
    out["global_territory_weekday_index"] = territory_out["territory_weekday_index"].astype(int)
    out["global_territory_weekday"] = territory_out["territory_weekday"].astype(str)
    return out


def cluster_clients(
    df_rep: pd.DataFrame,
    distance_matrix: np.ndarray | None,
    config: dict,
    matrix_client_ids: list[str] | None = None,
) -> pd.DataFrame:
    """Assign cluster_id and cluster summary columns to one rep's clients."""
    out = df_rep.copy().reset_index(drop=True)
    aligned_matrix = _aligned_distance_matrix(out, distance_matrix, matrix_client_ids)
    if "cluster_manual" in out.columns and out["cluster_manual"].notna().any():
        out["cluster_id"] = out["cluster_manual"].fillna("manual_missing").astype(str)
    else:
        n_clusters = _auto_cluster_count(len(out), config)
        if n_clusters <= 1:
            labels = np.zeros(len(out), dtype=int)
        elif bool(config.get("clustering", {}).get("use_distance_matrix", True)) and aligned_matrix is not None:
            labels = _k_medoids_labels(
                aligned_matrix,
                n_clusters,
                random_seed=int(config["candidate_routes"].get("random_seed", 42)),
                max_iterations=int(config.get("clustering", {}).get("k_medoids_max_iterations", 30)),
            )
        else:
            model = KMeans(n_clusters=n_clusters, random_state=int(config["candidate_routes"].get("random_seed", 42)), n_init=10)
            labels = model.fit_predict(out[["lat", "lon"]].to_numpy(dtype=float))
        out["cluster_id"] = labels.astype(str)

    summary = out.groupby("cluster_id").agg(cluster_size=("client_id", "size"), cluster_monthly_visits=("visit_frequency", "sum")).reset_index()
    out = out.merge(summary, on="cluster_id", how="left")
    if bool(config.get("territory_days", {}).get("enabled", True)):
        scope = str(config.get("territory_days", {}).get("scope", "per_rep")).lower()
        if scope == "global" and "global_territory_weekday_index" in out.columns:
            weekdays = list(config.get("working_days", {}).get("weekdays", []))
            out["territory_weekday_index"] = pd.to_numeric(out["global_territory_weekday_index"], errors="coerce").fillna(0).astype(int)
            if "global_territory_weekday" in out.columns:
                out["territory_weekday"] = out["global_territory_weekday"].fillna("").astype(str)
            else:
                out["territory_weekday"] = out["territory_weekday_index"].map(lambda index: str(weekdays[int(index)]) if weekdays else "")
        else:
            out = _assign_weekday_territories(out, config, aligned_matrix)
    return out
