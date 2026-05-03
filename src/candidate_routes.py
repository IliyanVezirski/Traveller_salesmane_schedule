"""Compact daily candidate route generation for route-first PVRP."""

from __future__ import annotations

import hashlib
import math
import pickle
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd

from .app_paths import get_cache_dir
from .route_costing import calculate_route_cost


def _client_set_key(client_ids: Iterable[str]) -> tuple[str, ...]:
    return tuple(sorted(str(c) for c in client_ids))


def _candidate_cache_file(df_rep: pd.DataFrame, config: dict) -> Path:
    rep = str(df_rep["sales_rep"].iloc[0]).replace(" ", "_")
    payload = "|".join(f"{r.client_id}:{r.cluster_id}:{r.lat:.6f}:{r.lon:.6f}" for r in df_rep.sort_values("client_id").itertuples())
    payload += f"|{config['candidate_routes'].get('random_seed')}|{config['daily_route']}"
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()[:20]
    return get_cache_dir() / "candidate_routes" / f"{rep}_{digest}.pkl"


def _compact_subset(seed_ids: list[str], candidate_pool: list[str], id_to_idx: dict[str, int], matrix: np.ndarray, target: int, max_clients: int) -> list[str]:
    chosen = list(dict.fromkeys(seed_ids))
    while len(chosen) < target and len(chosen) < max_clients:
        remaining = [c for c in candidate_pool if c not in chosen]
        if not remaining:
            break
        nxt = min(remaining, key=lambda cid: min(float(matrix[id_to_idx[cid], id_to_idx[x]]) for x in chosen))
        chosen.append(nxt)
    return chosen[:max_clients]


def _split_large_cluster(ids: list[str], id_to_idx: dict[str, int], matrix: np.ndarray, target: int, max_clients: int) -> list[list[str]]:
    remaining = ids[:]
    groups: list[list[str]] = []
    while remaining:
        seed = remaining.pop(0)
        group = _compact_subset([seed], remaining + [seed], id_to_idx, matrix, target, max_clients)
        groups.append(group)
        used = set(group)
        remaining = [c for c in remaining if c not in used]
    return groups


def _cluster_centers(df: pd.DataFrame) -> pd.DataFrame:
    return df.groupby("cluster_id").agg(lat=("lat", "mean"), lon=("lon", "mean")).reset_index()


def _add_candidate(raw: list[dict[str, Any]], client_ids: list[str], method: str, df_rep: pd.DataFrame, matrix_data: dict[str, Any], config: dict) -> None:
    target = int(config["daily_route"]["target_clients"])
    min_clients = int(config["daily_route"]["min_clients"])
    max_clients = int(config["daily_route"]["max_clients"])
    allow_under = bool(config["daily_route"].get("allow_underfilled", True))
    allow_over = bool(config["daily_route"].get("allow_overfilled", False))
    client_ids = list(dict.fromkeys(str(c) for c in client_ids))
    n = len(client_ids)
    if n == 0 or n > max_clients and not allow_over:
        return
    if n < min_clients and not allow_under:
        return

    cost = calculate_route_cost(client_ids, matrix_data, config["route_costing"].get("method", "nearest_neighbor_2opt"), config["route_costing"].get("route_type", "open"))
    cluster_lookup = df_rep.set_index("client_id")["cluster_id"].astype(str).to_dict()
    clusters = [cluster_lookup[c] for c in client_ids]
    cluster_counts = pd.Series(clusters).value_counts()
    main_cluster = str(cluster_counts.index[0])
    cluster_count = int(cluster_counts.size)
    raw.append(
        {
            "sales_rep": str(df_rep["sales_rep"].iloc[0]),
            "client_ids": client_ids,
            "number_of_clients": n,
            "route_km": float(cost["route_km"]),
            "route_duration_min": cost["route_duration_min"],
            "main_cluster": main_cluster,
            "clusters_used": ",".join(sorted(set(clusters))),
            "cluster_count": cluster_count,
            "generation_method": method,
            "underfilled_penalty": max(0, target - n),
            "overfilled_penalty": max(0, n - target),
            "cluster_mixing_penalty": max(0, cluster_count - 1),
        }
    )


def generate_candidate_routes_for_rep(df_rep: pd.DataFrame, matrix_data: dict[str, Any], config: dict) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Generate compact route-first daily candidates and per-client coverage."""
    cache_file = _candidate_cache_file(df_rep, config)
    if bool(config["candidate_routes"].get("cache", True)) and cache_file.exists():
        with cache_file.open("rb") as fh:
            return pickle.load(fh)

    rng = np.random.default_rng(int(config["candidate_routes"].get("random_seed", 42)))
    df_rep = df_rep.copy()
    client_ids_all = df_rep["client_id"].astype(str).tolist()
    matrix = np.asarray(matrix_data["distance_matrix_m"], dtype=float)
    id_to_idx = {cid: i for i, cid in enumerate(matrix_data["client_ids"])}
    target = int(config["daily_route"]["target_clients"])
    min_clients = int(config["daily_route"]["min_clients"])
    max_clients = int(config["daily_route"]["max_clients"])
    raw: list[dict[str, Any]] = []

    # 1. Cluster routes.
    for _, cluster_df in df_rep.groupby("cluster_id"):
        ids = cluster_df["client_id"].astype(str).tolist()
        if len(ids) > max_clients:
            for group in _split_large_cluster(ids, id_to_idx, matrix, target, max_clients):
                _add_candidate(raw, group, "cluster", df_rep, matrix_data, config)
        else:
            pool = client_ids_all
            group = _compact_subset(ids, pool, id_to_idx, matrix, target if len(ids) < min_clients else len(ids), max_clients)
            _add_candidate(raw, group, "cluster", df_rep, matrix_data, config)

    # 2. Cluster plus neighbor clusters.
    centers = _cluster_centers(df_rep)
    for row in centers.itertuples(index=False):
        ordered = []
        for other in centers.itertuples(index=False):
            dist = math.hypot(float(row.lat) - float(other.lat), float(row.lon) - float(other.lon))
            ordered.append((str(other.cluster_id), dist))
        neighbor_clusters = [cid for cid, _ in sorted(ordered, key=lambda x: x[1])[:3]]
        pool = df_rep[df_rep["cluster_id"].astype(str).isin(neighbor_clusters)]["client_id"].astype(str).tolist()
        seed = df_rep[df_rep["cluster_id"].astype(str).eq(str(row.cluster_id))]["client_id"].astype(str).tolist()[:1]
        _add_candidate(raw, _compact_subset(seed, pool, id_to_idx, matrix, target, max_clients), "cluster_neighbors", df_rep, matrix_data, config)

    # 3. Sweep windows.
    center_lat, center_lon = df_rep["lat"].mean(), df_rep["lon"].mean()
    sweep = df_rep.assign(angle=np.arctan2(df_rep["lat"] - center_lat, df_rep["lon"] - center_lon)).sort_values("angle")
    ordered_ids = sweep["client_id"].astype(str).tolist()
    circular = ordered_ids + ordered_ids
    for size in range(min_clients, max_clients + 1):
        step = max(1, size // 4)
        for start in range(0, len(ordered_ids), step):
            _add_candidate(raw, circular[start : start + size], "sweep", df_rep, matrix_data, config)

    # 4. Nearest-neighbor expansion from every seed.
    for seed in client_ids_all:
        nearest = sorted(client_ids_all, key=lambda cid: float(matrix[id_to_idx[seed], id_to_idx[cid]]))
        _add_candidate(raw, nearest[:target], "nearest_neighbor_expansion", df_rep, matrix_data, config)

    # 5. Randomized compact variants.
    requested = int(config["candidate_routes"].get("candidates_per_rep", 3000))
    iterations = max(100, requested)
    for _ in range(iterations):
        seed = str(rng.choice(client_ids_all))
        chosen = [seed]
        while len(chosen) < target:
            remaining = [c for c in client_ids_all if c not in chosen]
            if not remaining:
                break
            distances = np.array([min(float(matrix[id_to_idx[c], id_to_idx[x]]) for x in chosen) for c in remaining])
            ranks = np.argsort(np.argsort(distances))
            weights = np.exp(-ranks / max(1.0, target / 2))
            weights = weights / weights.sum()
            chosen.append(str(rng.choice(remaining, p=weights)))
        jitter = int(rng.integers(-2, 3))
        _add_candidate(raw, chosen[: max(min_clients, min(max_clients, target + jitter))], "randomized_compact", df_rep, matrix_data, config)

    candidates = pd.DataFrame(raw)
    if candidates.empty:
        raise RuntimeError(f"No candidate routes generated for sales_rep={df_rep['sales_rep'].iloc[0]}")

    candidates["set_key"] = candidates["client_ids"].map(_client_set_key)
    if bool(config["candidate_routes"].get("remove_duplicates", True)):
        candidates = candidates.sort_values(["route_km", "cluster_count"]).drop_duplicates("set_key", keep="first")

    median_km = float(candidates["route_km"].median())
    max_multiplier = float(config["candidate_routes"].get("max_route_km_median_multiplier", 2.8))
    if median_km > 0:
        candidates = candidates[candidates["route_km"].le(median_km * max_multiplier)]

    # Guarantee that every client appears at least once.
    coverage_counts = {cid: 0 for cid in client_ids_all}
    for ids in candidates["client_ids"]:
        for cid in ids:
            coverage_counts[cid] = coverage_counts.get(cid, 0) + 1
    for cid, count in coverage_counts.items():
        if count == 0:
            nearest = sorted(client_ids_all, key=lambda other: float(matrix[id_to_idx[cid], id_to_idx[other]]))
            _add_candidate(raw, nearest[:target], "coverage_repair", df_rep, matrix_data, config)
    candidates = pd.DataFrame(raw)
    candidates["set_key"] = candidates["client_ids"].map(_client_set_key)
    candidates = candidates.sort_values(["route_km", "cluster_count"]).drop_duplicates("set_key", keep="first")

    keep_top = int(config["candidate_routes"].get("keep_top_n_per_rep", requested))
    candidates["selection_score"] = candidates["route_km"] + candidates["underfilled_penalty"] * 5 + candidates["overfilled_penalty"] * 5 + candidates["cluster_mixing_penalty"] * 2
    essential = candidates[candidates["generation_method"].isin(["cluster", "coverage_repair"])].copy()
    top = candidates.nsmallest(keep_top, "selection_score").copy()
    candidates = pd.concat([top, essential], ignore_index=True)
    candidates["set_key"] = candidates["client_ids"].map(_client_set_key)
    candidates = candidates.sort_values(["selection_score", "route_km"]).drop_duplicates("set_key", keep="first").reset_index(drop=True)
    candidates["candidate_id"] = [f"{str(df_rep['sales_rep'].iloc[0]).replace(' ', '_')}_{i:05d}" for i in range(len(candidates))]
    candidates = candidates.drop(columns=["set_key", "selection_score"], errors="ignore")

    coverage_rows = []
    for row in df_rep.itertuples(index=False):
        count = int(sum(str(row.client_id) in ids for ids in candidates["client_ids"]))
        coverage_rows.append(
            {
                "sales_rep": row.sales_rep,
                "client_id": str(row.client_id),
                "client_name": row.client_name,
                "visit_frequency": int(row.visit_frequency),
                "number_of_candidates_containing_client": count,
                "severity": "ERROR" if count == 0 else ("WARNING" if count < int(row.visit_frequency) else "OK"),
            }
        )
    coverage_df = pd.DataFrame(coverage_rows)

    if bool(config["candidate_routes"].get("cache", True)):
        cache_file.parent.mkdir(parents=True, exist_ok=True)
        with cache_file.open("wb") as fh:
            pickle.dump((candidates, coverage_df), fh)
    return candidates, coverage_df
