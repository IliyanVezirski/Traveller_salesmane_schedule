"""Compact daily candidate route generation for route-first PVRP."""

from __future__ import annotations

import hashlib
import json
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
    cache_config = {
        "candidate_routes": {
            "random_seed": config["candidate_routes"].get("random_seed"),
            "generation_methods": config["candidate_routes"].get("generation_methods"),
            "keep_top_n_per_rep": config["candidate_routes"].get("keep_top_n_per_rep"),
            "min_candidates_per_client": config["candidate_routes"].get("min_candidates_per_client"),
            "max_route_km_median_multiplier": config["candidate_routes"].get("max_route_km_median_multiplier"),
        },
        "territory_days": config.get("territory_days", {}),
        "daily_route": config["daily_route"],
        "route_costing": {
            "method": config["route_costing"].get("method"),
            "route_type": config["route_costing"].get("route_type"),
        },
    }
    payload += "|" + json.dumps(cache_config, sort_keys=True)
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


def _coverage_targets(df_rep: pd.DataFrame, config: dict) -> dict[str, int]:
    """Return minimum post-pruning candidate coverage by client."""
    min_coverage = int(config["candidate_routes"].get("min_candidates_per_client", 0))
    return {
        str(row.client_id): max(int(row.visit_frequency), min_coverage)
        for row in df_rep[["client_id", "visit_frequency"]].itertuples(index=False)
    }


def _coverage_counts(candidates: pd.DataFrame, client_ids: list[str]) -> dict[str, int]:
    """Count how many candidate routes contain each client."""
    counts = {cid: 0 for cid in client_ids}
    if candidates.empty:
        return counts
    for ids in candidates["client_ids"]:
        for cid in ids:
            if cid in counts:
                counts[cid] += 1
    return counts


def _repair_groups_for_client(
    client_id: str,
    client_ids_all: list[str],
    id_to_idx: dict[str, int],
    matrix: np.ndarray,
    min_clients: int,
    target: int,
    max_clients: int,
) -> list[list[str]]:
    """Create varied compact neighborhood groups that all contain client_id."""
    others = [cid for cid in sorted(client_ids_all, key=lambda other: float(matrix[id_to_idx[client_id], id_to_idx[other]])) if cid != client_id]
    groups: list[list[str]] = []
    sizes = sorted({max(min_clients, min(target, len(client_ids_all))), min(max_clients, max(min_clients, target + 1)), min(max_clients, len(client_ids_all))})
    for size in sizes:
        groups.append([client_id] + others[: max(0, size - 1)])

    route_size = max(min_clients, min(target, max_clients, len(client_ids_all)))
    max_offset = min(16, max(0, len(others) - route_size + 1))
    for offset in range(max_offset):
        groups.append([client_id] + others[offset : offset + route_size - 1])
    return [list(dict.fromkeys(group)) for group in groups if group]


def _selection_score(candidates: pd.DataFrame) -> pd.Series:
    """Score candidates for pruning while keeping route-first costs dominant."""
    territory_mixing = candidates["territory_mixing_penalty"] if "territory_mixing_penalty" in candidates.columns else pd.Series(0, index=candidates.index)
    return (
        candidates["route_km"].astype(float)
        + candidates["underfilled_penalty"].astype(float) * 5
        + candidates["overfilled_penalty"].astype(float) * 5
        + candidates["cluster_mixing_penalty"].astype(float) * 2
        + territory_mixing.astype(float) * 3
    )


def _top_up_candidate_coverage(
    selected: pd.DataFrame,
    candidate_pool: pd.DataFrame,
    df_rep: pd.DataFrame,
    matrix_data: dict[str, Any],
    config: dict,
    client_ids_all: list[str],
    id_to_idx: dict[str, int],
    matrix: np.ndarray,
) -> pd.DataFrame:
    """Add best supplemental routes until every client has enough coverage."""
    selected = selected.copy()
    candidate_pool = candidate_pool.copy()
    targets = _coverage_targets(df_rep, config)
    min_clients = int(config["daily_route"]["min_clients"])
    target_clients = int(config["daily_route"]["target_clients"])
    max_clients = int(config["daily_route"]["max_clients"])

    for _ in range(3):
        counts = _coverage_counts(selected, client_ids_all)
        low_clients = [cid for cid in client_ids_all if counts.get(cid, 0) < targets[cid]]
        if not low_clients:
            break

        supplemental_raw: list[dict[str, Any]] = []
        for cid in low_clients:
            available_count = int(candidate_pool["client_ids"].map(lambda ids, cid=cid: cid in ids).sum())
            if available_count >= targets[cid]:
                continue
            for group in _repair_groups_for_client(cid, client_ids_all, id_to_idx, matrix, min_clients, target_clients, max_clients):
                _add_candidate(supplemental_raw, group, "coverage_repair", df_rep, matrix_data, config)

        if supplemental_raw:
            supplemental = pd.DataFrame(supplemental_raw)
            supplemental["set_key"] = supplemental["client_ids"].map(_client_set_key)
            supplemental["selection_score"] = _selection_score(supplemental)
            candidate_pool = pd.concat([candidate_pool, supplemental], ignore_index=True)
            candidate_pool = candidate_pool.sort_values(["selection_score", "route_km"]).drop_duplicates("set_key", keep="first").reset_index(drop=True)

        selected_keys = set(selected["set_key"])
        added_any = False
        for cid in sorted(low_clients, key=lambda c: counts.get(c, 0)):
            counts = _coverage_counts(selected, client_ids_all)
            needed = targets[cid] - counts.get(cid, 0)
            if needed <= 0:
                continue
            available = candidate_pool[candidate_pool["client_ids"].map(lambda ids, cid=cid: cid in ids)]
            available = available[~available["set_key"].isin(selected_keys)].sort_values(["selection_score", "route_km"])
            if available.empty:
                continue
            to_add = available.head(needed).copy()
            selected = pd.concat([selected, to_add], ignore_index=True)
            selected_keys.update(to_add["set_key"].tolist())
            added_any = True

        if not added_any:
            break
    return selected


def _split_weekday_values(value: Any) -> set[str]:
    """Parse comma/semicolon-separated weekday values."""
    if pd.isna(value) or value is None or str(value).strip() == "":
        return set()
    return {part.strip() for part in str(value).replace(";", ",").split(",") if part.strip()}


def _calendar_days(config: dict) -> list[dict[str, int | str]]:
    """Build lightweight day metadata without importing the calendar module."""
    weekdays = config["working_days"]["weekdays"]
    days: list[dict[str, int | str]] = []
    for week in range(1, int(config["working_days"]["weeks"]) + 1):
        for weekday_index, weekday in enumerate(weekdays):
            days.append(
                {
                    "day_index": len(days),
                    "week_index": week,
                    "weekday": weekday,
                    "weekday_index": weekday_index,
                }
            )
    return days


def _frequency_patterns(visit_frequency: int, config: dict) -> list[list[int]]:
    """Return valid day-index patterns for a client's visit frequency."""
    weekdays = list(range(len(config["working_days"]["weekdays"])))
    if visit_frequency == 2:
        return [[(week - 1) * 5 + weekday for week in week_pair] for week_pair in [(1, 3), (2, 4)] for weekday in weekdays]
    if visit_frequency == 4:
        return [[(week - 1) * 5 + weekday for week in range(1, 5)] for weekday in weekdays]
    if visit_frequency == 8:
        good_pairs = [(0, 3), (1, 4), (0, 2), (1, 3), (2, 4)]
        return [[(week - 1) * 5 + weekday for week in range(1, 5) for weekday in pair] for pair in good_pairs]
    return []


def _filter_patterns_for_weekdays(patterns: list[list[int]], row: Any, day_by_index: dict[int, dict[str, int | str]]) -> list[list[int]]:
    """Prefer patterns that respect fixed/forbidden/preferred weekday metadata."""
    forbidden = _split_weekday_values(getattr(row, "forbidden_weekdays", None))
    fixed = _split_weekday_values(getattr(row, "fixed_weekday", None))
    preferred = _split_weekday_values(getattr(row, "preferred_weekdays", None))
    feasible = [pattern for pattern in patterns if not any(str(day_by_index[day]["weekday"]) in forbidden for day in pattern)]
    if fixed:
        fixed_feasible = [pattern for pattern in feasible if all(str(day_by_index[day]["weekday"]) in fixed for day in pattern)]
        if fixed_feasible:
            feasible = fixed_feasible
    if preferred:
        preferred_feasible = [pattern for pattern in feasible if all(str(day_by_index[day]["weekday"]) in preferred for day in pattern)]
        if preferred_feasible:
            feasible = preferred_feasible
    return feasible or patterns


def _territory_pattern_penalty(pattern: list[int], row: Any, day_by_index: dict[int, dict[str, int | str]]) -> int:
    territory = getattr(row, "territory_weekday_index", None)
    if territory is None or pd.isna(territory):
        return 0
    territory_index = int(territory)
    return sum(1 for day in pattern if int(day_by_index[day]["weekday_index"]) != territory_index)


def _add_periodic_seed_candidates(raw: list[dict[str, Any]], df_rep: pd.DataFrame, matrix_data: dict[str, Any], config: dict) -> None:
    """Add a frequency-feasible seed schedule as route-first candidates."""
    days = _calendar_days(config)
    day_by_index = {int(day["day_index"]): day for day in days}
    buckets: dict[int, list[str]] = {int(day["day_index"]): [] for day in days}
    target = int(config["daily_route"]["target_clients"])
    max_clients = int(config["daily_route"]["max_clients"])

    sorted_df = df_rep.sort_values(["visit_frequency", "cluster_id", "client_id"], ascending=[False, True, True])
    for row in sorted_df.itertuples(index=False):
        patterns = _filter_patterns_for_weekdays(_frequency_patterns(int(row.visit_frequency), config), row, day_by_index)
        best_pattern: list[int] | None = None
        best_score: tuple[int, int, int, int, int] | None = None
        for pattern in patterns:
            projected = [len(buckets[day]) + 1 for day in pattern]
            hard_over = sum(max(0, load - max_clients) for load in projected)
            target_over = sum(max(0, load - target) for load in projected)
            territory_penalty = _territory_pattern_penalty(pattern, row, day_by_index)
            score = (territory_penalty, hard_over, max(projected), target_over, sum(len(buckets[day]) for day in pattern))
            if best_score is None or score < best_score:
                best_score = score
                best_pattern = pattern
        if best_pattern is None:
            continue
        for day in best_pattern:
            buckets[day].append(str(row.client_id))

    for day, client_ids in buckets.items():
        if client_ids:
            _add_candidate(raw, client_ids, "periodic_seed", df_rep, matrix_data, config, intended_day_index=day)


def _add_candidate(
    raw: list[dict[str, Any]],
    client_ids: list[str],
    method: str,
    df_rep: pd.DataFrame,
    matrix_data: dict[str, Any],
    config: dict,
    intended_day_index: int | None = None,
) -> None:
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
    territory_lookup = df_rep.set_index("client_id")["territory_weekday_index"].to_dict() if "territory_weekday_index" in df_rep.columns else {}
    clusters = [cluster_lookup[c] for c in client_ids]
    cluster_counts = pd.Series(clusters).value_counts()
    main_cluster = str(cluster_counts.index[0])
    cluster_count = int(cluster_counts.size)
    territories = [int(territory_lookup[c]) for c in client_ids if c in territory_lookup and not pd.isna(territory_lookup[c])]
    territory_weekday_index = None
    territory_mixing_penalty = 0
    if territories:
        territory_counts = pd.Series(territories).value_counts()
        territory_weekday_index = int(territory_counts.index[0])
        territory_mixing_penalty = int(len(territories) - int(territory_counts.iloc[0]))
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
            "territory_weekday_index": territory_weekday_index,
            "territory_mixing_penalty": territory_mixing_penalty,
            "generation_method": method,
            "underfilled_penalty": max(0, target - n),
            "overfilled_penalty": max(0, n - target),
            "cluster_mixing_penalty": max(0, cluster_count - 1),
            "intended_day_index": intended_day_index,
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

    # 0. Periodic seed routes. These are still route-first candidates, but they
    # provide a frequency-feasible exact-cover backbone for the master solver.
    _add_periodic_seed_candidates(raw, df_rep, matrix_data, config)

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

    # Guarantee that every client appears at least once before final pruning.
    coverage_counts = _coverage_counts(candidates, client_ids_all)
    for cid, count in coverage_counts.items():
        if count == 0:
            nearest = sorted(client_ids_all, key=lambda other: float(matrix[id_to_idx[cid], id_to_idx[other]]))
            _add_candidate(raw, nearest[:target], "coverage_repair", df_rep, matrix_data, config)
    candidates = pd.DataFrame(raw)
    candidates["set_key"] = candidates["client_ids"].map(_client_set_key)
    candidates = candidates.sort_values(["route_km", "cluster_count"]).drop_duplicates("set_key", keep="first")

    keep_top = int(config["candidate_routes"].get("keep_top_n_per_rep", requested))
    candidates["selection_score"] = _selection_score(candidates)
    candidate_pool = candidates.copy()
    essential = candidates[candidates["generation_method"].isin(["periodic_seed", "cluster", "coverage_repair"])].copy()
    top = candidates.nsmallest(keep_top, "selection_score").copy()
    candidates = pd.concat([top, essential], ignore_index=True)
    candidates["set_key"] = candidates["client_ids"].map(_client_set_key)
    candidates = candidates.sort_values(["selection_score", "route_km"]).drop_duplicates("set_key", keep="first").reset_index(drop=True)
    candidates = _top_up_candidate_coverage(candidates, candidate_pool, df_rep, matrix_data, config, client_ids_all, id_to_idx, matrix)
    candidates = candidates.sort_values(["selection_score", "route_km"]).drop_duplicates("set_key", keep="first").reset_index(drop=True)
    candidates["candidate_id"] = [f"{str(df_rep['sales_rep'].iloc[0]).replace(' ', '_')}_{i:05d}" for i in range(len(candidates))]
    candidates = candidates.drop(columns=["set_key", "selection_score"], errors="ignore")

    coverage_rows = []
    coverage_counts = _coverage_counts(candidates, client_ids_all)
    coverage_targets = _coverage_targets(df_rep, config)
    for row in df_rep.itertuples(index=False):
        count = coverage_counts[str(row.client_id)]
        target_count = coverage_targets[str(row.client_id)]
        coverage_rows.append(
            {
                "sales_rep": row.sales_rep,
                "client_id": str(row.client_id),
                "client_name": row.client_name,
                "visit_frequency": int(row.visit_frequency),
                "number_of_candidates_containing_client": count,
                "min_recommended_candidate_coverage": target_count,
                "severity": "ERROR" if count == 0 else ("WARNING" if count < target_count else "OK"),
            }
        )
    coverage_df = pd.DataFrame(coverage_rows)

    if bool(config["candidate_routes"].get("cache", True)):
        cache_file.parent.mkdir(parents=True, exist_ok=True)
        with cache_file.open("wb") as fh:
            pickle.dump((candidates, coverage_df), fh)
    return candidates, coverage_df
