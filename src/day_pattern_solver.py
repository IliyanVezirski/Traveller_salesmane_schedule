"""Client-day pattern CP-SAT scheduler.

This master model assigns each client to concrete visit days for the whole
month. Final stop ordering is handled later by PyVRP per selected day.
"""

from __future__ import annotations

from collections import defaultdict
from itertools import combinations, product
import math
from typing import Any

import pandas as pd
from ortools.sat.python import cp_model


def _split_weekdays(value: Any) -> set[str]:
    if pd.isna(value) or value is None or str(value).strip() == "":
        return set()
    return {part.strip() for part in str(value).replace(";", ",").split(",") if part.strip()}


def _distance_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Fast equirectangular distance for scoring pattern geography."""
    mean_lat = math.radians((lat1 + lat2) / 2.0)
    dx = (lon2 - lon1) * 111.320 * math.cos(mean_lat)
    dy = (lat2 - lat1) * 110.574
    return math.hypot(dx, dy)


def _allowed_days(client: Any, calendar_df: pd.DataFrame) -> list[int]:
    fixed = _split_weekdays(getattr(client, "fixed_weekday", None))
    forbidden = _split_weekdays(getattr(client, "forbidden_weekdays", None))
    days: list[int] = []
    for day in calendar_df.itertuples(index=False):
        weekday = str(day.weekday)
        if fixed and weekday not in fixed:
            continue
        if forbidden and weekday in forbidden:
            continue
        days.append(int(day.day_index))
    return days


def _frequency_patterns(client: Any, calendar_df: pd.DataFrame, config: dict) -> list[tuple[int, ...]]:
    """Generate feasible monthly visit-day patterns for one client."""
    allowed = set(_allowed_days(client, calendar_df))
    if not allowed:
        return []

    day_lookup = calendar_df.set_index("day_index").to_dict("index")
    week_days = {
        int(week): [int(day) for day in group["day_index"].tolist() if int(day) in allowed]
        for week, group in calendar_df.groupby("week_index")
    }
    weekday_days = {
        int(weekday_index): [int(day) for day in group["day_index"].tolist() if int(day) in allowed]
        for weekday_index, group in calendar_df.groupby("weekday_index")
    }

    freq = int(client.visit_frequency)
    if freq == 4:
        if bool(config.get("weekday_consistency", {}).get("frequency_4_same_weekday", True)):
            return [
                tuple(days)
                for _, days in sorted(weekday_days.items())
                if len(days) == int(calendar_df["week_index"].nunique())
            ]
        per_week = [week_days[week] for week in sorted(week_days)]
        if any(not days for days in per_week):
            return []
        return [tuple(pattern) for pattern in product(*per_week)]

    if freq == 8:
        if not bool(config.get("weekday_consistency", {}).get("frequency_8_same_weekday_pair", True)):
            per_week_pairs = [list(combinations(sorted(week_days[week]), 2)) for week in sorted(week_days)]
            if any(not pairs for pairs in per_week_pairs):
                return []
            return [tuple(day for pair in pattern for day in pair) for pattern in product(*per_week_pairs)]

        patterns: list[tuple[int, ...]] = []
        weekday_indices = sorted(calendar_df["weekday_index"].astype(int).unique())
        for wd1, wd2 in combinations(weekday_indices, 2):
            days: list[int] = []
            feasible = True
            for week in sorted(week_days):
                d1 = int((week - 1) * len(weekday_indices) + wd1)
                d2 = int((week - 1) * len(weekday_indices) + wd2)
                if d1 not in allowed or d2 not in allowed:
                    feasible = False
                    break
                days.extend([d1, d2])
            if feasible:
                patterns.append(tuple(days))
        return patterns

    if freq == 2:
        patterns = []
        if bool(config.get("weekday_consistency", {}).get("frequency_2_same_weekday", True)):
            for _, days in sorted(weekday_days.items()):
                patterns.extend(tuple(pair) for pair in combinations(sorted(days), 2))
            return patterns
        for d1, d2 in combinations(sorted(allowed), 2):
            # Avoid same-week pairs by default; they are rarely useful for a
            # two-visit monthly customer and make schedules look accidental.
            if int(day_lookup[d1]["week_index"]) == int(day_lookup[d2]["week_index"]):
                continue
            patterns.append((d1, d2))
        return patterns or [tuple(pair) for pair in combinations(sorted(allowed), 2)]

    return []


def _territory_centers(clients_df: pd.DataFrame, calendar_df: pd.DataFrame) -> dict[int, tuple[float, float]]:
    centers: dict[int, tuple[float, float]] = {}
    if "territory_weekday_index" in clients_df.columns:
        for day in calendar_df.itertuples(index=False):
            weekday_index = int(day.weekday_index)
            territory_df = clients_df[pd.to_numeric(clients_df["territory_weekday_index"], errors="coerce").eq(weekday_index)]
            if not territory_df.empty:
                centers[weekday_index] = (float(territory_df["lat"].mean()), float(territory_df["lon"].mean()))
    if not centers:
        centers = {
            int(day.weekday_index): (float(clients_df["lat"].mean()), float(clients_df["lon"].mean()))
            for day in calendar_df.itertuples(index=False)
        }
    return centers


def _pattern_cost(client: Any, pattern: tuple[int, ...], calendar_df: pd.DataFrame, config: dict, centers: dict[int, tuple[float, float]]) -> int:
    weights = config["weights"]
    day_lookup = calendar_df.set_index("day_index").to_dict("index")
    preferred = _split_weekdays(getattr(client, "preferred_weekdays", None))
    territory = getattr(client, "territory_weekday_index", None)
    cost = 0.0

    for day in pattern:
        meta = day_lookup[int(day)]
        weekday = str(meta["weekday"])
        weekday_index = int(meta["weekday_index"])
        if preferred and weekday not in preferred:
            cost += int(weights.get("preferred_weekday_violation", 500))
        if territory is not None and not pd.isna(territory) and weekday_index != int(territory):
            cost += int(weights.get("territory_client_weekday_violation", weights.get("territory_weekday_violation", 200000)))

        center = centers.get(weekday_index)
        if center is not None:
            cost += _distance_km(float(client.lat), float(client.lon), center[0], center[1]) * int(weights.get("route_km", 1000))

    if int(client.visit_frequency) == 2 and len(pattern) == 2:
        gap = abs(int(pattern[1]) - int(pattern[0]))
        ideal_gap = max(1, int(calendar_df["day_index"].max()) // 2)
        cost += abs(gap - ideal_gap) * int(weights.get("bad_spacing_frequency_2", 2000))

    if int(client.visit_frequency) == 8:
        for week in sorted(calendar_df["week_index"].unique()):
            week_days = [day for day in pattern if int(day_lookup[day]["week_index"]) == int(week)]
            if len(week_days) == 2 and abs(int(week_days[0]) - int(week_days[1])) == 1:
                cost += int(weights.get("bad_spacing_frequency_8", 2000))

    return int(round(cost))


def _route_estimate_km(client_ids: list[str], clients_df: pd.DataFrame) -> float:
    """Nearest-neighbor lat/lon estimate used only before final PyVRP costing."""
    if len(client_ids) <= 1:
        return 0.0
    lookup = clients_df.set_index("client_id")[["lat", "lon"]].astype(float).to_dict("index")
    remaining = set(client_ids)
    current = min(remaining)
    remaining.remove(current)
    total = 0.0
    while remaining:
        lat1, lon1 = lookup[current]["lat"], lookup[current]["lon"]
        nxt = min(remaining, key=lambda cid: _distance_km(lat1, lon1, lookup[cid]["lat"], lookup[cid]["lon"]))
        total += _distance_km(lat1, lon1, lookup[nxt]["lat"], lookup[nxt]["lon"])
        current = nxt
        remaining.remove(current)
    return float(total)


def _coverage_frame(clients_df: pd.DataFrame, pattern_counts: dict[str, int]) -> pd.DataFrame:
    rows = []
    for row in clients_df.itertuples(index=False):
        count = int(pattern_counts.get(str(row.client_id), 0))
        rows.append(
            {
                "sales_rep": row.sales_rep,
                "client_id": str(row.client_id),
                "client_name": row.client_name,
                "visit_frequency": int(row.visit_frequency),
                "number_of_candidates_containing_client": count,
                "min_recommended_candidate_coverage": 1,
                "severity": "ERROR" if count == 0 else "OK",
            }
        )
    return pd.DataFrame(rows)


def _solve_one_rep(clients_df: pd.DataFrame, calendar_df: pd.DataFrame, config: dict, time_limit_seconds: float) -> dict[str, Any]:
    model = cp_model.CpModel()
    days = calendar_df["day_index"].astype(int).tolist()
    day_lookup = calendar_df.set_index("day_index").to_dict("index")
    centers = _territory_centers(clients_df, calendar_df)
    weights = config["weights"]

    x_by_client: dict[str, list[tuple[cp_model.IntVar, tuple[int, ...], int]]] = {}
    pattern_counts: dict[str, int] = {}
    day_terms: dict[int, list[cp_model.IntVar]] = defaultdict(list)
    cluster_day_terms: dict[tuple[str, int], list[cp_model.IntVar]] = defaultdict(list)
    objective_terms = []

    for client in clients_df.itertuples(index=False):
        cid = str(client.client_id)
        patterns = _frequency_patterns(client, calendar_df, config)
        pattern_counts[cid] = len(patterns)
        if not patterns:
            return {
                "status": "INFEASIBLE",
                "selected_candidates": pd.DataFrame(),
                "objective_value": None,
                "solver_wall_time": 0.0,
                "coverage": _coverage_frame(clients_df, pattern_counts),
                "warnings": [f"Client {cid} has no feasible visit-day pattern."],
            }

        choices: list[tuple[cp_model.IntVar, tuple[int, ...], int]] = []
        for pattern_index, pattern in enumerate(patterns):
            var = model.NewBoolVar(f"pattern_{cid}_{pattern_index}")
            cost = _pattern_cost(client, pattern, calendar_df, config, centers)
            choices.append((var, pattern, cost))
            objective_terms.append(cost * var)
            for day in pattern:
                day_terms[int(day)].append(var)
                cluster_day_terms[(str(getattr(client, "cluster_id", "unknown")), int(day))].append(var)
        model.Add(sum(var for var, _, _ in choices) == 1)

        best_var = min(choices, key=lambda item: item[2])[0]
        model.AddHint(best_var, 1)
        x_by_client[cid] = choices

    target = int(config["daily_route"]["target_clients"])
    min_clients = int(config["daily_route"]["min_clients"])
    max_clients = int(config["daily_route"]["max_clients"])
    allow_under = bool(config["daily_route"].get("allow_underfilled", True))
    allow_over = bool(config["daily_route"].get("allow_overfilled", False))

    for day in days:
        load = model.NewIntVar(0, max(10000, len(clients_df) * 8), f"load_{day}")
        model.Add(load == sum(day_terms.get(day, [])))
        if allow_under:
            under = model.NewIntVar(0, max_clients, f"under_target_{day}")
            model.Add(under >= target - load)
            objective_terms.append(int(weights.get("underfilled_route", 500)) * under)
        else:
            model.Add(load >= min_clients)
        if allow_over:
            over = model.NewIntVar(0, max(10000, len(clients_df)), f"over_target_{day}")
            model.Add(over >= load - target)
            objective_terms.append(int(weights.get("over_target_clients", 300)) * over)
        else:
            model.Add(load <= max_clients)
            over_target = model.NewIntVar(0, max_clients, f"over_soft_target_{day}")
            model.Add(over_target >= load - target)
            objective_terms.append(int(weights.get("over_target_clients", 300)) * over_target)

    for (cluster_id, day), terms in cluster_day_terms.items():
        active = model.NewBoolVar(f"cluster_active_{cluster_id}_{day}")
        for term in terms:
            model.Add(active >= term)
        objective_terms.append(int(weights.get("cluster_mixing", 300)) * active)

    model.Minimize(sum(objective_terms))
    solver = cp_model.CpSolver()
    solver.parameters.max_time_in_seconds = float(time_limit_seconds)
    solver.parameters.num_search_workers = int(config["optimization"].get("num_workers", 8))
    solver.parameters.log_search_progress = bool(config["optimization"].get("log_search_progress", False))
    solver.parameters.stop_after_first_solution = bool(config["optimization"].get("stop_after_first_solution", False))
    solver.parameters.random_seed = int(config["candidate_routes"].get("random_seed", 42))
    status = solver.Solve(model)
    status_name = solver.StatusName(status)

    if status not in {cp_model.OPTIMAL, cp_model.FEASIBLE}:
        return {
            "status": status_name,
            "selected_candidates": pd.DataFrame(),
            "objective_value": None,
            "solver_wall_time": solver.WallTime(),
            "coverage": _coverage_frame(clients_df, pattern_counts),
            "warnings": ["Pattern CP-SAT did not find a feasible client-day assignment."],
        }

    clients_by_day: dict[int, list[str]] = {day: [] for day in days}
    for cid, choices in x_by_client.items():
        for var, pattern, _ in choices:
            if solver.Value(var):
                for day in pattern:
                    clients_by_day[int(day)].append(cid)
                break

    client_lookup = clients_df.set_index("client_id").to_dict("index")
    rows = []
    sales_rep = str(clients_df["sales_rep"].iloc[0])
    for day in days:
        client_ids = sorted(clients_by_day[day], key=lambda cid: (str(client_lookup[cid].get("cluster_id", "")), cid))
        if not client_ids:
            continue
        clusters = [str(client_lookup[cid].get("cluster_id", "")) for cid in client_ids]
        cluster_counts = pd.Series(clusters).value_counts()
        route_id = f"{sales_rep.replace(' ', '_')}_day_{day:02d}"
        row = {
            "candidate_id": route_id,
            "selected_candidate_id": route_id,
            "day_index": day,
            "sales_rep": sales_rep,
            "client_ids": client_ids,
            "number_of_clients": len(client_ids),
            "route_km": _route_estimate_km(client_ids, clients_df),
            "route_duration_min": None,
            "main_cluster": str(cluster_counts.index[0]) if not cluster_counts.empty else "",
            "clusters_used": ",".join(sorted(set(clusters))),
            "cluster_count": int(cluster_counts.size),
            "generation_method": "client_day_pattern_cp_sat",
            "underfilled_penalty": max(0, target - len(client_ids)),
            "overfilled_penalty": max(0, len(client_ids) - target),
            "cluster_mixing_penalty": max(0, int(cluster_counts.size) - 1),
        }
        row.update(day_lookup[day])
        rows.append(row)

    return {
        "status": status_name,
        "selected_candidates": pd.DataFrame(rows),
        "objective_value": solver.ObjectiveValue(),
        "solver_wall_time": solver.WallTime(),
        "coverage": _coverage_frame(clients_df, pattern_counts),
        "warnings": [],
    }


def solve_day_pattern_master(clients_df: pd.DataFrame, calendar_df: pd.DataFrame, config: dict) -> dict[str, Any]:
    """Solve the month-level client-day assignment problem."""
    sales_reps = sorted(str(rep) for rep in clients_df["sales_rep"].dropna().unique())
    total_time_limit = float(config["optimization"]["time_limit_seconds"])
    per_rep_time_limit = max(30.0, total_time_limit / max(1, len(sales_reps)))

    selected_frames: list[pd.DataFrame] = []
    coverage_frames: list[pd.DataFrame] = []
    statuses: list[str] = []
    warnings: list[str] = []
    objective_value = 0.0
    wall_time = 0.0

    for sales_rep in sales_reps:
        rep_clients = clients_df[clients_df["sales_rep"].astype(str).eq(sales_rep)].copy()
        result = _solve_one_rep(rep_clients, calendar_df, config, per_rep_time_limit)
        statuses.append(str(result["status"]))
        wall_time += float(result["solver_wall_time"])
        coverage_frames.append(result.get("coverage", pd.DataFrame()))
        if result["selected_candidates"].empty:
            warnings.extend([f"{sales_rep}: {warning}" for warning in result.get("warnings", [])])
            return {
                "status": str(result["status"]),
                "selected_candidates": pd.DataFrame(),
                "objective_value": None,
                "solver_wall_time": wall_time,
                "coverage": pd.concat(coverage_frames, ignore_index=True) if coverage_frames else pd.DataFrame(),
                "warnings": warnings,
            }
        selected_frames.append(result["selected_candidates"])
        if result.get("objective_value") is not None:
            objective_value += float(result["objective_value"])

    if all(status == "OPTIMAL" for status in statuses):
        status_name = "OPTIMAL"
    elif all(status in {"OPTIMAL", "FEASIBLE"} for status in statuses):
        status_name = "FEASIBLE"
    else:
        status_name = ",".join(statuses)

    return {
        "status": status_name,
        "selected_candidates": pd.concat(selected_frames, ignore_index=True) if selected_frames else pd.DataFrame(),
        "objective_value": objective_value,
        "solver_wall_time": wall_time,
        "coverage": pd.concat(coverage_frames, ignore_index=True) if coverage_frames else pd.DataFrame(),
        "warnings": warnings,
    }
