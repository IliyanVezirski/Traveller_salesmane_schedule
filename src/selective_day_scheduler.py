"""Iterative day scheduler where PyVRP selects compact clients from a pool."""

from __future__ import annotations

from itertools import combinations
import math
from typing import Any

import numpy as np
import pandas as pd

from .route_costing import calculate_route_cost


def _split_weekdays(value: Any) -> set[str]:
    if pd.isna(value) or value is None or str(value).strip() == "":
        return set()
    return {part.strip() for part in str(value).replace(";", ",").split(",") if part.strip()}


def _distance_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    mean_lat = math.radians((lat1 + lat2) / 2.0)
    dx = (lon2 - lon1) * 111.320 * math.cos(mean_lat)
    dy = (lat2 - lat1) * 110.574
    return math.hypot(dx, dy)


def _compactness_strength(config: dict) -> float:
    cfg = config.get("selective_day_routing", {})
    try:
        strength = float(cfg.get("compactness_strength", 1.0))
    except (TypeError, ValueError):
        strength = 1.0
    if not math.isfinite(strength):
        strength = 1.0
    return min(4.0, max(0.25, strength))


def _effective_pool_size(config: dict) -> int:
    cfg = config.get("selective_day_routing", {})
    configured_pool_size = int(cfg.get("pool_size", 60))
    strength = _compactness_strength(config)
    target = int(config["daily_route"]["target_clients"])
    max_clients = int(config["daily_route"]["max_clients"])
    min_pool = int(cfg.get("min_pool_size", max(max_clients, target + 5)))
    return max(min_pool, int(math.ceil(configured_pool_size / strength)))


def _weekday_allowed(client: dict[str, Any], weekday: str) -> bool:
    fixed = _split_weekdays(client.get("fixed_weekday"))
    forbidden = _split_weekdays(client.get("forbidden_weekdays"))
    if fixed and weekday not in fixed:
        return False
    return weekday not in forbidden


def _weekday_consistency_enabled(config: dict, key: str) -> bool:
    return bool(config.get("weekday_consistency", {}).get(key, True))


def _assigned_weekdays(state: dict[str, Any]) -> list[int]:
    values = state.get("assigned_weekday_indices")
    if values is not None:
        return [int(value) for value in values]
    value = state.get("assigned_weekday_index")
    return [] if value is None else [int(value)]


def _future_same_weekday_count(
    client: dict[str, Any],
    state: dict[str, Any],
    calendar_df: pd.DataFrame,
    start_day: int,
    weekday_index: int,
) -> int:
    count = 0
    for day in calendar_df[calendar_df["day_index"].astype(int).ge(int(start_day))].itertuples(index=False):
        if int(day.weekday_index) != int(weekday_index):
            continue
        if int(day.day_index) in state["selected_days"]:
            continue
        if not _weekday_allowed(client, str(day.weekday)):
            continue
        count += 1
    return count


def _territory_centers(clients_df: pd.DataFrame, calendar_df: pd.DataFrame) -> dict[int, tuple[float, float]]:
    centers: dict[int, tuple[float, float]] = {}
    if "territory_weekday_index" in clients_df.columns:
        for day in calendar_df.itertuples(index=False):
            idx = int(day.weekday_index)
            part = clients_df[pd.to_numeric(clients_df["territory_weekday_index"], errors="coerce").eq(idx)]
            if not part.empty:
                centers[idx] = (float(part["lat"].mean()), float(part["lon"].mean()))
    fallback = (float(clients_df["lat"].mean()), float(clients_df["lon"].mean()))
    return {int(day.weekday_index): centers.get(int(day.weekday_index), fallback) for day in calendar_df.itertuples(index=False)}


def _allowed_weekday_indices(client: dict[str, Any], calendar_df: pd.DataFrame) -> list[int]:
    weekdays = calendar_df[["weekday_index", "weekday"]].drop_duplicates().sort_values("weekday_index")
    return [int(row.weekday_index) for row in weekdays.itertuples(index=False) if _weekday_allowed(client, str(row.weekday))]


def _preassign_frequency4_weekdays(clients_df: pd.DataFrame, calendar_df: pd.DataFrame, config: dict) -> dict[str, int]:
    """Assign freq=4 customers to one weekday, preferring territory compactness."""
    client_lookup = clients_df.set_index("client_id").to_dict("index")
    max_clients = int(config["daily_route"]["max_clients"])
    cfg = config.get("selective_day_routing", {})
    strength = _compactness_strength(config)
    balance_weight = float(cfg.get("freq4_weekday_balance_weight", 50_000))
    territory_weight = float(cfg.get("freq4_territory_mismatch_penalty", 2_000)) * strength
    overload_weight = float(cfg.get("freq4_weekday_overload_penalty", 1_000_000))
    base_capacity_ratio = float(cfg.get("freq4_weekday_capacity_ratio", 0.85))
    if strength >= 1.0:
        capacity_ratio = min(1.0, base_capacity_ratio + (strength - 1.0) * 0.05)
    else:
        capacity_ratio = max(0.5, base_capacity_ratio - (1.0 - strength) * 0.20)
    soft_cap = max(1, int(math.ceil(max_clients * capacity_ratio)))

    assignments: dict[str, int] = {}
    loads: dict[int, int] = {int(idx): 0 for idx in sorted(calendar_df["weekday_index"].astype(int).unique())}
    deferred: list[tuple[str, dict[str, Any], list[int]]] = []

    for client_id, client in sorted(client_lookup.items(), key=lambda item: str(item[0])):
        if int(client["visit_frequency"]) != 4:
            continue
        allowed = _allowed_weekday_indices(client, calendar_df)
        if not allowed:
            continue
        if len(allowed) == 1:
            weekday_index = allowed[0]
            assignments[str(client_id)] = weekday_index
            loads[weekday_index] += 1
        else:
            deferred.append((str(client_id), client, allowed))

    def territory_index(client: dict[str, Any]) -> int | None:
        territory = client.get("territory_weekday_index")
        if territory is None or pd.isna(territory):
            return None
        return int(territory)

    def score(client: dict[str, Any], weekday_index: int) -> tuple[float, float, int]:
        projected_load = loads[weekday_index] + 1
        territory = territory_index(client)
        territory_penalty = 0.0
        if territory is not None and territory != int(weekday_index):
            territory_penalty = territory_weight
        overload_penalty = max(0, projected_load - soft_cap) * overload_weight
        return (overload_penalty + projected_load * balance_weight + territory_penalty, territory_penalty, weekday_index)

    # Place constrained clients first. For the rest, keep their territory day
    # whenever there is room, and only spill overflow to nearby load-balanced
    # weekdays. This keeps each day geographically recognizable.
    deferred.sort(key=lambda item: (len(item[2]), str(territory_index(item[1])), item[0]))
    for client_id, client, allowed in deferred:
        territory = territory_index(client)
        if territory in allowed and loads[int(territory)] < soft_cap:
            weekday_index = int(territory)
        else:
            weekday_index = min(allowed, key=lambda idx: score(client, idx))
        assignments[client_id] = weekday_index
        loads[weekday_index] += 1

    return assignments


def _preassign_frequency2_patterns(
    clients_df: pd.DataFrame,
    calendar_df: pd.DataFrame,
    config: dict,
    frequency4_assignments: dict[str, int],
) -> dict[str, tuple[int, tuple[int, int]]]:
    """Assign freq=2 customers to one repeat weekday and two balanced weeks."""
    client_lookup = {str(client_id): client for client_id, client in clients_df.set_index("client_id").to_dict("index").items()}
    cfg = config.get("selective_day_routing", {})
    strength = _compactness_strength(config)
    max_clients = int(config["daily_route"]["max_clients"])
    weeks = sorted(int(week) for week in calendar_df["week_index"].astype(int).unique())
    ideal_gap = max(1, len(weeks) // 2)
    week_pairs = sorted(combinations(weeks, 2), key=lambda pair: (abs((pair[1] - pair[0]) - ideal_gap), pair))
    balance_weight = float(cfg.get("frequency2_weekday_balance_weight", 5_000))
    territory_weight = float(cfg.get("territory_mismatch_penalty", 25_000)) * strength
    overload_weight = float(cfg.get("frequency2_weekday_overload_penalty", 1_000_000))
    spacing_weight = float(cfg.get("frequency2_phase_spacing_weight", 1_000))

    assignments: dict[str, tuple[int, tuple[int, int]]] = {}
    loads: dict[tuple[int, int], int] = {
        (int(day.week_index), int(day.weekday_index)): 0 for day in calendar_df.itertuples(index=False)
    }
    for client_id, weekday_index in frequency4_assignments.items():
        if str(client_id) in client_lookup:
            for week in weeks:
                loads[(int(week), int(weekday_index))] += 1

    deferred: list[tuple[str, dict[str, Any], list[int]]] = []
    for client_id, client in sorted(client_lookup.items(), key=lambda item: str(item[0])):
        if int(client["visit_frequency"]) != 2:
            continue
        allowed = _allowed_weekday_indices(client, calendar_df)
        if not allowed:
            continue
        if len(allowed) == 1:
            deferred.append((str(client_id), client, allowed))
        else:
            deferred.append((str(client_id), client, allowed))

    def territory_index(client: dict[str, Any]) -> int | None:
        territory = client.get("territory_weekday_index")
        if territory is None or pd.isna(territory):
            return None
        return int(territory)

    def score(client: dict[str, Any], weekday_index: int, week_pair: tuple[int, int]) -> tuple[float, float, float, int, tuple[int, int]]:
        territory = territory_index(client)
        territory_penalty = 0.0
        if territory is not None and territory != int(weekday_index):
            territory_penalty = territory_weight
        projected_loads = [loads[(int(week), int(weekday_index))] + 1 for week in week_pair]
        overload_penalty = sum(max(0, load - max_clients) for load in projected_loads) * overload_weight
        balance_penalty = sum(projected_loads) * balance_weight
        spacing_penalty = abs((int(week_pair[1]) - int(week_pair[0])) - ideal_gap) * spacing_weight
        return (overload_penalty + balance_penalty + territory_penalty + spacing_penalty, territory_penalty, spacing_penalty, weekday_index, week_pair)

    deferred.sort(key=lambda item: (len(item[2]), str(territory_index(item[1])), item[0]))
    for client_id, client, allowed in deferred:
        territory = territory_index(client)
        candidate_patterns = [(weekday_index, week_pair) for weekday_index in allowed for week_pair in week_pairs]
        if territory in allowed:
            territory_patterns = [(int(territory), week_pair) for week_pair in week_pairs]
            no_overload = [
                pattern
                for pattern in territory_patterns
                if all(loads[(int(week), int(territory))] + 1 <= max_clients for week in pattern[1])
            ]
            if no_overload:
                weekday_index, week_pair = min(no_overload, key=lambda pattern: score(client, pattern[0], pattern[1]))
            else:
                weekday_index, week_pair = min(candidate_patterns, key=lambda pattern: score(client, pattern[0], pattern[1]))
        else:
            weekday_index, week_pair = min(candidate_patterns, key=lambda pattern: score(client, pattern[0], pattern[1]))
        assignments[client_id] = (int(weekday_index), (int(week_pair[0]), int(week_pair[1])))
        for week in week_pair:
            loads[(int(week), int(weekday_index))] += 1

    return assignments


def _eligible_today(
    client: dict[str, Any],
    day: dict[str, Any],
    state: dict[str, Any],
    calendar_df: pd.DataFrame,
    config: dict,
) -> bool:
    if int(state["remaining_total"]) <= 0:
        return False
    weekday = str(day["weekday"])
    week = int(day["week_index"])
    if not _weekday_allowed(client, weekday):
        return False
    if int(day["day_index"]) in state["selected_days"]:
        return False

    freq = int(client["visit_frequency"])
    assigned = _assigned_weekdays(state)
    if freq == 4:
        if assigned and int(day["weekday_index"]) not in assigned:
            return False
        return int(state["visits_by_week"][week]) < 1
    if freq == 8:
        if _weekday_consistency_enabled(config, "frequency_8_same_weekday_pair"):
            if len(assigned) >= 2 and int(day["weekday_index"]) not in assigned:
                return False
        return int(state["visits_by_week"][week]) < 2
    if freq == 2:
        if _weekday_consistency_enabled(config, "frequency_2_same_weekday"):
            if assigned and int(day["weekday_index"]) not in assigned:
                return False
            assigned_visit_weeks = {int(week) for week in state.get("assigned_visit_weeks", [])}
            if assigned_visit_weeks and week not in assigned_visit_weeks:
                return False
            if not assigned:
                future_same_weekday = _future_same_weekday_count(client, state, calendar_df, int(day["day_index"]), int(day["weekday_index"]))
                if future_same_weekday < int(state["remaining_total"]):
                    return False
        return int(state["visits_by_week"][week]) < 1
    return False


def _future_eligible_days(
    client: dict[str, Any],
    state: dict[str, Any],
    calendar_df: pd.DataFrame,
    start_day: int,
    config: dict,
    *,
    same_week_only: bool = False,
    current_week: int | None = None,
) -> list[dict[str, Any]]:
    rows = []
    for day in calendar_df[calendar_df["day_index"].astype(int).ge(int(start_day))].itertuples(index=False):
        day_dict = day._asdict()
        if same_week_only and int(day_dict["week_index"]) != int(current_week):
            continue
        if _eligible_today(client, day_dict, state, calendar_df, config):
            rows.append(day_dict)
    return rows


def _remaining_week_opportunities(client: dict[str, Any], state: dict[str, Any], calendar_df: pd.DataFrame, start_day: int, config: dict) -> int:
    weeks = set()
    for day in _future_eligible_days(client, state, calendar_df, start_day, config):
        weeks.add(int(day["week_index"]))
    return len(weeks)


def _is_required_today(client: dict[str, Any], day: dict[str, Any], state: dict[str, Any], calendar_df: pd.DataFrame, config: dict) -> bool:
    freq = int(client["visit_frequency"])
    day_index = int(day["day_index"])
    week = int(day["week_index"])
    if freq == 4:
        need = 1 - int(state["visits_by_week"][week])
        opportunities = _future_eligible_days(client, state, calendar_df, day_index, config, same_week_only=True, current_week=week)
        return need > 0 and len(opportunities) <= need
    if freq == 8:
        need = 2 - int(state["visits_by_week"][week])
        opportunities = _future_eligible_days(client, state, calendar_df, day_index, config, same_week_only=True, current_week=week)
        return need > 0 and len(opportunities) <= need
    if freq == 2:
        opportunities = _future_eligible_days(client, state, calendar_df, day_index, config)
        return int(state["remaining_total"]) > 0 and len(opportunities) <= int(state["remaining_total"])
    return False


def _client_priority(client: dict[str, Any], day: dict[str, Any], state: dict[str, Any], centers: dict[int, tuple[float, float]], config: dict) -> float:
    cfg = config.get("selective_day_routing", {})
    strength = _compactness_strength(config)
    center = centers[int(day["weekday_index"])]
    dist = _distance_km(float(client["lat"]), float(client["lon"]), center[0], center[1])
    territory = client.get("territory_weekday_index")
    territory_bonus = 0.0
    territory_mismatch_penalty = 0.0
    if territory is not None and not pd.isna(territory):
        if int(territory) == int(day["weekday_index"]):
            territory_bonus = float(cfg.get("territory_bonus", 2_000))
        else:
            territory_mismatch_penalty = float(cfg.get("territory_mismatch_penalty", 25_000)) * strength
    preferred_bonus = float(cfg.get("preferred_bonus", 1_000)) if str(day["weekday"]) in _split_weekdays(client.get("preferred_weekdays")) else 0.0
    urgency = float(state["remaining_total"]) * float(cfg.get("urgency_bonus", 5_000))
    spacing_adjustment = 0.0
    if int(client["visit_frequency"]) == 2 and state["selected_days"]:
        last_visit = max(int(day_index) for day_index in state["selected_days"])
        gap = int(day["day_index"]) - last_visit
        ideal_gap = float(cfg.get("frequency2_ideal_gap_days", 10))
        if gap < ideal_gap:
            spacing_adjustment -= (ideal_gap - gap) * float(cfg.get("frequency2_close_gap_penalty", 1_500))
        else:
            spacing_adjustment += min(gap, ideal_gap * 2) * float(cfg.get("frequency2_spacing_bonus", 200))
    return urgency + territory_bonus + preferred_bonus + spacing_adjustment - territory_mismatch_penalty - dist * float(cfg.get("distance_penalty", 250)) * strength


def _select_with_pyvrp(
    pool: list[dict[str, Any]],
    required_ids: set[str],
    capacity: int,
    matrix_data: dict[str, Any],
    config: dict,
) -> tuple[list[str], str]:
    if not pool or capacity <= 0:
        return [], "empty_pool"

    try:
        from pyvrp import Model
        from pyvrp.stop import MaxIterations, MaxRuntime
    except ImportError:
        return _fallback_select(pool, required_ids, capacity, matrix_data), "nearest_neighbor_select_fallback_no_pyvrp"

    client_ids = [str(row["client_id"]) for row in pool]
    id_to_idx = {cid: idx for idx, cid in enumerate(matrix_data["client_ids"])}
    distance_matrix = np.asarray(matrix_data["distance_matrix_m"], dtype=float)
    select_cfg = config.get("selective_day_routing", {})

    model = Model()
    depot = model.add_depot(0, 0, name="dummy_open_route_depot")
    clients = []
    for idx, client_id in enumerate(client_ids):
        required = client_id in required_ids
        prize = int(pool[idx].get("prize", select_cfg.get("prize_base", 100_000)))
        clients.append(
            model.add_client(
                idx + 1,
                0,
                delivery=1,
                prize=prize,
                required=required,
                name=client_id,
            )
        )
    model.add_vehicle_type(num_available=1, capacity=int(capacity), start_depot=depot, end_depot=depot)

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

    max_iterations = int(select_cfg.get("pyvrp_max_iterations", config["route_costing"].get("pyvrp_max_iterations", 500)) or 0)
    if max_iterations > 0:
        stop = MaxIterations(max_iterations)
    else:
        stop = MaxRuntime(float(select_cfg.get("pyvrp_time_limit_seconds", config["route_costing"].get("pyvrp_time_limit_seconds", 3))))
    result = model.solve(stop, seed=int(config["candidate_routes"].get("random_seed", 42)), collect_stats=False, display=False)
    if not result.is_feasible() or not result.best.routes():
        return _fallback_select(pool, required_ids, capacity, matrix_data), "nearest_neighbor_select_fallback_from_pyvrp"

    selected: list[str] = []
    for route in result.best.routes():
        selected.extend(client_ids[int(visit_idx) - 1] for visit_idx in route.visits())
    required_missing = sorted(required_ids - set(selected))
    if required_missing:
        selected = list(dict.fromkeys(required_missing + selected))
    return selected[:capacity], "pyvrp_optional_select"


def _fallback_select(pool: list[dict[str, Any]], required_ids: set[str], capacity: int, matrix_data: dict[str, Any]) -> list[str]:
    selected = [str(row["client_id"]) for row in pool if str(row["client_id"]) in required_ids]
    remaining = [str(row["client_id"]) for row in sorted(pool, key=lambda item: float(item.get("priority", 0.0)), reverse=True) if str(row["client_id"]) not in set(selected)]
    if not selected and remaining:
        selected.append(remaining.pop(0))
    id_to_idx = {cid: idx for idx, cid in enumerate(matrix_data["client_ids"])}
    matrix = np.asarray(matrix_data["distance_matrix_m"], dtype=float)
    while len(selected) < capacity and remaining:
        nxt = min(remaining, key=lambda cid: min(float(matrix[id_to_idx[cid], id_to_idx[s]]) for s in selected))
        selected.append(nxt)
        remaining.remove(nxt)
    return selected[:capacity]


def _coverage_frame(clients_df: pd.DataFrame, states: dict[str, dict[str, Any]]) -> pd.DataFrame:
    rows = []
    for row in clients_df.itertuples(index=False):
        cid = str(row.client_id)
        selected_count = int(len(states[cid]["selected_days"]))
        expected = int(row.visit_frequency)
        rows.append(
            {
                "sales_rep": row.sales_rep,
                "client_id": cid,
                "client_name": row.client_name,
                "visit_frequency": expected,
                "number_of_candidates_containing_client": selected_count,
                "min_recommended_candidate_coverage": expected,
                "severity": "OK" if selected_count == expected else "ERROR",
            }
        )
    return pd.DataFrame(rows)


def _solve_one_rep(clients_df: pd.DataFrame, calendar_df: pd.DataFrame, matrix_data: dict[str, Any], config: dict) -> dict[str, Any]:
    client_lookup = clients_df.set_index("client_id").to_dict("index")
    states: dict[str, dict[str, Any]] = {}
    weeks = sorted(calendar_df["week_index"].astype(int).unique())
    freq4_weekdays = _preassign_frequency4_weekdays(clients_df, calendar_df, config)
    freq2_patterns = (
        _preassign_frequency2_patterns(clients_df, calendar_df, config, freq4_weekdays)
        if _weekday_consistency_enabled(config, "frequency_2_same_weekday")
        else {}
    )
    for client_id, client in client_lookup.items():
        assigned_weekday = freq4_weekdays.get(str(client_id))
        assigned_visit_weeks: list[int] = []
        if assigned_weekday is None and int(client["visit_frequency"]) == 2:
            pattern = freq2_patterns.get(str(client_id))
            if pattern is not None:
                assigned_weekday = int(pattern[0])
                assigned_visit_weeks = [int(week) for week in pattern[1]]
        states[str(client_id)] = {
            "remaining_total": int(client["visit_frequency"]),
            "visits_by_week": {int(week): 0 for week in weeks},
            "selected_days": [],
            "assigned_weekday_index": assigned_weekday,
            "assigned_weekday_indices": [] if assigned_weekday is None else [int(assigned_weekday)],
            "assigned_visit_weeks": assigned_visit_weeks,
        }

    centers = _territory_centers(clients_df, calendar_df)
    cfg = config.get("selective_day_routing", {})
    target = int(config["daily_route"]["target_clients"])
    max_clients = int(config["daily_route"]["max_clients"])
    pool_size = _effective_pool_size(config)
    prize_base = int(cfg.get("prize_base", 100_000))
    selected_rows = []
    warnings: list[str] = []
    total_route_estimate = 0.0

    for day in calendar_df.sort_values("day_index").itertuples(index=False):
        day_dict = day._asdict()
        required_ids: set[str] = set()
        optional_rows: list[dict[str, Any]] = []
        for client_id, client in client_lookup.items():
            cid = str(client_id)
            state = states[cid]
            if not _eligible_today(client, day_dict, state, calendar_df, config):
                continue
            required = _is_required_today(client, day_dict, state, calendar_df, config)
            priority = _client_priority(client, day_dict, state, centers, config)
            prize = max(1, int(prize_base + priority))
            row = {"client_id": cid, "required": required, "priority": priority, "prize": prize}
            if required:
                required_ids.add(cid)
            optional_rows.append(row)

        required_rows = [row for row in optional_rows if str(row["client_id"]) in required_ids]
        optional_only = [row for row in optional_rows if str(row["client_id"]) not in required_ids]
        optional_only = sorted(optional_only, key=lambda row: float(row["priority"]), reverse=True)[: max(0, pool_size - len(required_rows))]
        pool = required_rows + optional_only
        remaining_total_visits = sum(max(0, int(state["remaining_total"])) for state in states.values())
        remaining_days = max(1, int(calendar_df["day_index"].max()) - int(day.day_index) + 1)
        dynamic_target = int(math.ceil(remaining_total_visits / remaining_days))
        capacity = min(max_clients, max(len(required_ids), dynamic_target))
        if len(required_ids) > max_clients:
            return {
                "status": "INFEASIBLE",
                "selected_candidates": pd.DataFrame(),
                "coverage": _coverage_frame(clients_df, states),
                "solver_wall_time": 0.0,
                "objective_value": None,
                "warnings": [f"{len(required_ids)} required clients on day {int(day.day_index)} exceed max_clients={max_clients}."],
            }

        selected_ids, method = _select_with_pyvrp(pool, required_ids, capacity, matrix_data, config)
        selected_ids = [cid for cid in selected_ids if cid in client_lookup]
        if not selected_ids:
            continue

        for cid in selected_ids:
            client = client_lookup[cid]
            state = states[cid]
            state["remaining_total"] = int(state["remaining_total"]) - 1
            state["visits_by_week"][int(day.week_index)] += 1
            state["selected_days"].append(int(day.day_index))
            assigned = _assigned_weekdays(state)
            weekday_index = int(day.weekday_index)
            freq = int(client["visit_frequency"])
            should_lock_single = freq == 4 or (freq == 2 and _weekday_consistency_enabled(config, "frequency_2_same_weekday"))
            should_lock_pair = freq == 8 and _weekday_consistency_enabled(config, "frequency_8_same_weekday_pair")
            if should_lock_single and not assigned:
                state["assigned_weekday_index"] = weekday_index
                state["assigned_weekday_indices"] = [weekday_index]
            elif should_lock_pair and weekday_index not in assigned and len(assigned) < 2:
                state["assigned_weekday_indices"] = sorted(assigned + [weekday_index])

        cost = calculate_route_cost(selected_ids, matrix_data, config["route_costing"].get("method", "nearest_neighbor_2opt"), config["route_costing"].get("route_type", "open"))
        total_route_estimate += float(cost["route_km"])
        clusters = [str(client_lookup[cid].get("cluster_id", "")) for cid in selected_ids]
        cluster_counts = pd.Series(clusters).value_counts()
        route_id = f"{str(clients_df['sales_rep'].iloc[0]).replace(' ', '_')}_day_{int(day.day_index):02d}"
        row = {
            "candidate_id": route_id,
            "selected_candidate_id": route_id,
            "day_index": int(day.day_index),
            "week_index": int(day.week_index),
            "weekday": str(day.weekday),
            "weekday_index": int(day.weekday_index),
            "sales_rep": str(clients_df["sales_rep"].iloc[0]),
            "client_ids": selected_ids,
            "number_of_clients": len(selected_ids),
            "route_km": float(cost["route_km"]),
            "route_duration_min": cost.get("route_duration_min"),
            "main_cluster": str(cluster_counts.index[0]) if not cluster_counts.empty else "",
            "clusters_used": ",".join(sorted(set(clusters))),
            "cluster_count": int(cluster_counts.size),
            "generation_method": method,
            "underfilled_penalty": max(0, target - len(selected_ids)),
            "overfilled_penalty": max(0, len(selected_ids) - target),
            "cluster_mixing_penalty": max(0, int(cluster_counts.size) - 1),
            "pool_size": len(pool),
            "required_clients": len(required_ids),
        }
        selected_rows.append(row)

    uncovered = []
    for cid, state in states.items():
        if int(state["remaining_total"]) != 0:
            uncovered.append(f"Client {cid} remaining visits={int(state['remaining_total'])}.")
    if uncovered:
        warnings.extend(uncovered[:50])
        return {
            "status": "INFEASIBLE",
            "selected_candidates": pd.DataFrame(selected_rows),
            "coverage": _coverage_frame(clients_df, states),
            "solver_wall_time": 0.0,
            "objective_value": total_route_estimate,
            "warnings": warnings,
        }

    return {
        "status": "FEASIBLE",
        "selected_candidates": pd.DataFrame(selected_rows),
        "coverage": _coverage_frame(clients_df, states),
        "solver_wall_time": 0.0,
        "objective_value": total_route_estimate,
        "warnings": warnings,
    }


def solve_selective_day_schedule(clients_df: pd.DataFrame, calendar_df: pd.DataFrame, matrix_data_by_rep: dict[str, dict[str, Any]], config: dict) -> dict[str, Any]:
    selected_frames: list[pd.DataFrame] = []
    coverage_frames: list[pd.DataFrame] = []
    warnings: list[str] = []
    objective_value = 0.0
    for sales_rep in sorted(str(rep) for rep in clients_df["sales_rep"].dropna().unique()):
        rep_clients = clients_df[clients_df["sales_rep"].astype(str).eq(sales_rep)].copy()
        result = _solve_one_rep(rep_clients, calendar_df, matrix_data_by_rep[sales_rep], config)
        coverage_frames.append(result.get("coverage", pd.DataFrame()))
        if result.get("objective_value") is not None:
            objective_value += float(result["objective_value"])
        if result["selected_candidates"].empty or str(result["status"]) not in {"FEASIBLE", "OPTIMAL"}:
            warnings.extend([f"{sales_rep}: {warning}" for warning in result.get("warnings", [])])
            return {
                "status": str(result["status"]),
                "selected_candidates": result["selected_candidates"],
                "coverage": pd.concat(coverage_frames, ignore_index=True) if coverage_frames else pd.DataFrame(),
                "solver_wall_time": 0.0,
                "objective_value": objective_value,
                "warnings": warnings,
            }
        selected_frames.append(result["selected_candidates"])
        warnings.extend([f"{sales_rep}: {warning}" for warning in result.get("warnings", [])])

    return {
        "status": "FEASIBLE",
        "selected_candidates": pd.concat(selected_frames, ignore_index=True) if selected_frames else pd.DataFrame(),
        "coverage": pd.concat(coverage_frames, ignore_index=True) if coverage_frames else pd.DataFrame(),
        "solver_wall_time": 0.0,
        "objective_value": objective_value,
        "warnings": warnings,
    }
