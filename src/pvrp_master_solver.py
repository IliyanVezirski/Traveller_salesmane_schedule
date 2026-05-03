"""OR-Tools CP-SAT master solver over pre-costed candidate routes."""

from __future__ import annotations

from collections import defaultdict
from typing import Any

import pandas as pd
from ortools.sat.python import cp_model


def _split_weekdays(value: Any) -> set[str]:
    if pd.isna(value) or value is None or str(value).strip() == "":
        return set()
    return {p.strip() for p in str(value).replace(";", ",").split(",") if p.strip()}


def _diagnostics(clients_df: pd.DataFrame, candidates_df: pd.DataFrame, config: dict) -> list[str]:
    warnings: list[str] = []
    containing: dict[str, int] = defaultdict(int)
    for ids in candidates_df["client_ids"]:
        for cid in ids:
            containing[str(cid)] += 1
    for row in clients_df.itertuples(index=False):
        if containing[str(row.client_id)] == 0:
            warnings.append(f"Client {row.client_id} has 0 candidate coverage.")
        elif containing[str(row.client_id)] < int(row.visit_frequency):
            warnings.append(f"Client {row.client_id} has low candidate coverage: {containing[str(row.client_id)]}.")
    max_capacity = int(config["working_days"]["weeks"]) * len(config["working_days"]["weekdays"]) * int(config["daily_route"]["max_clients"])
    for rep, rep_df in clients_df.groupby("sales_rep"):
        required = int(rep_df["visit_frequency"].sum())
        if required > max_capacity:
            warnings.append(f"Sales rep {rep} requires {required} visits but max capacity is {max_capacity}.")
    if not warnings:
        warnings.append("No obvious data/candidate diagnostic found. Try increasing candidates_per_rep, lowering min_clients, or increasing solver time.")
    return warnings


def solve_pvrp_master(clients_df: pd.DataFrame, calendar_df: pd.DataFrame, candidates_df: pd.DataFrame, config: dict) -> dict[str, Any]:
    """Solve the route-first PVRP master problem with CP-SAT."""
    model = cp_model.CpModel()
    weights = config["weights"]
    candidate_rows = {str(r.candidate_id): r for r in candidates_df.itertuples(index=False)}
    days = calendar_df["day_index"].astype(int).tolist()
    day_lookup = calendar_df.set_index("day_index").to_dict("index")

    z: dict[tuple[str, int], cp_model.IntVar] = {}
    for candidate_id in candidate_rows:
        for day in days:
            z[(candidate_id, day)] = model.NewBoolVar(f"z_{candidate_id}_{day}")

    # One route per rep per day.
    for sales_rep, rep_candidates in candidates_df.groupby("sales_rep"):
        rep_ids = rep_candidates["candidate_id"].astype(str).tolist()
        for day in days:
            model.Add(sum(z[(cid, day)] for cid in rep_ids) <= 1)

    client_to_candidate_ids: dict[str, list[str]] = defaultdict(list)
    for row in candidates_df.itertuples(index=False):
        for client_id in row.client_ids:
            client_to_candidate_ids[str(client_id)].append(str(row.candidate_id))

    objective_terms = []
    for row in candidates_df.itertuples(index=False):
        base = int(round(float(row.route_km) * float(weights["route_km"])))
        base += int(row.underfilled_penalty) * int(weights["underfilled_route"])
        base += int(row.overfilled_penalty) * int(weights["over_target_clients"])
        base += int(row.cluster_mixing_penalty) * int(weights["cluster_mixing"])
        for day in days:
            objective_terms.append(base * z[(str(row.candidate_id), day)])

    for client in clients_df.itertuples(index=False):
        cid = str(client.client_id)
        cand_ids = client_to_candidate_ids.get(cid, [])
        if not cand_ids:
            continue
        # Never visit the same client twice on one day.
        day_visit: dict[int, cp_model.IntVar] = {}
        for day in days:
            var = model.NewBoolVar(f"visit_{cid}_{day}")
            model.Add(var == sum(z[(candidate_id, day)] for candidate_id in cand_ids))
            model.Add(sum(z[(candidate_id, day)] for candidate_id in cand_ids) <= 1)
            day_visit[day] = var

        freq = int(client.visit_frequency)
        if freq == 2:
            model.Add(sum(day_visit.values()) == 2)
            week_visit = {}
            week_active = {}
            for week in sorted(calendar_df["week_index"].unique()):
                week_days = calendar_df.loc[calendar_df["week_index"].eq(week), "day_index"].astype(int).tolist()
                wv = model.NewIntVar(0, 2, f"week_visits_{cid}_{week}")
                wa = model.NewBoolVar(f"week_active_{cid}_{week}")
                model.Add(wv == sum(day_visit[d] for d in week_days))
                model.Add(wv >= 1).OnlyEnforceIf(wa)
                model.Add(wv == 0).OnlyEnforceIf(wa.Not())
                surplus = model.NewIntVar(0, 1, f"same_week_surplus_{cid}_{week}")
                model.Add(surplus >= wv - 1)
                objective_terms.append(int(weights["bad_spacing_frequency_2"]) * 8 * surplus)
                week_visit[int(week)] = wv
                week_active[int(week)] = wa
            pair_penalties = {(1, 3): 0, (2, 4): 0, (1, 4): 1, (1, 2): 5, (2, 3): 3, (3, 4): 5}
            for (w1, w2), penalty in pair_penalties.items():
                pair = model.NewBoolVar(f"freq2_pair_{cid}_{w1}_{w2}")
                model.Add(pair <= week_active[w1])
                model.Add(pair <= week_active[w2])
                model.Add(pair >= week_active[w1] + week_active[w2] - 1)
                if penalty:
                    objective_terms.append(int(weights["bad_spacing_frequency_2"]) * penalty * pair)
        elif freq == 4:
            for week in sorted(calendar_df["week_index"].unique()):
                week_days = calendar_df.loc[calendar_df["week_index"].eq(week), "day_index"].astype(int).tolist()
                model.Add(sum(day_visit[d] for d in week_days) == 1)
        elif freq == 8:
            for week in sorted(calendar_df["week_index"].unique()):
                week_days = calendar_df.loc[calendar_df["week_index"].eq(week), "day_index"].astype(int).tolist()
                model.Add(sum(day_visit[d] for d in week_days) == 2)
                by_weekday = {int(day_lookup[d]["weekday_index"]): day_visit[d] for d in week_days}
                for wd in range(4):
                    consecutive = model.NewBoolVar(f"consecutive_{cid}_{week}_{wd}")
                    model.Add(consecutive <= by_weekday[wd])
                    model.Add(consecutive <= by_weekday[wd + 1])
                    model.Add(consecutive >= by_weekday[wd] + by_weekday[wd + 1] - 1)
                    objective_terms.append(int(weights["bad_spacing_frequency_8"]) * consecutive)

        fixed = _split_weekdays(getattr(client, "fixed_weekday", None))
        forbidden = _split_weekdays(getattr(client, "forbidden_weekdays", None))
        preferred = _split_weekdays(getattr(client, "preferred_weekdays", None))
        for day, visit_var in day_visit.items():
            weekday = str(day_lookup[day]["weekday"])
            if forbidden and weekday in forbidden:
                model.Add(visit_var == 0)
            if fixed and weekday not in fixed:
                objective_terms.append(int(weights["fixed_weekday_violation"]) * visit_var)
            if preferred and weekday not in preferred:
                objective_terms.append(int(weights["preferred_weekday_violation"]) * visit_var)

    model.Minimize(sum(objective_terms))
    solver = cp_model.CpSolver()
    solver.parameters.max_time_in_seconds = float(config["optimization"]["time_limit_seconds"])
    solver.parameters.num_search_workers = int(config["optimization"]["num_workers"])
    solver.parameters.log_search_progress = bool(config["optimization"].get("log_search_progress", False))
    status = solver.Solve(model)
    status_name = solver.StatusName(status)

    if status not in {cp_model.OPTIMAL, cp_model.FEASIBLE}:
        return {"status": status_name, "selected_candidates": pd.DataFrame(), "objective_value": None, "solver_wall_time": solver.WallTime(), "warnings": _diagnostics(clients_df, candidates_df, config)}

    selected_rows = []
    for (candidate_id, day), var in z.items():
        if solver.Value(var):
            base = candidate_rows[candidate_id]._asdict()
            base.update(day_lookup[day])
            base["day_index"] = day
            base["selected_candidate_id"] = candidate_id
            selected_rows.append(base)
    selected_df = pd.DataFrame(selected_rows)
    return {"status": status_name, "selected_candidates": selected_df, "objective_value": solver.ObjectiveValue(), "solver_wall_time": solver.WallTime(), "warnings": []}
