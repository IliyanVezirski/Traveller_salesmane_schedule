"""Scoring and validation for final PVRP schedules."""

from __future__ import annotations

from typing import Any

import pandas as pd


def _validation_issue(severity: str, sales_rep: Any, client_id: Any, message: str) -> dict[str, Any]:
    return {"severity": severity, "sales_rep": sales_rep, "client_id": client_id, "message": message}


def validate_solution(daily_routes_df: pd.DataFrame, clients_df: pd.DataFrame, selected_candidates_df: pd.DataFrame, calendar_df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """Validate final stop-level schedule against frequency and routing rules."""
    issues: list[dict[str, Any]] = []
    if daily_routes_df.empty:
        return pd.DataFrame([_validation_issue("ERROR", None, None, "No daily routes were produced.")])

    for client in clients_df.itertuples(index=False):
        visits = daily_routes_df[daily_routes_df["client_id"].astype(str).eq(str(client.client_id))]
        freq = int(client.visit_frequency)
        if freq == 2 and len(visits) != 2:
            issues.append(_validation_issue("ERROR", client.sales_rep, client.client_id, f"Expected 2 monthly visits, got {len(visits)}."))
        if freq == 4:
            for week in range(1, 5):
                count = int(visits["week_index"].eq(week).sum())
                if count != 1:
                    issues.append(_validation_issue("ERROR", client.sales_rep, client.client_id, f"Expected 1 visit in week {week}, got {count}."))
        if freq == 8:
            for week in range(1, 5):
                count = int(visits["week_index"].eq(week).sum())
                if count != 2:
                    issues.append(_validation_issue("ERROR", client.sales_rep, client.client_id, f"Expected 2 visits in week {week}, got {count}."))
        duplicate_days = visits.groupby("day_index").size()
        if (duplicate_days > 1).any():
            issues.append(_validation_issue("ERROR", client.sales_rep, client.client_id, "Client is visited more than once on a day."))
        if not visits.empty and not visits["sales_rep"].eq(client.sales_rep).all():
            issues.append(_validation_issue("ERROR", client.sales_rep, client.client_id, "Client assigned to wrong sales_rep."))

    routes_per_rep_day = selected_candidates_df.groupby(["sales_rep", "day_index"]).size()
    for (sales_rep, day), count in routes_per_rep_day.items():
        if int(count) > 1:
            issues.append(_validation_issue("ERROR", sales_rep, None, f"More than one route on day {day}."))

    route_lengths = daily_routes_df.groupby(["sales_rep", "day_index"]).size()
    min_clients = int(config["daily_route"]["min_clients"])
    max_clients = int(config["daily_route"]["max_clients"])
    for (sales_rep, day), count in route_lengths.items():
        if count > max_clients:
            issues.append(_validation_issue("ERROR", sales_rep, None, f"Route day {day} has {count} clients over max {max_clients}."))
        elif count < min_clients:
            issues.append(_validation_issue("WARNING", sales_rep, None, f"Route day {day} is underfilled with {count} clients."))

    if daily_routes_df["route_km_total"].isna().any():
        issues.append(_validation_issue("ERROR", None, None, "At least one selected route has missing route_km."))
    if daily_routes_df["route_order"].isna().any():
        issues.append(_validation_issue("ERROR", None, None, "At least one selected route has missing route_order."))

    if not issues:
        issues.append(_validation_issue("OK", None, None, "Solution passed validation checks."))
    return pd.DataFrame(issues)


def score_solution(daily_routes_df: pd.DataFrame, validation_df: pd.DataFrame) -> dict[str, Any]:
    """Return simple aggregate score diagnostics."""
    return {
        "total_route_km": float(daily_routes_df.drop_duplicates(["sales_rep", "day_index"])["route_km_total"].sum()) if not daily_routes_df.empty else 0.0,
        "validation_errors": int(validation_df["severity"].eq("ERROR").sum()) if not validation_df.empty else 0,
        "routes": int(daily_routes_df.drop_duplicates(["sales_rep", "day_index"]).shape[0]) if not daily_routes_df.empty else 0,
    }
