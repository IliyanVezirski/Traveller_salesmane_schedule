"""Excel export for final schedules and diagnostics."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd


def _flatten_config(config: dict, prefix: str = "") -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for key, value in config.items():
        name = f"{prefix}.{key}" if prefix else str(key)
        if isinstance(value, dict):
            rows.extend(_flatten_config(value, name))
        else:
            rows.append({"parameter": name, "value": ", ".join(map(str, value)) if isinstance(value, list) else value})
    return rows


def export_schedule_excel(
    path: str,
    final_schedule_df: pd.DataFrame,
    selected_candidates_df: pd.DataFrame,
    coverage_df: pd.DataFrame,
    validation_df: pd.DataFrame,
    clients_df: pd.DataFrame,
    config: dict,
) -> None:
    """Write the multi-sheet production schedule workbook."""
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    daily_route_cols = [
        "day_index",
        "week_index",
        "weekday",
        "sales_rep",
        "route_order",
        "client_id",
        "client_name",
        "distance_from_previous_km",
        "cumulative_km",
        "route_km_total",
        "final_route_method",
    ]
    daily_routes_df = final_schedule_df[[c for c in daily_route_cols if c in final_schedule_df.columns]].copy()

    routes = final_schedule_df.drop_duplicates(["sales_rep", "day_index"])
    summary_rep = clients_df.groupby("sales_rep").agg(total_clients=("client_id", "nunique"), required_monthly_visits=("visit_frequency", "sum")).reset_index()
    route_totals = final_schedule_df.drop_duplicates(["sales_rep", "day_index"])[["sales_rep", "route_km_total"]]
    planned = final_schedule_df.groupby("sales_rep").agg(planned_monthly_visits=("client_id", "count")).reset_index()
    planned = planned.merge(route_totals.groupby("sales_rep").agg(total_route_km=("route_km_total", "sum")).reset_index(), on="sales_rep", how="left")
    clients_per_day = final_schedule_df.groupby(["sales_rep", "day_index"]).size().reset_index(name="clients_day")
    day_stats = clients_per_day.groupby("sales_rep").agg(avg_clients_per_day=("clients_day", "mean"), min_clients_day=("clients_day", "min"), max_clients_day=("clients_day", "max")).reset_index()
    summary_rep = summary_rep.merge(planned, on="sales_rep", how="left").merge(day_stats, on="sales_rep", how="left")
    route_days = routes.groupby("sales_rep").size().rename("route_days").reset_index()
    summary_rep = summary_rep.merge(route_days, on="sales_rep", how="left")
    summary_rep["avg_route_km_per_day"] = summary_rep["total_route_km"] / summary_rep["route_days"].replace(0, pd.NA)
    errors = validation_df[validation_df["severity"].eq("ERROR")].groupby("sales_rep").size().rename("validation_errors").reset_index() if not validation_df.empty else pd.DataFrame(columns=["sales_rep", "validation_errors"])
    summary_rep = summary_rep.merge(errors, on="sales_rep", how="left").fillna({"validation_errors": 0})

    summary_day_cols = ["day_index", "week_index", "weekday", "sales_rep", "selected_candidate_id", "number_of_clients", "route_km", "main_cluster", "clusters_used"]
    summary_day = selected_candidates_df[[c for c in summary_day_cols if c in selected_candidates_df.columns]].copy()
    selected_cols = ["candidate_id", "selected_candidate_id", "day_index", "week_index", "weekday", "sales_rep", "number_of_clients", "route_km", "main_cluster", "clusters_used", "generation_method"]
    selected_export = selected_candidates_df[[c for c in selected_cols if c in selected_candidates_df.columns]].copy()

    final_cols = [
        "day_index",
        "week_index",
        "weekday",
        "sales_rep",
        "route_order",
        "client_id",
        "client_name",
        "lat",
        "lon",
        "visit_frequency",
        "cluster_id",
        "territory_weekday",
        "global_territory_cluster_id",
        "global_territory_weekday",
        "distance_from_previous_km",
        "cumulative_km",
        "route_km_total",
        "final_route_method",
    ]
    client_cols = [
        "sales_rep",
        "client_id",
        "client_name",
        "lat",
        "lon",
        "visit_frequency",
        "cluster_id",
        "territory_weekday_index",
        "territory_weekday",
        "global_territory_cluster_id",
        "global_territory_weekday_index",
        "global_territory_weekday",
    ]
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        final_schedule_df[[c for c in final_cols if c in final_schedule_df.columns]].to_excel(writer, sheet_name="Final_Schedule", index=False)
        daily_routes_df.to_excel(writer, sheet_name="Daily_Routes", index=False)
        summary_rep.to_excel(writer, sheet_name="Summary_By_Sales_Rep", index=False)
        summary_day.to_excel(writer, sheet_name="Summary_By_Day", index=False)
        validation_df.to_excel(writer, sheet_name="Validation", index=False)
        selected_export.to_excel(writer, sheet_name="Candidate_Routes_Selected", index=False)
        coverage_df.to_excel(writer, sheet_name="Candidate_Coverage", index=False)
        clients_df[[c for c in client_cols if c in clients_df.columns]].to_excel(writer, sheet_name="Clients_Geography", index=False)
        pd.DataFrame(_flatten_config(config)).to_excel(writer, sheet_name="Parameters", index=False)
