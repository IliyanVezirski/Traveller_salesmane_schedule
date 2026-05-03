"""Validation of client input data and capacity sanity checks."""

from __future__ import annotations

from typing import Any

import pandas as pd


def _issue(severity: str, row: Any, field: str, message: str) -> dict[str, Any]:
    return {
        "severity": severity,
        "sales_rep": getattr(row, "sales_rep", None) if row is not None else None,
        "client_id": getattr(row, "client_id", None) if row is not None else None,
        "field": field,
        "message": message,
    }


def validate_clients(df: pd.DataFrame, config: dict) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Validate input clients and return clean rows plus a validation report."""
    issues: list[dict[str, Any]] = []

    for row in df.itertuples(index=False):
        if pd.isna(row.client_id) or str(row.client_id).strip() == "":
            issues.append(_issue("ERROR", row, "client_id", "client_id is required."))
        if pd.isna(row.client_name) or str(row.client_name).strip() == "":
            issues.append(_issue("WARNING", row, "client_name", "client_name is missing."))
        if pd.isna(row.sales_rep) or str(row.sales_rep).strip() == "":
            issues.append(_issue("ERROR", row, "sales_rep", "sales_rep is required."))
        if pd.isna(row.lat) or not (-90 <= float(row.lat) <= 90):
            issues.append(_issue("ERROR", row, "lat", "Latitude must be between -90 and 90."))
        if pd.isna(row.lon) or not (-180 <= float(row.lon) <= 180):
            issues.append(_issue("ERROR", row, "lon", "Longitude must be between -180 and 180."))
        if pd.isna(row.visit_frequency) or int(row.visit_frequency) not in {2, 4, 8}:
            issues.append(_issue("ERROR", row, "visit_frequency", "visit_frequency must be one of 2, 4, 8."))

    duplicated = df[df["client_id"].notna() & df["client_id"].duplicated(keep=False)]
    for row in duplicated.itertuples(index=False):
        issues.append(_issue("ERROR", row, "client_id", "client_id must be unique."))

    weeks = int(config["working_days"]["weeks"])
    days_per_week = len(config["working_days"]["weekdays"])
    max_clients = int(config["daily_route"]["max_clients"])
    max_capacity = weeks * days_per_week * max_clients
    for sales_rep, rep_df in df.groupby("sales_rep", dropna=True):
        total_required = int(pd.to_numeric(rep_df["visit_frequency"], errors="coerce").fillna(0).sum())
        pseudo_row = type("Row", (), {"sales_rep": sales_rep, "client_id": None})()
        if total_required > max_capacity:
            issues.append(_issue("ERROR", pseudo_row, "capacity", f"Required visits {total_required} exceed max capacity {max_capacity}."))
        elif total_required >= 0.9 * max_capacity:
            issues.append(_issue("WARNING", pseudo_row, "capacity", f"Required visits {total_required} are close to max capacity {max_capacity}."))

    validation_df = pd.DataFrame(issues, columns=["severity", "sales_rep", "client_id", "field", "message"])
    error_ids = set(validation_df.loc[validation_df["severity"].eq("ERROR") & validation_df["client_id"].notna(), "client_id"])
    clean_df = df[~df["client_id"].isin(error_ids)].copy()
    clean_df = clean_df[clean_df["sales_rep"].notna() & clean_df["lat"].notna() & clean_df["lon"].notna() & clean_df["visit_frequency"].isin([2, 4, 8])]
    clean_df["visit_frequency"] = clean_df["visit_frequency"].astype(int)
    return clean_df.reset_index(drop=True), validation_df
