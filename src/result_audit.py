"""Independent business-rule audit for exported PVRP schedules."""

from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from typing import Any

import pandas as pd
import yaml


DEFAULT_AUDIT_CONFIG: dict[str, Any] = {
    "working_days": {
        "weeks": 4,
        "weekdays": ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"],
    },
    "daily_route": {
        "target_clients": 20,
        "min_clients": 17,
        "max_clients": 22,
        "allow_overfilled": False,
    },
    "audit": {
        "route_km_median_multiplier_warning": 2.5,
        "max_clusters_per_route_warning": 5,
        "sofia_bounds": {
            "min_lat": 41.90,
            "max_lat": 43.10,
            "min_lon": 22.40,
            "max_lon": 24.30,
        },
    },
}

REQUIRED_CLIENT_COLUMNS = {
    "client_id",
    "client_name",
    "sales_rep",
    "lat",
    "lon",
    "visit_frequency",
}

EXPECTED_OPTIONAL_COLUMNS = [
    "fixed_weekday",
    "forbidden_weekdays",
    "preferred_weekdays",
    "cluster_manual",
    "notes",
]

CHECKS = [
    "input_validation",
    "frequency_correctness",
    "duplicate_same_day",
    "sales_rep_consistency",
    "one_route_per_rep_day",
    "daily_route_size",
    "route_km",
    "route_density",
    "coverage",
    "summary_consistency",
    "candidate_coverage",
]


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = deepcopy(base)
    for key, value in (override or {}).items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = deepcopy(value)
    return merged


def load_audit_config(config_path: str | None = None) -> dict[str, Any]:
    """Load and normalize audit config, defaulting to project business rules."""
    if not config_path:
        return deepcopy(DEFAULT_AUDIT_CONFIG)
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    with path.open("r", encoding="utf-8") as fh:
        raw = yaml.safe_load(fh) or {}
    return _deep_merge(DEFAULT_AUDIT_CONFIG, raw)


def _normalize_columns(df: pd.DataFrame | None) -> pd.DataFrame:
    if df is None:
        return pd.DataFrame()
    out = df.copy()
    out.columns = [str(c).strip().lower().replace(" ", "_") for c in out.columns]
    return out


def _calendar(config: dict[str, Any]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    weekdays = config["working_days"]["weekdays"]
    for week_index in range(1, int(config["working_days"]["weeks"]) + 1):
        for weekday_index, weekday in enumerate(weekdays):
            rows.append(
                {
                    "day_index": len(rows),
                    "week_index": week_index,
                    "weekday": weekday,
                    "weekday_index": weekday_index,
                }
            )
    return pd.DataFrame(rows)


def _coerce_clients(clients_df: pd.DataFrame) -> pd.DataFrame:
    out = _normalize_columns(clients_df)
    for col in EXPECTED_OPTIONAL_COLUMNS:
        if col not in out.columns:
            out[col] = pd.NA
    if "client_id" in out.columns:
        out["client_id"] = out["client_id"].astype("string").str.strip()
    if "client_name" in out.columns:
        out["client_name"] = out["client_name"].astype("string").str.strip()
    if "sales_rep" in out.columns:
        out["sales_rep"] = out["sales_rep"].astype("string").str.strip()
    for col in ["lat", "lon", "visit_frequency"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    return out


def _coerce_schedule(schedule_df: pd.DataFrame, calendar_df: pd.DataFrame) -> pd.DataFrame:
    out = _normalize_columns(schedule_df)
    if "client_id" in out.columns:
        out["client_id"] = out["client_id"].astype("string").str.strip()
    if "sales_rep" in out.columns:
        out["sales_rep"] = out["sales_rep"].astype("string").str.strip()
    for col in ["day_index", "week_index", "route_order", "visit_frequency"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    for col in ["route_km_total", "route_km"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    if "day_index" in out.columns and ("week_index" not in out.columns or out["week_index"].isna().any()):
        cal = calendar_df[["day_index", "week_index", "weekday"]].copy()
        out = out.drop(columns=[c for c in ["week_index", "weekday"] if c in out.columns], errors="ignore")
        out = out.merge(cal, on="day_index", how="left")
    return out


def _issue(
    check: str,
    severity: str,
    message: str,
    sales_rep: Any = None,
    client_id: Any = None,
    day_index: Any = None,
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    row = {
        "check": check,
        "severity": severity,
        "sales_rep": None if pd.isna(sales_rep) else str(sales_rep) if sales_rep is not None else None,
        "client_id": None if pd.isna(client_id) else str(client_id) if client_id is not None else None,
        "day_index": None if pd.isna(day_index) else int(day_index) if day_index is not None else None,
        "message": message,
    }
    if details:
        row.update(details)
    return row


def _status_from_issues(issues: list[dict[str, Any]]) -> str:
    if any(item["severity"] == "ERROR" for item in issues):
        return "FAIL"
    if any(item["severity"] == "WARNING" for item in issues):
        return "WARNING"
    return "PASS"


def _json_value(value: Any) -> Any:
    if pd.isna(value):
        return None
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            return value
    return value


def validate_input_clients_for_audit(
    clients_df: pd.DataFrame,
    config: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Validate input client data, including Sofia-region coordinate sanity."""
    cfg = _deep_merge(DEFAULT_AUDIT_CONFIG, config or {})
    clients = _coerce_clients(clients_df)
    issues: list[dict[str, Any]] = []
    missing = sorted(REQUIRED_CLIENT_COLUMNS - set(clients.columns))
    for col in missing:
        issues.append(_issue("input_validation", "ERROR", f"Missing required column: {col}", details={"field": col}))
    if missing:
        return issues

    bounds = cfg["audit"]["sofia_bounds"]
    for row in clients.itertuples(index=False):
        client_id = getattr(row, "client_id")
        sales_rep = getattr(row, "sales_rep")
        if pd.isna(client_id) or str(client_id).strip() == "":
            issues.append(_issue("input_validation", "ERROR", "client_id is required.", sales_rep, client_id, details={"field": "client_id"}))
        if pd.isna(sales_rep) or str(sales_rep).strip() == "":
            issues.append(_issue("input_validation", "ERROR", "sales_rep is required.", sales_rep, client_id, details={"field": "sales_rep"}))
        lat = getattr(row, "lat")
        lon = getattr(row, "lon")
        if pd.isna(lat):
            issues.append(_issue("input_validation", "ERROR", "lat is required and must be numeric.", sales_rep, client_id, details={"field": "lat"}))
        if pd.isna(lon):
            issues.append(_issue("input_validation", "ERROR", "lon is required and must be numeric.", sales_rep, client_id, details={"field": "lon"}))
        if not pd.isna(lat) and not (-90 <= float(lat) <= 90):
            issues.append(_issue("input_validation", "ERROR", "lat is outside the valid world range.", sales_rep, client_id, details={"field": "lat"}))
        if not pd.isna(lon) and not (-180 <= float(lon) <= 180):
            issues.append(_issue("input_validation", "ERROR", "lon is outside the valid world range.", sales_rep, client_id, details={"field": "lon"}))
        if not pd.isna(lat) and not pd.isna(lon):
            inside_sofia_region = (
                bounds["min_lat"] <= float(lat) <= bounds["max_lat"]
                and bounds["min_lon"] <= float(lon) <= bounds["max_lon"]
            )
            if not inside_sofia_region:
                issues.append(
                    _issue(
                        "input_validation",
                        "ERROR",
                        "Coordinate is outside the configured Sofia/Sofia-region bounds.",
                        sales_rep,
                        client_id,
                        details={"field": "lat_lon"},
                    )
                )
        freq = getattr(row, "visit_frequency")
        if pd.isna(freq) or int(freq) not in {2, 4, 8}:
            issues.append(
                _issue(
                    "input_validation",
                    "ERROR",
                    "visit_frequency must be one of 2, 4, 8.",
                    sales_rep,
                    client_id,
                    details={"field": "visit_frequency"},
                )
            )

    duplicated = clients[clients["client_id"].notna() & clients["client_id"].duplicated(keep=False)]
    for row in duplicated.itertuples(index=False):
        issues.append(
            _issue(
                "input_validation",
                "ERROR",
                "client_id must be unique.",
                getattr(row, "sales_rep", None),
                getattr(row, "client_id", None),
                details={"field": "client_id"},
            )
        )

    valid_freq = pd.to_numeric(clients["visit_frequency"], errors="coerce").fillna(0)
    weeks = int(cfg["working_days"]["weeks"])
    days_per_week = len(cfg["working_days"]["weekdays"])
    target_clients = int(cfg["daily_route"]["target_clients"])
    max_clients = int(cfg["daily_route"]["max_clients"])
    route_days = weeks * days_per_week
    total_required = int(valid_freq.sum())
    total_target_capacity = int(clients["sales_rep"].nunique() * route_days * target_clients)
    total_hard_capacity = int(clients["sales_rep"].nunique() * route_days * max_clients)
    if total_required > total_hard_capacity:
        issues.append(
            _issue(
                "input_validation",
                "ERROR",
                f"Required visits {total_required} exceed hard capacity {total_hard_capacity}.",
                details={"field": "capacity"},
            )
        )
    elif total_required > total_target_capacity:
        issues.append(
            _issue(
                "input_validation",
                "WARNING",
                f"Required visits {total_required} exceed target capacity {total_target_capacity}.",
                details={"field": "capacity"},
            )
        )

    for sales_rep, rep_df in clients.groupby("sales_rep", dropna=True):
        required = int(pd.to_numeric(rep_df["visit_frequency"], errors="coerce").fillna(0).sum())
        hard_capacity = route_days * max_clients
        target_capacity = route_days * target_clients
        if required > hard_capacity:
            issues.append(
                _issue(
                    "input_validation",
                    "ERROR",
                    f"Required visits {required} exceed hard rep capacity {hard_capacity}.",
                    sales_rep=sales_rep,
                    details={"field": "capacity"},
                )
            )
        elif required > target_capacity:
            issues.append(
                _issue(
                    "input_validation",
                    "WARNING",
                    f"Required visits {required} exceed target rep capacity {target_capacity}.",
                    sales_rep=sales_rep,
                    details={"field": "capacity"},
                )
            )
    return issues


def audit_schedule(
    schedule_df: pd.DataFrame,
    clients_df: pd.DataFrame,
    calendar_df: pd.DataFrame | None = None,
    config: dict[str, Any] | None = None,
    summary_by_sales_rep_df: pd.DataFrame | None = None,
    selected_candidates_df: pd.DataFrame | None = None,
    candidate_coverage_df: pd.DataFrame | None = None,
) -> dict[str, Any]:
    """Audit a stop-level schedule against PVRP business rules."""
    cfg = _deep_merge(DEFAULT_AUDIT_CONFIG, config or {})
    calendar = _normalize_columns(calendar_df) if calendar_df is not None else _calendar(cfg)
    clients = _coerce_clients(clients_df)
    schedule = _coerce_schedule(schedule_df, calendar)
    summary_by_sales_rep = _normalize_columns(summary_by_sales_rep_df)
    selected_candidates = _normalize_columns(selected_candidates_df)
    candidate_coverage = _normalize_columns(candidate_coverage_df)
    issues = validate_input_clients_for_audit(clients, cfg)

    required_schedule_cols = {"client_id", "sales_rep", "day_index", "week_index", "route_km_total"}
    missing_schedule_cols = sorted(required_schedule_cols - set(schedule.columns))
    for col in missing_schedule_cols:
        issues.append(_issue("coverage", "ERROR", f"Final schedule missing required column: {col}", details={"field": col}))
    if schedule.empty:
        issues.append(_issue("coverage", "ERROR", "Final schedule is empty."))
    if missing_schedule_cols or schedule.empty:
        return _finalize_audit(issues, clients, schedule)

    schedule_client_ids = set(schedule["client_id"].dropna().astype(str))
    input_client_ids = set(clients["client_id"].dropna().astype(str))
    for client_id in sorted(schedule_client_ids - input_client_ids):
        issues.append(_issue("coverage", "ERROR", "Scheduled client is not present in input clients.", client_id=client_id))

    duplicate_days = schedule[schedule.duplicated(["client_id", "day_index"], keep=False)]
    for row in duplicate_days[["client_id", "day_index", "sales_rep"]].drop_duplicates().itertuples(index=False):
        issues.append(
            _issue(
                "duplicate_same_day",
                "ERROR",
                "Client is visited more than once on the same day.",
                sales_rep=row.sales_rep,
                client_id=row.client_id,
                day_index=row.day_index,
            )
        )

    client_lookup = clients.drop_duplicates("client_id").set_index("client_id").to_dict("index")
    valid_clients = clients[clients["visit_frequency"].isin([2, 4, 8])].copy()
    for client in valid_clients.itertuples(index=False):
        cid = str(client.client_id)
        visits = schedule[schedule["client_id"].astype(str).eq(cid)]
        freq = int(client.visit_frequency)
        expected_total = freq
        if len(visits) != expected_total:
            issues.append(
                _issue(
                    "frequency_correctness",
                    "ERROR",
                    f"Expected {expected_total} monthly visits, got {len(visits)}.",
                    sales_rep=client.sales_rep,
                    client_id=cid,
                    details={"expected": expected_total, "actual": int(len(visits))},
                )
            )
            if len(visits) == 0:
                issues.append(_issue("coverage", "ERROR", "Client is missing completely from final schedule.", client.sales_rep, cid))

        if freq in {4, 8}:
            expected_weekly = 1 if freq == 4 else 2
            weekly = visits.groupby("week_index").size().reindex(range(1, int(cfg["working_days"]["weeks"]) + 1), fill_value=0)
            for week, count in weekly.items():
                if int(count) != expected_weekly:
                    issues.append(
                        _issue(
                            "frequency_correctness",
                            "ERROR",
                            f"Expected {expected_weekly} visits in week {int(week)}, got {int(count)}.",
                            sales_rep=client.sales_rep,
                            client_id=cid,
                            details={"week_index": int(week), "expected": expected_weekly, "actual": int(count)},
                        )
                    )

        if not visits.empty and not visits["sales_rep"].astype(str).eq(str(client.sales_rep)).all():
            wrong_reps = sorted(set(visits.loc[~visits["sales_rep"].astype(str).eq(str(client.sales_rep)), "sales_rep"].astype(str)))
            issues.append(
                _issue(
                    "sales_rep_consistency",
                    "ERROR",
                    f"Client was visited by wrong sales_rep(s): {', '.join(wrong_reps)}.",
                    sales_rep=client.sales_rep,
                    client_id=cid,
                )
            )

    if not selected_candidates.empty and {"sales_rep", "day_index"}.issubset(selected_candidates.columns):
        route_counts = selected_candidates.groupby(["sales_rep", "day_index"]).size()
        for (sales_rep, day_index), count in route_counts.items():
            if int(count) > 1:
                issues.append(
                    _issue(
                        "one_route_per_rep_day",
                        "ERROR",
                        f"Sales rep has {int(count)} selected routes on the same day.",
                        sales_rep=sales_rep,
                        day_index=day_index,
                    )
                )
    elif "selected_candidate_id" in schedule.columns:
        route_counts = schedule.groupby(["sales_rep", "day_index"])["selected_candidate_id"].nunique(dropna=True)
        for (sales_rep, day_index), count in route_counts.items():
            if int(count) > 1:
                issues.append(
                    _issue(
                        "one_route_per_rep_day",
                        "ERROR",
                        f"Sales rep has {int(count)} route ids on the same day.",
                        sales_rep=sales_rep,
                        day_index=day_index,
                    )
                )

    route_sizes = schedule.groupby(["sales_rep", "day_index"]).size().rename("number_of_clients")
    min_clients = int(cfg["daily_route"]["min_clients"])
    max_clients = int(cfg["daily_route"]["max_clients"])
    allow_overfilled = bool(cfg["daily_route"].get("allow_overfilled", False))
    for (sales_rep, day_index), count in route_sizes.items():
        count = int(count)
        if count > max_clients and not allow_overfilled:
            issues.append(
                _issue(
                    "daily_route_size",
                    "ERROR",
                    f"Route has {count} clients over max {max_clients}.",
                    sales_rep=sales_rep,
                    day_index=day_index,
                    details={"actual": count, "max_clients": max_clients},
                )
            )
        elif count < min_clients:
            issues.append(
                _issue(
                    "daily_route_size",
                    "WARNING",
                    f"Route has {count} clients below normal minimum {min_clients}.",
                    sales_rep=sales_rep,
                    day_index=day_index,
                    details={"actual": count, "min_clients": min_clients},
                )
            )

    route_km = schedule.groupby(["sales_rep", "day_index"])["route_km_total"].agg(["first", "nunique", "size"])
    for (sales_rep, day_index), row in route_km.iterrows():
        value = row["first"]
        if pd.isna(value):
            issues.append(_issue("route_km", "ERROR", "route_km_total is missing.", sales_rep=sales_rep, day_index=day_index))
        elif float(value) < 0:
            issues.append(_issue("route_km", "ERROR", "route_km_total is negative.", sales_rep=sales_rep, day_index=day_index))
        if int(row["nunique"]) > 1:
            issues.append(_issue("route_km", "ERROR", "route_km_total is inconsistent within a route.", sales_rep=sales_rep, day_index=day_index))

    route_rows = route_km.reset_index().rename(columns={"first": "route_km_total"})
    multiplier = float(cfg["audit"]["route_km_median_multiplier_warning"])
    for sales_rep, rep_routes in route_rows.groupby("sales_rep"):
        non_null = rep_routes["route_km_total"].dropna()
        median = float(non_null.median()) if not non_null.empty else 0.0
        if median <= 0 or len(non_null) < 3:
            continue
        for route in rep_routes.itertuples(index=False):
            if pd.notna(route.route_km_total) and float(route.route_km_total) > multiplier * median:
                issues.append(
                    _issue(
                        "route_density",
                        "WARNING",
                        f"Route km {float(route.route_km_total):.1f} is over {multiplier:.1f}x sales_rep median {median:.1f}.",
                        sales_rep=sales_rep,
                        day_index=route.day_index,
                        details={"route_km_total": float(route.route_km_total), "median_route_km": median},
                    )
                )

    cluster_col = "cluster_id" if "cluster_id" in schedule.columns else "cluster_manual" if "cluster_manual" in schedule.columns else None
    if cluster_col:
        max_clusters = int(cfg["audit"]["max_clusters_per_route_warning"])
        cluster_counts = schedule.groupby(["sales_rep", "day_index"])[cluster_col].nunique(dropna=True)
        for (sales_rep, day_index), count in cluster_counts.items():
            if int(count) > max_clusters:
                issues.append(
                    _issue(
                        "route_density",
                        "WARNING",
                        f"Route contains {int(count)} clusters/zones, above warning threshold {max_clusters}.",
                        sales_rep=sales_rep,
                        day_index=day_index,
                        details={"cluster_count": int(count)},
                    )
                )

    required_by_rep = clients.groupby("sales_rep")["visit_frequency"].sum(min_count=1).fillna(0).astype(int)
    planned_by_rep = schedule.groupby("sales_rep").size().astype(int)
    for sales_rep in sorted(set(required_by_rep.index.astype(str)) | set(planned_by_rep.index.astype(str))):
        required = int(required_by_rep.get(sales_rep, 0))
        planned = int(planned_by_rep.get(sales_rep, 0))
        if required != planned:
            issues.append(
                _issue(
                    "summary_consistency",
                    "ERROR",
                    f"required_monthly_visits {required} != planned_monthly_visits {planned}.",
                    sales_rep=sales_rep,
                    details={"required_monthly_visits": required, "planned_monthly_visits": planned},
                )
            )

    if not summary_by_sales_rep.empty and {"sales_rep", "required_monthly_visits", "planned_monthly_visits"}.issubset(summary_by_sales_rep.columns):
        for row in summary_by_sales_rep.itertuples(index=False):
            required = int(getattr(row, "required_monthly_visits") or 0)
            planned = int(getattr(row, "planned_monthly_visits") or 0)
            if required != planned:
                issues.append(
                    _issue(
                        "summary_consistency",
                        "ERROR",
                        "Exported Summary_By_Sales_Rep required/planned totals do not match.",
                        sales_rep=getattr(row, "sales_rep"),
                        details={"required_monthly_visits": required, "planned_monthly_visits": planned},
                    )
                )

    if not candidate_coverage.empty and "severity" in candidate_coverage.columns:
        coverage_errors = candidate_coverage[candidate_coverage["severity"].astype(str).str.upper().eq("ERROR")]
        coverage_warnings = candidate_coverage[candidate_coverage["severity"].astype(str).str.upper().eq("WARNING")]
        for row in coverage_errors.itertuples(index=False):
            issues.append(
                _issue(
                    "candidate_coverage",
                    "ERROR",
                    "Candidate coverage reports zero coverage for a client.",
                    sales_rep=getattr(row, "sales_rep", None),
                    client_id=getattr(row, "client_id", None),
                )
            )
        for row in coverage_warnings.itertuples(index=False):
            issues.append(
                _issue(
                    "candidate_coverage",
                    "WARNING",
                    "Candidate coverage is below visit frequency for a client.",
                    sales_rep=getattr(row, "sales_rep", None),
                    client_id=getattr(row, "client_id", None),
                )
            )

    return _finalize_audit(issues, clients, schedule)


def _finalize_audit(issues: list[dict[str, Any]], clients: pd.DataFrame, schedule: pd.DataFrame) -> dict[str, Any]:
    errors = [item for item in issues if item["severity"] == "ERROR"]
    warnings = [item for item in issues if item["severity"] == "WARNING"]
    check_status = {}
    for check in CHECKS:
        check_issues = [item for item in issues if item["check"] == check]
        check_status[check] = {
            "status": _status_from_issues(check_issues),
            "errors": sum(1 for item in check_issues if item["severity"] == "ERROR"),
            "warnings": sum(1 for item in check_issues if item["severity"] == "WARNING"),
        }

    route_groups = schedule.groupby(["sales_rep", "day_index"]) if {"sales_rep", "day_index"}.issubset(schedule.columns) else []
    route_sizes = route_groups.size() if hasattr(route_groups, "size") else pd.Series(dtype=int)
    route_km_total = 0.0
    if "route_km_total" in schedule.columns and not schedule.empty:
        route_km_total = float(schedule.drop_duplicates(["sales_rep", "day_index"])["route_km_total"].fillna(0).sum())
    summary = {
        "total_clients": int(clients["client_id"].nunique()) if "client_id" in clients.columns else 0,
        "required_monthly_visits": int(pd.to_numeric(clients.get("visit_frequency", pd.Series(dtype=float)), errors="coerce").fillna(0).sum()),
        "planned_monthly_visits": int(len(schedule)),
        "number_of_routes": int(len(route_sizes)),
        "total_route_km": route_km_total,
        "avg_clients_per_route": float(route_sizes.mean()) if len(route_sizes) else None,
        "min_clients_per_route": int(route_sizes.min()) if len(route_sizes) else None,
        "max_clients_per_route": int(route_sizes.max()) if len(route_sizes) else None,
    }
    return {
        "status": "FAIL" if errors else "WARNING" if warnings else "PASS",
        "passed": not errors,
        "errors": [_clean_issue(item) for item in errors],
        "warnings": [_clean_issue(item) for item in warnings],
        "checks": check_status,
        "summary": {key: _json_value(value) for key, value in summary.items()},
    }


def _clean_issue(issue: dict[str, Any]) -> dict[str, Any]:
    return {key: _json_value(value) for key, value in issue.items()}


def audit_final_schedule(
    final_schedule_path: str,
    input_clients_path: str,
    config_path: str | None = None,
) -> dict[str, Any]:
    """Read final_schedule.xlsx plus input clients and return audit results."""
    schedule_path = Path(final_schedule_path)
    input_path = Path(input_clients_path)
    if not schedule_path.exists():
        raise FileNotFoundError(f"Final schedule workbook not found: {schedule_path}")
    if not input_path.exists():
        raise FileNotFoundError(f"Input clients workbook not found: {input_path}")

    cfg = load_audit_config(config_path)
    xls = pd.ExcelFile(schedule_path)
    schedule = pd.read_excel(xls, sheet_name="Final_Schedule")
    summary = pd.read_excel(xls, sheet_name="Summary_By_Sales_Rep") if "Summary_By_Sales_Rep" in xls.sheet_names else None
    selected = pd.read_excel(xls, sheet_name="Candidate_Routes_Selected") if "Candidate_Routes_Selected" in xls.sheet_names else None
    coverage = pd.read_excel(xls, sheet_name="Candidate_Coverage") if "Candidate_Coverage" in xls.sheet_names else None
    clients = pd.read_excel(input_path)
    return audit_schedule(
        schedule_df=schedule,
        clients_df=clients,
        calendar_df=_calendar(cfg),
        config=cfg,
        summary_by_sales_rep_df=summary,
        selected_candidates_df=selected,
        candidate_coverage_df=coverage,
    )
