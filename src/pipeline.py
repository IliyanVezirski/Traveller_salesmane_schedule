"""GUI-friendly wrapper around the route-first PVRP backend pipeline."""

from __future__ import annotations

from contextlib import contextmanager
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Iterator
import os
import traceback

import pandas as pd

from .app_paths import ensure_runtime_dirs, get_cache_dir, get_project_root, get_output_dir
from .calendar_builder import build_calendar
from .clustering import assign_global_weekday_territories, cluster_clients
from .data_loader import load_clients
from .day_pattern_solver import solve_day_pattern_master
from .export_excel import export_schedule_excel
from .final_routing import optimize_selected_daily_routes
from .logging_utils import setup_run_logger
from .local_search import improve_solution
from .map_visualization import generate_schedule_map
from .osrm_matrix import build_distance_matrix_for_rep
from .scoring import score_solution, validate_solution
from .selective_day_scheduler import solve_selective_day_schedule
from .validation import validate_clients
from .version import APP_BUILD, APP_NAME, APP_VERSION


ProgressCallback = Callable[[int, str], None]
LogCallback = Callable[[str], None]
CancelChecker = Callable[[], bool]


DEFAULT_CONFIG: dict[str, Any] = {
    "working_days": {
        "weeks": 4,
        "weekdays": ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"],
    },
    "weekday_consistency": {
        "frequency_2_same_weekday": True,
        "frequency_4_same_weekday": True,
        "frequency_8_same_weekday_pair": True,
    },
    "clustering": {
        "use_distance_matrix": True,
        "k_medoids_max_iterations": 30,
        "target_cluster_size": 4,
        "max_clusters_per_rep": 60,
    },
    "territory_days": {
        "enabled": True,
        "hard_client_weekday": False,
        "scope": "per_rep",
        "use_distance_matrix": True,
        "max_daily_territory_km": 75,
        "route_span_weight": 25,
        "route_span_over_limit_weight": 5000,
        "load_balance_weight": 250,
        "overload_weight": 1000000,
        "local_refinement_iterations": 25,
    },
    "global_geography": {
        "enabled": True,
        "global_cluster_count": 30,
    },
    "daily_route": {
        "target_clients": 20,
        "min_clients": 17,
        "max_clients": 22,
        "allow_underfilled": True,
        "allow_overfilled": False,
    },
    "candidate_routes": {
        "candidates_per_rep": 3000,
        "random_seed": 42,
        "generation_methods": [
            "periodic_seed",
            "cluster",
            "cluster_neighbors",
            "sweep",
            "nearest_neighbor_expansion",
            "randomized_compact",
        ],
        "remove_duplicates": True,
        "keep_top_n_per_rep": 3000,
        "min_candidates_per_client": 6,
        "cache": True,
        "max_route_km_median_multiplier": 2.8,
    },
    "selective_day_routing": {
        "enabled": True,
        "compactness_strength": 1.0,
        "pool_size": 45,
        "prize_base": 100000,
        "urgency_bonus": 5000,
        "territory_bonus": 2000,
        "territory_mismatch_penalty": 40000,
        "preferred_bonus": 1000,
        "distance_penalty": 2000,
        "freq4_weekday_balance_weight": 50000,
        "freq4_territory_mismatch_penalty": 2000,
        "freq4_weekday_overload_penalty": 1000000,
        "freq4_weekday_capacity_ratio": 0.85,
        "frequency2_ideal_gap_days": 10,
        "frequency2_close_gap_penalty": 1500,
        "frequency2_spacing_bonus": 200,
        "frequency2_weekday_balance_weight": 5000,
        "frequency2_weekday_overload_penalty": 1000000,
        "frequency2_phase_spacing_weight": 1000,
        "pyvrp_time_limit_seconds": 2,
        "pyvrp_max_iterations": 500,
    },
    "osrm": {
        "url": "http://localhost:5000",
        "use_osrm": True,
        "use_cache": True,
        "fallback_to_haversine": True,
        "request_timeout_seconds": 30,
    },
    "route_costing": {
        "method": "nearest_neighbor_2opt",
        "final_method": "pyvrp",
        "pyvrp_time_limit_seconds": 3,
        "pyvrp_max_iterations": 500,
        "route_type": "open",
        "use_duration_if_available": True,
    },
    "optimization": {
        "solver": "ortools_cpsat",
        "time_limit_seconds": 3600,
        "num_workers": 8,
        "decompose_by_sales_rep": True,
        "stop_after_first_solution": False,
        "log_search_progress": True,
    },
    "weights": {
        "route_km": 1000,
        "underfilled_route": 500,
        "over_target_clients": 300,
        "bad_spacing_frequency_8": 2000,
        "bad_spacing_frequency_2": 2000,
        "cluster_mixing": 300,
        "unused_day": 100,
        "fixed_weekday_violation": 5000,
        "preferred_weekday_violation": 500,
    },
    "output": {
        "excel_path": "output/final_schedule.xlsx",
        "map_path": "output/maps/final_schedule_map.html",
    },
}


RESULT_DEFAULTS: dict[str, Any] = {
    "status": "failed",
    "excel_path": None,
    "map_path": None,
    "summary_by_sales_rep": [],
    "summary_by_day": [],
    "validation": [],
    "total_route_km": None,
    "message": "",
}


class PipelineCancelled(RuntimeError):
    """Raised when a GUI caller requests cancellation between backend stages."""


@contextmanager
def _working_directory(path: Path) -> Iterator[None]:
    previous = Path.cwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(previous)


def _project_root() -> Path:
    return get_project_root()


def _emit_progress(callback: ProgressCallback | None, percent: int, message: str) -> None:
    if callback:
        callback(max(0, min(100, int(percent))), message)


def _emit_log(callback: LogCallback | None, message: str) -> None:
    if callback:
        callback(message)


def _check_cancel(cancel_checker: CancelChecker | None) -> None:
    if cancel_checker and cancel_checker():
        raise PipelineCancelled("Optimization was cancelled by the user.")


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = deepcopy(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = deepcopy(value)
    return merged


def _normalize_config(config: dict[str, Any] | None) -> dict[str, Any]:
    raw = deepcopy(config or {})
    if "working_days" not in raw and isinstance(raw.get("calendar"), dict):
        calendar = raw["calendar"]
        raw["working_days"] = {
            "weeks": calendar.get("weeks", calendar.get("num_weeks", 4)),
            "weekdays": calendar.get(
                "weekdays",
                calendar.get("working_weekdays", DEFAULT_CONFIG["working_days"]["weekdays"]),
            ),
        }
    normalized = _deep_merge(DEFAULT_CONFIG, raw)
    normalized.setdefault("application", {})
    normalized["application"].update({"name": APP_NAME, "version": APP_VERSION, "build": APP_BUILD})
    return normalized


def _clean_value(value: Any) -> Any:
    if pd.isna(value):
        return None
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            return value
    return value


def _records(value: Any) -> list[dict[str, Any]]:
    if value is None:
        return []
    if isinstance(value, pd.DataFrame):
        df = value.copy()
    elif isinstance(value, list):
        return [
            {str(k): _clean_value(v) for k, v in row.items()} if isinstance(row, dict) else {"value": _clean_value(row)}
            for row in value
        ]
    else:
        df = pd.DataFrame(value)
    if df.empty:
        return []
    df = df.astype(object).where(pd.notna(df), None)
    return [{str(k): _clean_value(v) for k, v in row.items()} for row in df.to_dict("records")]


def _contract_result(result: dict[str, Any]) -> dict[str, Any]:
    out = {**RESULT_DEFAULTS, **result}
    out["summary_by_sales_rep"] = _records(out.get("summary_by_sales_rep"))
    out["summary_by_day"] = _records(out.get("summary_by_day"))
    out["validation"] = _records(out.get("validation"))
    out["excel_path"] = str(out["excel_path"]) if out.get("excel_path") else None
    out["map_path"] = str(out["map_path"]) if out.get("map_path") else None
    out["total_route_km"] = float(out["total_route_km"]) if out.get("total_route_km") is not None else None
    out["message"] = str(out.get("message") or "")
    return out


def _output_paths(config: dict[str, Any], output_dir: str | None) -> tuple[Path, Path]:
    root = _project_root()
    configured_excel = Path(str(config["output"]["excel_path"]))
    configured_map = Path(str(config["output"]["map_path"]))

    if output_dir:
        requested_out_dir = Path(output_dir).expanduser()
        out_dir = requested_out_dir if requested_out_dir.is_absolute() else root / requested_out_dir
        out_dir = out_dir.resolve()
        excel_path = out_dir / configured_excel.name
        map_path = out_dir / "maps" / configured_map.name
    else:
        excel_path = configured_excel if configured_excel.is_absolute() else root / configured_excel
        map_path = configured_map if configured_map.is_absolute() else root / configured_map

    config.setdefault("output", {})
    config["output"]["excel_path"] = str(excel_path)
    config["output"]["map_path"] = str(map_path)
    return excel_path, map_path


def _build_summary_frames(
    final_routes: pd.DataFrame,
    selected_candidates: pd.DataFrame,
    validation_df: pd.DataFrame,
    clients_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if final_routes.empty:
        return pd.DataFrame(), pd.DataFrame()

    routes = final_routes.drop_duplicates(["sales_rep", "day_index"])
    summary_rep = (
        clients_df.groupby("sales_rep")
        .agg(total_clients=("client_id", "nunique"), required_monthly_visits=("visit_frequency", "sum"))
        .reset_index()
    )
    planned = final_routes.groupby("sales_rep").agg(planned_monthly_visits=("client_id", "count")).reset_index()
    route_km = routes.groupby("sales_rep").agg(total_route_km=("route_km_total", "sum"), route_days=("day_index", "count")).reset_index()
    clients_per_day = final_routes.groupby(["sales_rep", "day_index"]).size().reset_index(name="clients_day")
    day_stats = (
        clients_per_day.groupby("sales_rep")
        .agg(
            avg_clients_per_day=("clients_day", "mean"),
            min_clients_day=("clients_day", "min"),
            max_clients_day=("clients_day", "max"),
        )
        .reset_index()
    )
    summary_rep = summary_rep.merge(planned, on="sales_rep", how="left")
    summary_rep = summary_rep.merge(route_km, on="sales_rep", how="left")
    summary_rep = summary_rep.merge(day_stats, on="sales_rep", how="left")
    summary_rep["avg_route_km_per_day"] = summary_rep["total_route_km"] / summary_rep["route_days"].replace(0, pd.NA)

    if not validation_df.empty and "severity" in validation_df.columns:
        errors = (
            validation_df[validation_df["severity"].eq("ERROR")]
            .groupby("sales_rep", dropna=False)
            .size()
            .rename("validation_errors")
            .reset_index()
        )
    else:
        errors = pd.DataFrame(columns=["sales_rep", "validation_errors"])
    summary_rep = summary_rep.merge(errors, on="sales_rep", how="left").fillna({"validation_errors": 0})

    summary_cols = [
        "sales_rep",
        "total_clients",
        "required_monthly_visits",
        "planned_monthly_visits",
        "total_route_km",
        "avg_route_km_per_day",
        "avg_clients_per_day",
        "min_clients_day",
        "max_clients_day",
        "validation_errors",
    ]
    summary_rep = summary_rep[[c for c in summary_cols if c in summary_rep.columns]]

    day_cols = [
        "day_index",
        "week_index",
        "weekday",
        "sales_rep",
        "selected_candidate_id",
        "number_of_clients",
        "route_km",
        "main_cluster",
        "clusters_used",
    ]
    summary_day = selected_candidates[[c for c in day_cols if c in selected_candidates.columns]].copy()
    return summary_rep, summary_day


def run_pipeline(
    input_path: str,
    config: dict[str, Any],
    output_dir: str | None = "output",
    progress_callback: ProgressCallback | None = None,
    log_callback: LogCallback | None = None,
    cancel_checker: CancelChecker | None = None,
) -> dict[str, Any]:
    """Public GUI/CLI contract for running the route-first PVRP pipeline."""
    try:
        result = _run_pipeline_impl(
            input_path=input_path,
            config=config,
            output_dir=output_dir,
            progress_callback=progress_callback,
            log_callback=log_callback,
            cancel_checker=cancel_checker,
        )
        return _contract_result(result)
    except PipelineCancelled as exc:
        _emit_progress(progress_callback, 100, "Отказано")
        _emit_log(log_callback, str(exc))
        return _contract_result({"status": "cancelled", "message": str(exc)})
    except Exception as exc:
        try:
            fallback_log_dir = Path(output_dir).expanduser() if output_dir else get_output_dir()
            if not fallback_log_dir.is_absolute():
                fallback_log_dir = get_project_root() / fallback_log_dir
            setup_run_logger(str(fallback_log_dir)).exception("Pipeline failed before normal logger setup")
        except Exception:
            pass
        _emit_log(log_callback, f"ERROR: {exc}")
        _emit_log(log_callback, traceback.format_exc())
        return _contract_result({"status": "failed", "message": str(exc)})


def _run_pipeline_impl(
    input_path: str,
    config: dict[str, Any],
    output_dir: str | None,
    progress_callback: ProgressCallback | None = None,
    log_callback: LogCallback | None = None,
    cancel_checker: CancelChecker | None = None,
) -> dict[str, Any]:
    """Run the existing route-first PVRP pipeline and return GUI-ready results.

    This function deliberately delegates optimization work to the existing backend
    modules. It only adds progress reporting, output-path handling, and result
    packaging for desktop callers.
    """
    ensure_runtime_dirs()
    root = _project_root()
    runtime_config = _normalize_config(config)
    excel_path, map_path = _output_paths(runtime_config, output_dir)
    excel_path.parent.mkdir(parents=True, exist_ok=True)
    map_path.parent.mkdir(parents=True, exist_ok=True)

    ui_log_callback = log_callback
    file_logger = None
    try:
        file_logger = setup_run_logger(str(excel_path.parent))
    except Exception as exc:
        _emit_log(ui_log_callback, f"WARNING: File logging could not be initialized: {exc}")

    def combined_log(message: str) -> None:
        if file_logger:
            file_logger.info(message)
        if ui_log_callback:
            ui_log_callback(message)

    log_callback = combined_log
    _emit_log(log_callback, f"{APP_NAME} v{APP_VERSION} ({APP_BUILD})")

    with _working_directory(root):
        _check_cancel(cancel_checker)
        _emit_progress(progress_callback, 3, "Зареждане на входния файл")
        _emit_log(log_callback, f"Loading input: {input_path}")
        clients_raw = load_clients(input_path)

        _check_cancel(cancel_checker)
        _emit_progress(progress_callback, 8, "Валидиране на данните")
        clients_df, input_validation = validate_clients(clients_raw, runtime_config)
        error_count = int(input_validation["severity"].eq("ERROR").sum()) if not input_validation.empty else 0
        warning_count = int(input_validation["severity"].eq("WARNING").sum()) if not input_validation.empty else 0
        _emit_log(log_callback, f"Input validation: {error_count} errors, {warning_count} warnings")
        if error_count:
            return {
                "status": "failed",
                "message": "Input validation failed. Fix ERROR rows before solving.",
                "validation": input_validation,
                "excel_path": None,
                "map_path": None,
                "summary_by_sales_rep": pd.DataFrame(),
                "summary_by_day": pd.DataFrame(),
                "total_route_km": None,
                "planned_visits": 0,
            }

        _check_cancel(cancel_checker)
        _emit_progress(progress_callback, 13, "Създаване на календар")
        calendar_df = build_calendar(runtime_config)
        planning_clients_df = clients_df
        global_geography_enabled = bool(runtime_config.get("global_geography", {}).get("enabled", False))
        global_planning_enabled = str(runtime_config.get("territory_days", {}).get("scope", "per_rep")).lower() == "global"
        if global_geography_enabled or global_planning_enabled:
            planning_clients_df = assign_global_weekday_territories(clients_df, runtime_config)
            _emit_log(
                log_callback,
                f"Assigned global weekday territories across {planning_clients_df['client_id'].nunique()} clients.",
            )
        matrix_data_by_rep: dict[str, dict[str, Any]] = {}
        clustered_reps: list[pd.DataFrame] = []

        rep_groups = list(planning_clients_df.groupby("sales_rep"))
        total_reps = max(1, len(rep_groups))
        for rep_index, (sales_rep, rep_df) in enumerate(rep_groups, start=1):
            base = 15 + int((rep_index - 1) * 45 / total_reps)
            rep_label = str(sales_rep)

            _check_cancel(cancel_checker)
            _emit_progress(progress_callback, base, f"Изчисляване на матрици: {rep_label}")
            _emit_log(log_callback, f"[{rep_label}] Building distance matrix")
            matrix_data = build_distance_matrix_for_rep(rep_df, runtime_config, str(get_cache_dir() / "osrm_matrices"))
            if matrix_data.get("source") == "haversine" and bool(runtime_config["osrm"].get("use_osrm", True)):
                _emit_log(log_callback, f"WARNING: [{rep_label}] OSRM unavailable; haversine fallback was used.")
            matrix_data_by_rep[rep_label] = matrix_data

            _check_cancel(cancel_checker)
            _emit_progress(progress_callback, base + 6, f"Клъстериране на клиенти: {rep_label}")
            clustered = cluster_clients(rep_df, matrix_data["distance_matrix_m"], runtime_config, matrix_data.get("client_ids"))
            clustered_reps.append(clustered)

            _check_cancel(cancel_checker)
            _emit_progress(progress_callback, base + 12, f"Генериране на кандидат-маршрути: {rep_label}")
            _emit_log(log_callback, f"[{rep_label}] Prepared client-day pattern inputs")

        _check_cancel(cancel_checker)
        _emit_progress(progress_callback, 62, "Изчисляване на route costs")
        clients_clustered = pd.concat(clustered_reps, ignore_index=True)
        _emit_log(
            log_callback,
            f"Prepared client-day pattern model across {clients_clustered['sales_rep'].nunique()} sales reps.",
        )

        _check_cancel(cancel_checker)
        _emit_progress(progress_callback, 70, "Решаване на PVRP master модела")
        if bool(runtime_config.get("selective_day_routing", {}).get("enabled", True)):
            solver_result = solve_selective_day_schedule(clients_clustered, calendar_df, matrix_data_by_rep, runtime_config)
        else:
            solver_result = solve_day_pattern_master(clients_clustered, calendar_df, runtime_config)
        solver_status = str(solver_result["status"])
        _emit_log(log_callback, f"Solver status: {solver_status}; wall_time={solver_result['solver_wall_time']:.2f}s")
        if solver_result["selected_candidates"].empty:
            for warning in solver_result.get("warnings", []):
                _emit_log(log_callback, f"DIAGNOSTIC: {warning}")
            return {
                "status": "infeasible",
                "solver_status": solver_status,
                "message": "No feasible client-day schedule found.",
                "validation": input_validation,
                "excel_path": None,
                "map_path": None,
                "summary_by_sales_rep": pd.DataFrame(),
                "summary_by_day": pd.DataFrame(),
                "total_route_km": None,
                "planned_visits": 0,
                "warnings": solver_result.get("warnings", []),
            }
        coverage_df = solver_result.get("coverage", pd.DataFrame())

        _check_cancel(cancel_checker)
        _emit_progress(progress_callback, 82, "Финално подреждане на маршрути")
        selected_candidates = improve_solution(
            solver_result["selected_candidates"], pd.DataFrame(), clients_clustered, calendar_df, runtime_config
        )
        final_routes = optimize_selected_daily_routes(selected_candidates, clients_clustered, matrix_data_by_rep, runtime_config)

        _check_cancel(cancel_checker)
        _emit_progress(progress_callback, 88, "Валидация на решението")
        solution_validation = validate_solution(final_routes, clients_clustered, selected_candidates, calendar_df, runtime_config)
        validation_df = pd.concat([input_validation, solution_validation], ignore_index=True)

        _check_cancel(cancel_checker)
        _emit_progress(progress_callback, 92, "Експорт към Excel")
        export_schedule_excel(str(excel_path), final_routes, selected_candidates, coverage_df, validation_df, clients_clustered, runtime_config)

        _check_cancel(cancel_checker)
        _emit_progress(progress_callback, 96, "Генериране на HTML карта")
        generate_schedule_map(final_routes, str(map_path))

        score = score_solution(final_routes, solution_validation)
        summary_rep, summary_day = _build_summary_frames(final_routes, selected_candidates, validation_df, clients_clustered)
        _emit_log(log_callback, f"Output Excel: {excel_path}")
        _emit_log(log_callback, f"Output map: {map_path}")
        _emit_log(
            log_callback,
            f"Routes: {score['routes']} | total km: {score['total_route_km']:.1f} | validation errors: {score['validation_errors']}",
        )
        _emit_progress(progress_callback, 100, "Завършено успешно")
        return {
            "status": "success",
            "solver_status": solver_status,
            "message": "Optimization completed successfully.",
            "excel_path": str(excel_path),
            "map_path": str(map_path),
            "summary_by_sales_rep": summary_rep,
            "summary_by_day": summary_day,
            "validation": validation_df,
            "total_route_km": float(score["total_route_km"]),
            "planned_visits": int(final_routes.shape[0]),
            "routes": int(score["routes"]),
            "number_of_sales_reps": int(clients_clustered["sales_rep"].nunique()),
            "avg_clients_per_route": float(final_routes.groupby(["sales_rep", "day_index"]).size().mean()),
            "min_clients_per_route": int(final_routes.groupby(["sales_rep", "day_index"]).size().min()),
            "max_clients_per_route": int(final_routes.groupby(["sales_rep", "day_index"]).size().max()),
            "validation_errors": int(score["validation_errors"]),
            "solver_wall_time": float(solver_result["solver_wall_time"]),
        }
