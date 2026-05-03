"""Measure route-first PVRP pipeline stages on the 1800-client Sofia dataset."""

from __future__ import annotations

import argparse
import json
import sys
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

import pandas as pd
import yaml

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.generate_synthetic_clients import generate_synthetic_clients  # noqa: E402
from src.app_paths import get_cache_dir  # noqa: E402
from src.calendar_builder import build_calendar  # noqa: E402
from src.candidate_routes import generate_candidate_routes_for_rep  # noqa: E402
from src.clustering import cluster_clients  # noqa: E402
from src.data_loader import load_clients  # noqa: E402
from src.export_excel import export_schedule_excel  # noqa: E402
from src.final_routing import optimize_selected_daily_routes  # noqa: E402
from src.local_search import improve_solution  # noqa: E402
from src.osrm_matrix import build_distance_matrix_for_rep  # noqa: E402
from src.pvrp_master_solver import solve_pvrp_master  # noqa: E402
from src.result_audit import audit_final_schedule  # noqa: E402
from src.scoring import validate_solution  # noqa: E402
from src.validation import validate_clients  # noqa: E402


@contextmanager
def _timer(timings: dict[str, float], name: str) -> Iterator[None]:
    started = time.perf_counter()
    try:
        yield
    finally:
        timings[name] = time.perf_counter() - started


def _load_config(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as fh:
        return yaml.safe_load(fh) or {}


def _performance_config(config: dict[str, Any], args: argparse.Namespace) -> dict[str, Any]:
    updated = dict(config)
    updated.setdefault("osrm", {})
    updated.setdefault("candidate_routes", {})
    updated.setdefault("optimization", {})
    updated["osrm"].update({"use_osrm": False, "fallback_to_haversine": True, "use_cache": False})
    updated["candidate_routes"]["cache"] = False
    updated["optimization"]["log_search_progress"] = False
    updated["optimization"]["time_limit_seconds"] = args.time_limit
    if args.candidates_per_rep is not None:
        updated["candidate_routes"]["candidates_per_rep"] = args.candidates_per_rep
        updated["candidate_routes"]["keep_top_n_per_rep"] = args.candidates_per_rep
    return updated


def _write_reports(output_dir: Path, payload: dict[str, Any]) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / "performance_1800_report.json"
    md_path = output_dir / "performance_1800_report.md"
    json_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")

    lines = [
        "# Performance 1800 Report",
        "",
        f"Status: **{payload['status']}**",
        f"Input: `{payload['input_path']}`",
        f"Output Excel: `{payload.get('excel_path')}`",
        "",
        "## Stage Timings",
        "",
    ]
    for name, seconds in payload["timings_seconds"].items():
        lines.append(f"- {name}: `{seconds:.2f}s`")
    lines.extend(
        [
            "",
            "## Solver",
            "",
            f"- Solver status: `{payload.get('solver_status')}`",
            f"- Routes: `{payload.get('routes')}`",
            f"- Planned visits: `{payload.get('planned_visits')}`",
            f"- Total route km: `{payload.get('total_route_km')}`",
            "",
            "## Candidate Coverage",
            "",
        ]
    )
    for key, value in payload.get("candidate_coverage_summary", {}).items():
        lines.append(f"- {key}: `{value}`")
    if payload.get("audit"):
        lines.extend(
            [
                "",
                "## Result Audit",
                "",
                f"- Audit status: `{payload['audit'].get('status')}`",
                f"- Audit errors: `{len(payload['audit'].get('errors', []))}`",
                f"- Audit warnings: `{len(payload['audit'].get('warnings', []))}`",
            ]
        )
    if payload.get("error"):
        lines.extend(["", "## Error", "", payload["error"]])
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run 1800-client performance test with haversine distances.")
    parser.add_argument("--input", default=str(ROOT / "data" / "synthetic_1800_sofia.xlsx"), help="Input 1800-client workbook.")
    parser.add_argument("--config", default=str(ROOT / "config.yaml"), help="Config YAML.")
    parser.add_argument("--output-dir", default=str(ROOT / "output" / "logic_validation"), help="Report/output directory.")
    parser.add_argument("--time-limit", type=int, default=600, help="Master solver time limit in seconds.")
    parser.add_argument("--candidates-per-rep", type=int, default=None, help="Optional candidate count override.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    input_path = Path(args.input).expanduser()
    if not input_path.is_absolute():
        input_path = (ROOT / input_path).resolve()
    config_path = Path(args.config).expanduser()
    if not config_path.is_absolute():
        config_path = (ROOT / config_path).resolve()
    output_dir = Path(args.output_dir).expanduser()
    if not output_dir.is_absolute():
        output_dir = (ROOT / output_dir).resolve()
    run_dir = output_dir / "performance_1800_run"
    excel_path = run_dir / "final_schedule.xlsx"

    if not input_path.exists():
        generate_synthetic_clients(18, 1800, "full_1800", str(input_path), random_seed=42)

    config = _performance_config(_load_config(config_path), args)
    timings: dict[str, float] = {}
    payload: dict[str, Any] = {
        "status": "failed",
        "input_path": str(input_path),
        "timings_seconds": timings,
        "excel_path": None,
    }

    try:
        with _timer(timings, "validation"):
            clients_raw = load_clients(str(input_path))
            clients_df, input_validation = validate_clients(clients_raw, config)
            errors = int(input_validation["severity"].eq("ERROR").sum()) if not input_validation.empty else 0
            if errors:
                raise RuntimeError(f"Input validation failed with {errors} errors.")
            calendar_df = build_calendar(config)

        matrix_data_by_rep: dict[str, dict[str, Any]] = {}
        clustered_reps: list[pd.DataFrame] = []
        candidates: list[pd.DataFrame] = []
        coverage: list[pd.DataFrame] = []

        with _timer(timings, "matrix_building"):
            for sales_rep, rep_df in clients_df.groupby("sales_rep"):
                matrix_data_by_rep[str(sales_rep)] = build_distance_matrix_for_rep(
                    rep_df,
                    config,
                    str(get_cache_dir() / "osrm_matrices"),
                )

        with _timer(timings, "candidate_generation"):
            for sales_rep, rep_df in clients_df.groupby("sales_rep"):
                matrix_data = matrix_data_by_rep[str(sales_rep)]
                clustered = cluster_clients(rep_df, matrix_data["distance_matrix_m"], config)
                clustered_reps.append(clustered)
                rep_candidates, rep_coverage = generate_candidate_routes_for_rep(clustered, matrix_data, config)
                candidates.append(rep_candidates)
                coverage.append(rep_coverage)
            clients_clustered = pd.concat(clustered_reps, ignore_index=True)
            candidates_df = pd.concat(candidates, ignore_index=True)
            coverage_df = pd.concat(coverage, ignore_index=True)

        with _timer(timings, "master_solving"):
            solver_result = solve_pvrp_master(clients_clustered, calendar_df, candidates_df, config)
        if solver_result["selected_candidates"].empty:
            payload.update(
                {
                    "status": "infeasible",
                    "solver_status": solver_result["status"],
                    "solver_warnings": solver_result.get("warnings", []),
                }
            )
            _write_reports(output_dir, payload)
            print(f"Performance status: {payload['status']}")
            return 2

        with _timer(timings, "final_routing"):
            selected_candidates = improve_solution(
                solver_result["selected_candidates"],
                candidates_df,
                clients_clustered,
                calendar_df,
                config,
            )
            final_routes = optimize_selected_daily_routes(selected_candidates, clients_clustered, matrix_data_by_rep, config)
            solution_validation = validate_solution(final_routes, clients_clustered, selected_candidates, calendar_df, config)
            validation_df = pd.concat([input_validation, solution_validation], ignore_index=True)

        with _timer(timings, "export"):
            export_schedule_excel(str(excel_path), final_routes, selected_candidates, coverage_df, validation_df, clients_clustered, config)

        audit = audit_final_schedule(str(excel_path), str(input_path), str(config_path))
        route_days = final_routes.drop_duplicates(["sales_rep", "day_index"])
        payload.update(
            {
                "status": "success" if audit["passed"] else "failed",
                "solver_status": solver_result["status"],
                "solver_wall_time": float(solver_result["solver_wall_time"]),
                "routes": int(route_days.shape[0]),
                "planned_visits": int(final_routes.shape[0]),
                "total_route_km": float(route_days["route_km_total"].sum()),
                "excel_path": str(excel_path),
                "candidate_coverage_summary": coverage_df["severity"].value_counts().to_dict(),
                "audit": audit,
            }
        )
    except Exception as exc:
        payload["error"] = repr(exc)

    _write_reports(output_dir, payload)
    print(f"Performance status: {payload['status']}")
    print(f"Report: {output_dir / 'performance_1800_report.md'}")
    return 0 if payload["status"] == "success" else 1


if __name__ == "__main__":
    raise SystemExit(main())
