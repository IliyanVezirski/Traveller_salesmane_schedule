"""Run pipeline plus independent logic audit for a synthetic/client workbook."""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.pipeline import run_pipeline  # noqa: E402
from src.result_audit import audit_final_schedule, validate_input_clients_for_audit  # noqa: E402


def _load_config(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as fh:
        return yaml.safe_load(fh) or {}


def _logic_validation_config(config: dict[str, Any], args: argparse.Namespace) -> dict[str, Any]:
    updated = dict(config)
    updated.setdefault("osrm", {})
    updated.setdefault("candidate_routes", {})
    updated.setdefault("optimization", {})
    updated.setdefault("daily_route", {})
    updated["osrm"].update({"use_osrm": False, "fallback_to_haversine": True, "use_cache": False})
    updated["candidate_routes"]["cache"] = False
    updated["optimization"]["log_search_progress"] = False
    if args.time_limit is not None:
        updated["optimization"]["time_limit_seconds"] = args.time_limit
    if args.candidates_per_rep is not None:
        updated["candidate_routes"]["candidates_per_rep"] = args.candidates_per_rep
        updated["candidate_routes"]["keep_top_n_per_rep"] = args.candidates_per_rep
    for attr, key in [
        ("target_clients", "target_clients"),
        ("min_clients", "min_clients"),
        ("max_clients", "max_clients"),
    ]:
        value = getattr(args, attr)
        if value is not None:
            updated["daily_route"][key] = value
    return updated


def _apply_small_input_route_scaling(config: dict[str, Any], clients: pd.DataFrame, args: argparse.Namespace) -> None:
    """Keep tiny smoke datasets feasible without changing production defaults."""
    if not args.auto_scale_small_routes:
        return
    if any(getattr(args, attr) is not None for attr in ["target_clients", "min_clients", "max_clients"]):
        return
    if len(clients) > 150:
        return
    config.setdefault("daily_route", {})
    config["daily_route"].update(
        {
            "target_clients": 10,
            "min_clients": 1,
            "max_clients": 12,
            "allow_underfilled": True,
            "allow_overfilled": False,
        }
    )


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def _write_markdown(path: Path, payload: dict[str, Any]) -> None:
    audit = payload.get("audit") or {}
    pipeline = payload.get("pipeline") or {}
    input_errors = payload.get("input_validation_errors", [])
    input_warnings = payload.get("input_validation_warnings", [])
    lines = [
        "# Logic Validation Run",
        "",
        f"Input: `{payload['input_path']}`",
        f"Overall status: **{payload['overall_status']}**",
        f"Runtime seconds: `{payload['runtime_seconds']:.2f}`",
        "",
        "## Pipeline",
        "",
        f"- Status: `{pipeline.get('status')}`",
        f"- Message: {pipeline.get('message', '')}",
        f"- Excel: `{pipeline.get('excel_path')}`",
        f"- Total route km: `{pipeline.get('total_route_km')}`",
        "",
        "## Input Validation",
        "",
        f"- Errors: `{len(input_errors)}`",
        f"- Warnings: `{len(input_warnings)}`",
        "",
        "## Result Audit",
        "",
        f"- Status: `{audit.get('status')}`",
        f"- Passed: `{audit.get('passed')}`",
        f"- Errors: `{len(audit.get('errors', []))}`",
        f"- Warnings: `{len(audit.get('warnings', []))}`",
        "",
        "## Summary",
        "",
    ]
    for key, value in (audit.get("summary") or {}).items():
        lines.append(f"- {key}: `{value}`")
    if audit.get("errors"):
        lines.extend(["", "## Errors", ""])
        for issue in audit["errors"][:50]:
            lines.append(f"- `{issue['check']}` {issue.get('sales_rep') or ''} {issue.get('client_id') or ''}: {issue['message']}")
    if audit.get("warnings"):
        lines.extend(["", "## Warnings", ""])
        for issue in audit["warnings"][:50]:
            lines.append(f"- `{issue['check']}` {issue.get('sales_rep') or ''} {issue.get('client_id') or ''}: {issue['message']}")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run route-first pipeline and independent result audit.")
    parser.add_argument("--input", required=True, help="Input clients .xlsx/.csv.")
    parser.add_argument("--config", default=str(ROOT / "config.yaml"), help="Config YAML.")
    parser.add_argument("--output-dir", default=str(ROOT / "output" / "logic_validation"), help="Logic validation output directory.")
    parser.add_argument("--audit-only-final-schedule", help="Audit an existing final_schedule.xlsx instead of running the pipeline.")
    parser.add_argument("--time-limit", type=int, default=60, help="Solver time limit override for validation runs.")
    parser.add_argument("--candidates-per-rep", type=int, default=500, help="Candidate count override for validation runs.")
    parser.add_argument("--target-clients", type=int, help="Override daily_route.target_clients.")
    parser.add_argument("--min-clients", type=int, help="Override daily_route.min_clients.")
    parser.add_argument("--max-clients", type=int, help="Override daily_route.max_clients.")
    parser.add_argument("--auto-scale-small-routes", action="store_true", help="Scale route sizes for tiny experimental inputs.")
    parser.add_argument("--allow-invalid-input", action="store_true", help="Run pipeline even when audit input validation finds errors.")
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
    run_output_dir = output_dir / input_path.stem
    report_json = output_dir / f"{input_path.stem}_logic_validation_result.json"
    report_md = output_dir / f"{input_path.stem}_logic_validation_result.md"

    started = time.perf_counter()
    config = _logic_validation_config(_load_config(config_path), args)
    clients = pd.read_excel(input_path) if input_path.suffix.lower() in {".xlsx", ".xlsm", ".xls"} else pd.read_csv(input_path)
    _apply_small_input_route_scaling(config, clients, args)
    run_output_dir.mkdir(parents=True, exist_ok=True)
    runtime_config_path = run_output_dir / "logic_validation_runtime_config.yaml"
    runtime_config_path.write_text(yaml.safe_dump(config, sort_keys=False, allow_unicode=True), encoding="utf-8")

    input_issues = validate_input_clients_for_audit(clients, config)
    input_errors = [issue for issue in input_issues if issue["severity"] == "ERROR"]
    input_warnings = [issue for issue in input_issues if issue["severity"] == "WARNING"]

    pipeline_result: dict[str, Any] = {
        "status": "skipped",
        "message": "Skipped because input validation failed.",
        "excel_path": None,
    }
    audit_result: dict[str, Any] | None = None

    if args.audit_only_final_schedule:
        final_schedule = Path(args.audit_only_final_schedule).expanduser()
        if not final_schedule.is_absolute():
            final_schedule = (ROOT / final_schedule).resolve()
        audit_result = audit_final_schedule(str(final_schedule), str(input_path), str(runtime_config_path))
        pipeline_result = {"status": "not_run", "message": "Audit-only mode.", "excel_path": str(final_schedule)}
    elif not input_errors or args.allow_invalid_input:
        pipeline_result = run_pipeline(
            input_path=str(input_path),
            config=config,
            output_dir=str(run_output_dir),
            progress_callback=lambda percent, message: print(f"[{percent:3d}%] {message}"),
            log_callback=print,
        )
        if pipeline_result.get("status") == "success" and pipeline_result.get("excel_path"):
            audit_result = audit_final_schedule(str(pipeline_result["excel_path"]), str(input_path), str(runtime_config_path))

    if input_errors:
        overall_status = "FAIL"
    elif audit_result:
        overall_status = "PASS" if audit_result["status"] == "PASS" else "PARTIAL" if audit_result["passed"] else "FAIL"
    elif pipeline_result.get("status") == "infeasible":
        overall_status = "FAIL"
    elif pipeline_result.get("status") == "success":
        overall_status = "PARTIAL"
    else:
        overall_status = "FAIL"

    payload = {
        "input_path": str(input_path),
        "config_path": str(runtime_config_path),
        "overall_status": overall_status,
        "runtime_seconds": time.perf_counter() - started,
        "input_validation_errors": input_errors,
        "input_validation_warnings": input_warnings,
        "pipeline": pipeline_result,
        "audit": audit_result,
    }
    _write_json(report_json, payload)
    _write_markdown(report_md, payload)
    print(f"Overall status: {overall_status}")
    print(f"JSON report: {report_json}")
    print(f"Markdown report: {report_md}")
    return 0 if overall_status in {"PASS", "PARTIAL"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
