# Integration Report

## Status

PASS

The latest Backend Optimization Agent change was validated end-to-end. The project still uses the route-first master model with `z[candidate_route, day]`; the new PyVRP final-routing path is integrated through the existing pipeline, exported to Excel, and covered by tests.

One independent audit run reports `PARTIAL` only because the smoke config intentionally allows tiny/underfilled routes for fast QA. Hard correctness checks pass: frequency rules, duplicate visits, sales rep consistency, one route per rep/day, route_km, coverage, and summary consistency.

## Commands run

```bash
python -c "from src.pipeline import run_pipeline; import inspect; print(inspect.signature(run_pipeline))"
python -c "from src.data_loader import load_clients; from src.validation import validate_clients; print('core imports ok')"
python -c "from src.final_routing import optimize_selected_daily_routes; print('final_routing import ok')"
python -c "import pandas, numpy, openpyxl, sklearn, ortools, requests, yaml, folium, tqdm; print('base deps ok')"
python -c "import pyvrp; print('pyvrp ok')"
python -c "import PySide6; import run_gui; from gui.main_window import MainWindow; print('gui imports ok')"
python -c "import yaml; data=yaml.safe_load(open('config.yaml', encoding='utf-8')); required={'working_days','daily_route','candidate_routes','osrm','route_costing','optimization','weights','output'}; print('missing', sorted(required-set(data)))"
python -c "import yaml; from src.data_loader import load_clients; from src.validation import validate_clients; from src.osrm_matrix import build_distance_matrix_for_rep; cfg=yaml.safe_load(open('config.yaml', encoding='utf-8')); cfg['osrm'].update({'use_osrm': True, 'fallback_to_haversine': True, 'use_cache': False, 'url': 'http://127.0.0.1:9', 'request_timeout_seconds': 1}); df,_=validate_clients(load_clients('data/sample_clients.xlsx'), cfg); _,rep=next(iter(df.groupby('sales_rep'))); m=build_distance_matrix_for_rep(rep, cfg, 'cache/osrm_matrices'); print(m['source'], m['distance_matrix_m'].shape)"
python main.py --help
python -m pytest tests
python scripts/smoke_test.py
python main.py --input data/sample_clients.xlsx --output output/final_cli_validation --no-osrm --no-cache --quiet-solver --time-limit 20 --num-workers 4 --target-clients 6 --min-clients 1 --max-clients 8 --candidates-per-rep 250 --keep-top-n-per-rep 250
python scripts/check_gui_import.py
python scripts/run_logic_validation.py --input data/sample_clients.xlsx --audit-only-final-schedule output/final_cli_validation/final_schedule.xlsx --output-dir output/final_logic_validation --target-clients 6 --min-clients 1 --max-clients 8
python -m compileall src gui scripts main.py run_gui.py
python scripts/release_smoke_test.py
```

## Verified results

- Imports: PASS
- Base dependencies: PASS
- GUI imports: PASS
- PyVRP import: PASS
- Config sections: PASS
- OSRM fallback: PASS, source returned `haversine`
- Pytest: PASS, `17 passed`
- Pipeline smoke: PASS, `status=success`
- CLI smoke: PASS, `status=success`
- Release smoke: PASS, `status=success`
- Compile check: PASS
- Independent result audit: PARTIAL with warnings only, no hard failures

## Backend change validation

- `route_costing.final_method: "pyvrp"` is now implemented for open final routes.
- `tests/test_final_routing_pyvrp.py` passes and confirms PyVRP is selected when configured.
- End-to-end Excel output from `output/final_cli_validation/final_schedule.xlsx` has `final_route_method = pyvrp` for all 136 scheduled stop rows.
- `route_km_total` is populated for every scheduled stop.
- The backend falls back to `nearest_neighbor_2opt` in code when PyVRP is unavailable or cannot solve a final route.

## Excel/output checks

Validated workbook:

```text
output/final_cli_validation/final_schedule.xlsx
```

Required sheets found:

- `Final_Schedule`
- `Daily_Routes`
- `Summary_By_Sales_Rep`
- `Summary_By_Day`
- `Validation`
- `Candidate_Routes_Selected`
- `Candidate_Coverage`
- `Parameters`

Schedule checks:

- frequency 2 clients have exactly 2 visits: PASS
- frequency 4 clients have exactly 1 visit per week: PASS
- frequency 8 clients have exactly 2 visits per week: PASS
- duplicate client-day visits: 0
- max routes per sales rep/day: 1
- missing route_km rows: 0
- `final_route_method` exported: PASS

## Fixed issues in this pass

- README installation instructions now document split dependency files:
  - `requirements.txt` for core CLI/runtime
  - `requirements-gui.txt` for GUI
  - `requirements-optional.txt` for PyVRP
  - `requirements-dev.txt` plus optional dependencies for full QA/dev
- QA checklist updated for PyVRP final routing, release smoke, and independent audit checks.

## Remaining backend issues

- No blocking backend issue found in the PyVRP integration.
- Smoke/runtime note: with PyVRP enabled, the end-to-end smoke run takes around 110-116 seconds on this machine. If this is too slow for CI, consider a dedicated smoke config with a lower `route_costing.pyvrp_time_limit_seconds` or a targeted PyVRP unit test plus a nearest-neighbor E2E smoke.

## Remaining GUI issues

- GUI import and worker contract checks pass.
- Full manual GUI click-through was not performed because starting the interactive event loop would block the automated QA run.

## Output files generated

- `output/smoke_test/final_schedule.xlsx`
- `output/smoke_test/maps/final_schedule_map.html`
- `output/final_cli_validation/final_schedule.xlsx`
- `output/final_cli_validation/maps/final_schedule_map.html`
- `output/release_smoke_test/final_schedule.xlsx`
- `output/release_smoke_test/maps/final_schedule_map.html`
- `output/final_logic_validation/sample_clients_logic_validation_result.json`
- `output/final_logic_validation/sample_clients_logic_validation_result.md`

## Recommendations

- Keep `python -m pytest tests` as the fast regression gate.
- Keep `python scripts/smoke_test.py` or `python scripts/release_smoke_test.py` as the end-to-end gate before releases.
- For CI speed, add a separate very-small final-routing fixture that exercises PyVRP without solving a full monthly sample.
