# Sales PVRP Scheduler

Route-first Periodic Vehicle Routing Problem scheduler for monthly sales representative visit planning.

The backend generates compact candidate daily routes with precomputed `route_km`, then the master optimizer chooses:

```text
z[candidate_route, day] = 1
```

It does not use a client-day-first scheduler.

## Installation

Core CLI/runtime dependencies:

```bash
python -m pip install -r requirements.txt
```

GUI dependencies:

```bash
python -m pip install -r requirements-gui.txt
```

Optional PyVRP final-routing dependency:

```bash
python -m pip install -r requirements-optional.txt
```

Development, tests, and packaging:

```bash
python -m pip install -r requirements-dev.txt -r requirements-optional.txt
```

Python 3.11+ is recommended.

## Run CLI

Fast smoke run with the included sample data:

```bash
python main.py --input data/sample_clients.xlsx --output output/cli_test --no-osrm --no-cache --quiet-solver --time-limit 20 --num-workers 4 --target-clients 6 --min-clients 1 --max-clients 8 --candidates-per-rep 250 --keep-top-n-per-rep 250
```

Production-style run:

```bash
python main.py --input data/input_clients.xlsx --config config.yaml --output output
```

If `data/input_clients.xlsx` does not exist, `python main.py` falls back to `data/sample_clients.xlsx`, but the default production config can take much longer than the smoke command.

## Run GUI

Install GUI dependencies first:

```bash
python -m pip install -r requirements-gui.txt
```

```bash
python run_gui.py
```

GUI flow:

1. Choose an Excel/CSV input file.
2. Load and validate the data.
3. Adjust runtime parameters.
4. Start optimization.
5. Open the generated Excel workbook, HTML map, or output folder.

The GUI calls the backend only through:

```python
from src.pipeline import run_pipeline
```

## Public Pipeline Contract

```python
run_pipeline(
    input_path: str,
    config: dict,
    output_dir: str = "output",
    progress_callback=None,
    log_callback=None,
    cancel_checker=None,
) -> dict
```

The returned dict includes `status`, `excel_path`, `map_path`, `summary_by_sales_rep`, `summary_by_day`, `validation`, `total_route_km`, and `message`.

## Input Excel Format

Required columns:

- `client_id`
- `client_name`
- `sales_rep`
- `lat`
- `lon`
- `visit_frequency`

Optional columns:

- `fixed_weekday`
- `forbidden_weekdays`
- `preferred_weekdays`
- `cluster_manual`
- `notes`

`visit_frequency` must be one of `2`, `4`, or `8`.

Included files:

- `data/sample_clients.xlsx`
- `data/input_clients_template.xlsx`

## Output Files

Default workbook:

```bash
output/final_schedule.xlsx
```

Expected sheets:

- `Final_Schedule`
- `Daily_Routes`
- `Summary_By_Sales_Rep`
- `Summary_By_Day`
- `Validation`
- `Candidate_Routes_Selected`
- `Candidate_Coverage`
- `Parameters`

Default map:

```bash
output/maps/final_schedule_map.html
```

## Frequency Rules

- Frequency `2`: exactly 2 monthly visits, with Week 1+3 or Week 2+4 preferred.
- Frequency `4`: exactly 1 visit each week.
- Frequency `8`: exactly 2 visits each week.

These are hard constraints in the route-first master model. Spacing preferences are objective penalties.

## Final Routing

`route_costing.final_method: "pyvrp"` runs PyVRP for selected daily routes when `route_type: "open"`. The implementation models the route with one vehicle and a zero-cost dummy depot, then writes `final_route_method` to the exported schedule rows. If PyVRP is unavailable or cannot solve a route, the backend falls back to `nearest_neighbor_2opt` and records that fallback method in the same column.

## OSRM Fallback

Use OSRM when available:

```yaml
osrm:
  url: "http://localhost:5000"
  use_osrm: true
  fallback_to_haversine: true
```

If OSRM is unavailable and `fallback_to_haversine: true`, the backend builds an approximate haversine distance matrix so development and diagnostics can continue. Matrices are cached under `cache/osrm_matrices` when `use_cache` is enabled.

## Infeasible Solution Diagnostics

When no feasible route-first plan is found, the pipeline returns `status: "infeasible"` and emits diagnostics for common causes:

- overloaded sales representatives
- clients with zero or low candidate coverage
- candidate generation gaps
- overly tight route size or weekday constraints

Useful first adjustments:

- increase `candidate_routes.candidates_per_rep`
- increase `candidate_routes.keep_top_n_per_rep`
- reduce `daily_route.min_clients`
- set `daily_route.allow_underfilled: true`
- increase `optimization.time_limit_seconds`
- relax fixed, preferred, or forbidden weekday constraints

## Smoke Tests

Install development and optional final-routing dependencies first:

```bash
python -m pip install -r requirements-dev.txt -r requirements-optional.txt
```

Run the full integration smoke script:

```bash
python scripts/smoke_test.py
```

Run the lightweight pytest suite:

```bash
python -m pytest tests
```

## Troubleshooting

- Missing columns: use `data/input_clients_template.xlsx` as the input format reference.
- Locked Excel file: close `final_schedule.xlsx` before rerunning export.
- OSRM connection errors: use `--no-osrm` for CLI smoke runs or keep `fallback_to_haversine: true`.
- Slow solver: lower candidate counts for smoke testing; raise them for production quality.
- Infeasible result: inspect `Validation`, `Candidate_Coverage`, and CLI/GUI logs before changing business rules.

## Packaging

Install GUI/build dependencies:

```bash
python -m pip install -r requirements-dev.txt
```

Build the Windows one-folder executable:

```bat
scripts\build_exe.bat
```

Prepare a release folder:

```bat
scripts\prepare_release.bat
```

Packaging details are in `packaging/PACKAGING_README.md` and `PACKAGING_REPORT.md`.
