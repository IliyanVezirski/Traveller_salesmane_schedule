# Integration Report

## Status

PASS

Commit `0ebde1e Fix route-first 1800-client feasibility` is present locally and matches `origin/main`. The fix is validated: route-first architecture is preserved, the small feasible logic run passes, and the 1800-client performance run produces a feasible audited schedule.

## Git verification

```bash
git fetch origin
git rev-parse HEAD
git status --short --branch
git show --stat --oneline --decorate HEAD
```

Result:

- HEAD: `0ebde1ea31ad9f9b9d783df81747921de2a890e6`
- Branch: `main...origin/main`
- Commit: `0ebde1e (HEAD -> main, origin/main) Fix route-first 1800-client feasibility`
- Working tree before report edits: clean

## Architecture checks

Verified in code:

- `src/candidate_routes.py` adds `periodic_seed` route-first candidates.
- `src/candidate_routes.py` keeps route candidates with calculated `route_km`; no client-day scheduler replacement was introduced.
- `candidate_routes.min_candidates_per_client: 6` is present in config/defaults.
- Post-pruning coverage top-up is implemented.
- `src/pvrp_master_solver.py` still creates `z[candidate_route, day]` decision variables.
- Solver decomposition is by independent `sales_rep` subproblem.
- No `x[client, day]` architecture was introduced.

## Commands run

```bash
python -m pytest tests
python -m pytest tests\test_master_solver_objective.py -q
python scripts/run_logic_validation.py --input data\synthetic_small_feasible.xlsx --time-limit 60 --candidates-per-rep 500
python scripts/run_performance_test_1800.py
python scripts/run_performance_test_1800.py --output-dir output\final_performance_validation
```

Additional direct 1800 workbook checks:

```bash
python -c "import openpyxl, pandas as pd; p='output/final_performance_validation/performance_1800_run/final_schedule.xlsx'; wb=openpyxl.load_workbook(p, read_only=True, data_only=True); print(wb.sheetnames); df=pd.read_excel(p, sheet_name='Final_Schedule'); print(df.shape); print(df['final_route_method'].value_counts(dropna=False).to_dict())"
python -c "import pandas as pd; p='output/final_performance_validation/performance_1800_run/final_schedule.xlsx'; df=pd.read_excel(p, sheet_name='Final_Schedule'); print((df[df.visit_frequency.eq(2)].groupby('client_id').size()==2).all())"
```

## Test results

### Pytest

```text
19 passed
```

Added targeted objective tests:

- `test_master_solver_prefers_lower_route_km_when_candidates_are_equivalent`
- `test_master_solver_uses_total_objective_not_route_km_alone`

Targeted run:

```text
2 passed
```

### Small logic validation

Command:

```bash
python scripts/run_logic_validation.py --input data\synthetic_small_feasible.xlsx --time-limit 60 --candidates-per-rep 500
```

Result:

- Overall status: `PASS`
- Solver status: `OPTIMAL`
- Generated candidates: `1,010`
- Routes: `16`
- Planned visits: `320`
- Validation errors: `0`
- Output: `output/logic_validation/synthetic_small_feasible/final_schedule.xlsx`

### 1800 performance validation

The first run to the default existing output path reached export but failed with:

```text
PermissionError(13, 'Permission denied')
```

This happened while overwriting the existing workbook at `output/logic_validation/performance_1800_run/final_schedule.xlsx`. A fresh output directory was then used to isolate solver correctness from file-lock/overwrite risk.

Command:

```bash
python scripts/run_performance_test_1800.py --output-dir output\final_performance_validation
```

Result:

- Performance status: `success`
- Solver status: `FEASIBLE`
- Routes: `360`
- Planned visits: `6600`
- Candidate coverage: `1800 OK`
- Audit status: `WARNING`
- Audit passed: `True`
- Audit errors: `0`
- Audit warnings: `134` route-density quality warnings
- Output: `output/final_performance_validation/performance_1800_run/final_schedule.xlsx`

Stage timings:

- validation: `0.23s`
- matrix_building: `0.06s`
- candidate_generation: `270.04s`
- master_solving: `343.53s`
- final_routing: `2.80s`
- export: `2.10s`

## 1800 Excel validation

Workbook:

```text
output/final_performance_validation/performance_1800_run/final_schedule.xlsx
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

Direct checks:

- Final schedule rows: `6600`
- `final_route_method`: `{'pyvrp': 6600}`
- Missing `route_km_total`: `0`
- Duplicate client/day visits: `0`
- Frequency 2 exactly 2 visits: `PASS`
- Frequency 4 exactly 1 visit per week: `PASS`
- Frequency 8 exactly 2 visits per week: `PASS`
- Max selected routes per sales rep/day: `1`
- Candidate coverage severity: `{'OK': 1800}`
- Minimum candidate coverage: `6`
- Clients below recommended candidate coverage: `0`

## Fixed issues validated

- Completed: route-first exact-cover backbone via `periodic_seed` candidates.
- Completed: post-pruning top-up to minimum candidate coverage.
- Completed: decomposed CP-SAT by sales rep while preserving route-first `z[candidate_route, day]`.
- Completed: first feasible performance mode for 1800 runner.
- Completed: PyVRP iteration-limited performance final routing; fresh run final routing took `2.80s`.
- Completed: deterministic master-solver objective tests now verify that route distance affects candidate selection and that configured penalties participate in the total objective.

## Remaining backend issues

- No correctness blocker found.
- Route-density warnings remain quality-only: `134` warnings, `0` audit errors.
- The 1800 run is still heavy overall: about `10m 17s` measured across candidate generation and master solving on this machine.

## Remaining integration issues

- The first 1800 run failed at export because the default existing workbook path was not writable. This is consistent with a locked/open Excel file or overwrite permission issue. It did not indicate solver failure.
- Recommendation: performance/release scripts should write to a fresh timestamped run folder or handle locked output workbooks with a clearer message and alternate filename.

## Remaining GUI issues

- No GUI changes required for this backend fix.

## Output files generated/validated

- `output/logic_validation/synthetic_small_feasible/final_schedule.xlsx`
- `output/final_performance_validation/performance_1800_report.json`
- `output/final_performance_validation/performance_1800_report.md`
- `output/final_performance_validation/performance_1800_run/final_schedule.xlsx`

## Recommendations

- Accept the backend fix as correct for feasibility and hard constraints.
- Keep `python -m pytest tests` and the small logic validation as routine gates.
- Run the 1800 performance test before release builds, preferably with a unique output directory.
- Treat route-density warnings as quality optimization work for Backend Optimization Agent, not an integration blocker.
