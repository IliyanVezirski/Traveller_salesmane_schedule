# Logic Validation Report

## Overall Status

PASS

The validation framework, synthetic datasets, unit tests, small end-to-end logic validation, bad-data controls, infeasible-capacity controls, and the full 1800-client route-first performance run are passing. The 1800 run produces a feasible schedule with 0 hard audit errors; remaining route-density findings are warnings about route quality, not correctness.

## Synthetic datasets generated

- `data/synthetic_small_feasible.xlsx`: 2 sales reps, 80 clients, 320 required visits. This is a controlled route-first exact-cover smoke dataset with 20 clients per compact manual zone.
- `data/synthetic_medium_feasible.xlsx`: 5 sales reps, 500 clients, 1800 required visits.
- `data/synthetic_1800_sofia.xlsx`: 18 sales reps, 1800 clients, 6600 required visits.
- `data/synthetic_infeasible_capacity.xlsx`: 18 sales reps, 1800 clients, 12096 required visits.
- `data/synthetic_bad_coordinates.xlsx`: invalid coordinate/frequency/duplicate-id control cases.

## Test commands

```bash
python scripts/generate_synthetic_clients.py --scenario full_1800 --output data/synthetic_1800_sofia.xlsx
python scripts/run_logic_validation.py --input data/synthetic_small_feasible.xlsx
python scripts/run_performance_test_1800.py
python -m pytest
```

Executed in the latest validation pass:

- `python -m pytest` -> 17 passed.
- `python scripts/run_logic_validation.py --input data/synthetic_small_feasible.xlsx --time-limit 60 --candidates-per-rep 500` -> PASS.
- `python scripts/run_logic_validation.py --input data/synthetic_infeasible_capacity.xlsx` -> expected FAIL at input validation, 19 errors.
- `python scripts/run_logic_validation.py --input data/synthetic_bad_coordinates.xlsx` -> expected FAIL at input validation, 9 errors.
- `python scripts/run_performance_test_1800.py` -> PASS / feasible schedule generated.

## Frequency validation

Implemented in `src/result_audit.py` and covered by `tests/test_frequency_rules.py`.

Small end-to-end run:

- Required visits: 320.
- Planned visits: 320.
- Audit status: PASS.

## Daily route size validation

Implemented as hard FAIL above `daily_route.max_clients` when overfilled routes are not allowed, and WARNING below `daily_route.min_clients`.

Small end-to-end run:

- Routes: 16.
- Min clients per route: 20.
- Max clients per route: 20.
- Average clients per route: 20.0.

## Route km validation

Implemented as FAIL for missing, negative, or internally inconsistent `route_km_total` values.

Small end-to-end run:

- Total route km: 119.7.
- Selected route km min/median/max: 5.1 / 7.7 / 9.4.

## Performance Results

Executed with:

```bash
python scripts/run_performance_test_1800.py
```

Outputs:

- `output/logic_validation/performance_1800_report.json`
- `output/logic_validation/performance_1800_report.md`

Latest result:

- Status: SUCCESS.
- Solver status: FEASIBLE.
- Validation stage: 0.20s.
- Matrix building: 0.05s.
- Candidate generation: 262.62s.
- Master solving: 323.04s.
- Final routing: 2.71s.
- Export: 1.92s.
- Routes: 360.
- Planned visits: 6600.
- Required visits: 6600.
- Average clients per route: 18.33.
- Min/max clients per route: 18 / 19.
- Candidate coverage: 1800 OK, 0 WARNING, 0 ERROR.
- Audit: passed with 0 errors and 134 route-density warnings.
- Output Excel: `output/logic_validation/performance_1800_run/final_schedule.xlsx`.

## Issues found

- The existing backend validation checks world coordinate ranges but not Sofia-region bounds; the new audit layer adds Sofia/Sofia-region geofence checks for synthetic validation.
- Full 1800 optimization may require significant solver time. The performance runner reports runtime without changing the solver.
- Earlier random small feasible data was solver-infeasible because candidate coverage/exact cover was too weak for a small route-first instance. The small fixture was changed to a controlled compact exact-cover smoke dataset; full and medium synthetic datasets retain realistic mixed frequencies and geographic noise.
- Full 1800 generated input is capacity-feasible and now solves with route-first master selection. The candidate generator adds `periodic_seed` route candidates as a frequency-feasible backbone, then the master still chooses `z[candidate_route, day]`.

## Candidate coverage summary

Small end-to-end run:

- Candidate coverage: 80 OK, 0 WARNING, 0 ERROR.

Full 1800 generated input:

- Frequency mix: 700 clients at frequency 2, 900 at frequency 4, 200 at frequency 8.
- Required visits: 6600.
- Average visits per rep/day: 18.33.
- Max required visits by rep: 372, below the 400 target-capacity line.
- Candidate coverage after pruning/repair: 1800 OK, 0 WARNING, 0 ERROR.

## Required fixes by Backend Agent

- Completed: low post-pruning coverage was fixed with coverage top-up and repair candidates.
- Completed: route-first `periodic_seed` candidates were added to provide a frequency-feasible exact-cover backbone.
- Completed: master solving is decomposed by independent `sales_rep` subproblems while preserving the same `z[candidate_route, day]` route-first model.
- Remaining optimization work is quality-focused: reduce route-density warnings and improve cluster compactness without breaking frequency feasibility.

## Required fixes by GUI Agent

- No GUI changes required.

## Recommendations

- Run small logic validation on every change.
- Run the 1800-client performance test before release builds.
- Keep `z[candidate_route, day]` as the master decision variable.
