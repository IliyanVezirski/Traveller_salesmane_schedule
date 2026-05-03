# Logic Validation Guide

This project uses a route-first PVRP architecture: candidate daily routes are generated and costed first, then the master solver selects `z[candidate_route, day] = 1`.

The validation framework audits input data and exported schedules without replacing the solver with a client-day scheduler.

## Generate Synthetic Data

```bash
python scripts/generate_synthetic_clients.py --scenario full_1800 --output data/synthetic_1800_sofia.xlsx
```

Generate all standard datasets:

```bash
python scripts/generate_synthetic_clients.py --scenario all --output data/synthetic_1800_sofia.xlsx
```

## Run Logic Validation

```bash
python scripts/run_logic_validation.py --input data/synthetic_small_feasible.xlsx
```

Reports are written to `output/logic_validation/`.

## Run Performance Test

```bash
python scripts/run_performance_test_1800.py
```

The performance test forces haversine distances and does not require OSRM.

## PASS / WARNING / FAIL

PASS means the pipeline produced an Excel schedule and the independent audit found no business-rule violations.

WARNING means the result is logically usable but has soft issues such as underfilled days, unusually long routes, or low candidate coverage.

FAIL means at least one hard business rule failed: wrong visit frequency, duplicate same-day visit, wrong sales rep, missing route kilometers, over-capacity route, missing client coverage, or invalid/infeasible input.

## Audited Rules

- Frequency 2 clients must appear exactly twice in the month.
- Frequency 4 clients must appear exactly once in every week.
- Frequency 8 clients must appear exactly twice in every week.
- A client cannot be visited twice on the same day.
- A client must be visited only by its assigned `sales_rep`.
- Each `sales_rep` can have at most one selected candidate route per day.
- Daily route size should normally be 17-22 clients.
- Every selected route must have `route_km_total`.
- Route compactness warnings are raised for high route-km outliers or too many zones in one route.
- Required monthly visits must equal planned monthly visits per sales rep.
