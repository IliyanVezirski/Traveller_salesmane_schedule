# Packaging Report

## Status

PARTIAL

Packaging structure, runtime path handling, dependency split, PyInstaller spec,
release scripts, logging helpers, OSRM status check, and user documentation were
added. Local smoke checks passed.

The PyInstaller build configuration is real, but an executable build should be
performed on the target Windows release machine before marking the release PASS.

## Added files

- `src/app_paths.py`
- `src/version.py`
- `src/logging_utils.py`
- `src/osrm_status.py`
- `requirements-gui.txt`
- `requirements-dev.txt`
- `requirements-optional.txt`
- `packaging/SalesPVRP.spec`
- `packaging/PACKAGING_README.md`
- `scripts/build_exe.bat`
- `scripts/build_exe.ps1`
- `scripts/clean_build.bat`
- `scripts/prepare_release.bat`
- `scripts/run_cli.bat`
- `scripts/run_gui.bat`
- `scripts/check_gui_import.py`
- `scripts/release_smoke_test.py`
- `README_USER.md`
- `.gitignore`

## Build command

```bat
scripts\build_exe.bat
```

PowerShell:

```powershell
.\scripts\build_exe.ps1
```

Release folder:

```bat
scripts\prepare_release.bat
```

Expected executable:

```text
dist\SalesPVRP\SalesPVRP.exe
```

The build scripts copy editable runtime files into `dist\SalesPVRP` after
PyInstaller completes, so the executable can run directly from `dist`.

Expected release folder:

```text
release\SalesPVRP\
```

## Dependencies

Backend dependencies are in `requirements.txt`:

- pandas
- numpy
- openpyxl
- scikit-learn
- ortools
- requests
- pyyaml
- folium
- tqdm

GUI dependencies are in `requirements-gui.txt`.

Developer/build dependencies are in `requirements-dev.txt`.

## PyVRP

`pyvrp` is marked optional in `requirements-optional.txt`. The current runtime
path uses `nearest_neighbor_2opt` for final route ordering and does not import
`pyvrp`, so the packaged app can run without PyVRP.

## Smoke tests

```bat
python scripts\check_gui_import.py
python scripts\release_smoke_test.py
python scripts\smoke_test.py
python -m pytest tests
```

`release_smoke_test.py` disables OSRM and cache usage so it can run on a clean
developer or release machine.

Verified locally:

- `python -m py_compile ...` passed for changed Python files.
- `python main.py --version` returned `Sales PVRP Scheduler v0.1.0 (dev)`.
- `python scripts\check_gui_import.py` passed.
- `python main.py --input data/sample_clients.xlsx --config config.yaml --output output/test_run` returned controlled `infeasible` with diagnostics.
- `python scripts\release_smoke_test.py` passed and created Excel output.
- `python scripts\smoke_test.py` passed and verified Excel sheets.
- `python -m pytest tests` passed: 5 tests.

## Known issues

- PyInstaller build was not committed as a binary artifact; build it locally on
  Windows before release.
- The app uses a PNG for the Qt window icon. A Windows `.ico` can be added later
  for the executable file icon.
- OSRM is optional. If unavailable, haversine fallback is used and route
  distances are approximate.

## Recommendations

- Run `scripts\prepare_release.bat` on a clean Windows venv before release.
- Test `release\SalesPVRP\SalesPVRP.exe` by opening `data\sample_clients.xlsx`.
- Keep `fallback_to_haversine: true` in distributed `config.yaml`.
- Add code signing and an installer wrapper after the one-folder build is stable.
- Keep route-first optimization ownership with the Backend Agent. This packaging
  pass did not replace the `z[candidate_route, day]` architecture.
