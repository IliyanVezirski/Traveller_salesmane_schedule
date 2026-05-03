# Packaging Report

## Status

PASS

Packaging structure, runtime path handling, dependency split, PyInstaller spec,
release scripts, logging helpers, OSRM status check, and user documentation were
added. PyInstaller one-folder build was executed successfully and the packaged
EXE was smoke-tested.

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

Verified ready executable:

```text
D:\Programming\Schedule_TP\sales_pvrp_scheduler\dist\SalesPVRP\SalesPVRP.exe
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

PyInstaller spec includes:

- PySide6 QtCore/QtGui/QtWidgets hidden imports
- collected submodules for sklearn, ortools, yaml, folium, and openpyxl
- OR-Tools dynamic libraries from `ortools\.libs`
- external runtime files copied next to the executable after build

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

- `python run_gui.py` started successfully and stayed alive during an offscreen GUI smoke window.
- `python -m py_compile ...` passed for changed Python files.
- `python main.py --version` returned `Sales PVRP Scheduler v0.1.0 (dev)`.
- `python scripts\check_gui_import.py` passed.
- `python main.py --input data/sample_clients.xlsx --config config.yaml --output output/test_run` returned controlled `infeasible` with diagnostics.
- `python scripts\release_smoke_test.py` passed and created Excel output.
- `python scripts\smoke_test.py` passed and verified Excel sheets.
- `python -m pytest tests` passed: 5 tests.
- `scripts\build_exe.bat` completed successfully.
- `dist\SalesPVRP\SalesPVRP.exe` started successfully with a sanitized PATH and stayed alive during an offscreen smoke window.
- GUI sample-load smoke passed with `data/sample_clients.xlsx` and loaded 40 rows.

Verified `dist\SalesPVRP` structure:

```text
dist\SalesPVRP\
  SalesPVRP.exe
  config.yaml
  README_USER.md
  data\
    input_clients_template.xlsx
    sample_clients.xlsx
  output\
  cache\
  logs\
  _internal\
```

## Known issues

- The app uses a PNG for the Qt window icon. A Windows `.ico` can be added later
  for the executable file icon.
- OSRM is optional. If unavailable, haversine fallback is used and route
  distances are approximate.
- The default production-style config returns controlled `infeasible` for the
  small sample workbook. The lighter smoke config in `scripts\release_smoke_test.py`
  produces a successful Excel output.
- PyInstaller emits benign warnings for optional modules such as sklearn's torch
  compatibility namespace and some optional scipy/pycparser internals.

## Recommendations

- Run `scripts\prepare_release.bat` when a separate `release\SalesPVRP` folder is needed.
- Test `release\SalesPVRP\SalesPVRP.exe` by opening `data\sample_clients.xlsx`.
- Keep `fallback_to_haversine: true` in distributed `config.yaml`.
- Add code signing and an installer wrapper after the one-folder build is stable.
- Keep route-first optimization ownership with the Backend Agent. This packaging
  pass did not replace the `z[candidate_route, day]` architecture.
