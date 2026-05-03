# Packaging README

## Build a Windows EXE

From the project root:

```bat
scripts\build_exe.bat
```

PowerShell alternative:

```powershell
.\scripts\build_exe.ps1
```

The build uses PyInstaller one-folder mode and writes:

```text
dist\SalesPVRP\SalesPVRP.exe
```

The build scripts also copy editable runtime files next to the executable:
`config.yaml`, `README_USER.md`, sample files under `data`, and writable
`output`, `cache`, and `logs` folders.

## Prepare Release Folder

```bat
scripts\prepare_release.bat
```

This creates:

```text
release\SalesPVRP\
```

with the executable, `config.yaml`, sample Excel files, `README_USER.md`,
and writable `output`, `cache`, and `logs` folders.

## Smoke Checks

```bat
python scripts\check_gui_import.py
python scripts\release_smoke_test.py
```

`release_smoke_test.py` disables OSRM and caches so it can run without a local
OSRM server.
