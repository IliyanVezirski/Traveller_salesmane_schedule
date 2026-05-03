$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")

if (Test-Path ".venv\Scripts\Activate.ps1") {
    . ".venv\Scripts\Activate.ps1"
} elseif (Test-Path "venv\Scripts\Activate.ps1") {
    . "venv\Scripts\Activate.ps1"
}

python -m pip install --upgrade pip
python -m pip install -r requirements-dev.txt
python scripts\check_gui_import.py
python -m PyInstaller packaging\SalesPVRP.spec --clean --noconfirm

$distDir = "dist\SalesPVRP"
if (Test-Path $distDir) {
    Copy-Item "config.yaml" (Join-Path $distDir "config.yaml") -Force
    New-Item -ItemType Directory -Force -Path (Join-Path $distDir "data") | Out-Null
    if (Test-Path "data\input_clients_template.xlsx") {
        Copy-Item "data\input_clients_template.xlsx" (Join-Path $distDir "data\input_clients_template.xlsx") -Force
    }
    if (Test-Path "data\sample_clients.xlsx") {
        Copy-Item "data\sample_clients.xlsx" (Join-Path $distDir "data\sample_clients.xlsx") -Force
    }
    Copy-Item "README_USER.md" (Join-Path $distDir "README_USER.md") -Force
    foreach ($folder in @(
        "output",
        "output\maps",
        "output\logs",
        "output\runs",
        "cache",
        "cache\osrm_matrices",
        "cache\candidate_routes",
        "cache\route_costs",
        "logs"
    )) {
        New-Item -ItemType Directory -Force -Path (Join-Path $distDir $folder) | Out-Null
    }
}

Write-Host ""
Write-Host "Build complete: dist\SalesPVRP\SalesPVRP.exe"
