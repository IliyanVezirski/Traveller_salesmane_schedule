@echo off
setlocal
cd /d "%~dp0\.."

if exist ".venv\Scripts\activate.bat" (
    call ".venv\Scripts\activate.bat"
) else if exist "venv\Scripts\activate.bat" (
    call "venv\Scripts\activate.bat"
)

python -m pip install --upgrade pip
if errorlevel 1 exit /b 1

python -m pip install -r requirements-dev.txt
if errorlevel 1 exit /b 1

python scripts\check_gui_import.py
if errorlevel 1 exit /b 1

python -m PyInstaller packaging\SalesPVRP.spec --clean --noconfirm
if errorlevel 1 exit /b 1

set "DIST_DIR=dist\SalesPVRP"
if exist "%DIST_DIR%" (
    copy /Y "config.yaml" "%DIST_DIR%\config.yaml" >nul
    mkdir "%DIST_DIR%\data" >nul 2>nul
    if exist "data\input_clients_template.xlsx" copy /Y "data\input_clients_template.xlsx" "%DIST_DIR%\data\input_clients_template.xlsx" >nul
    if exist "data\sample_clients.xlsx" copy /Y "data\sample_clients.xlsx" "%DIST_DIR%\data\sample_clients.xlsx" >nul
    copy /Y "README_USER.md" "%DIST_DIR%\README_USER.md" >nul
    mkdir "%DIST_DIR%\output" >nul 2>nul
    mkdir "%DIST_DIR%\output\maps" >nul 2>nul
    mkdir "%DIST_DIR%\output\logs" >nul 2>nul
    mkdir "%DIST_DIR%\output\runs" >nul 2>nul
    mkdir "%DIST_DIR%\cache" >nul 2>nul
    mkdir "%DIST_DIR%\cache\osrm_matrices" >nul 2>nul
    mkdir "%DIST_DIR%\cache\candidate_routes" >nul 2>nul
    mkdir "%DIST_DIR%\cache\route_costs" >nul 2>nul
    mkdir "%DIST_DIR%\logs" >nul 2>nul
)

echo.
echo Build complete: dist\SalesPVRP\SalesPVRP.exe
endlocal
