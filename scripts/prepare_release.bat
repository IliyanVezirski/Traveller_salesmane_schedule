@echo off
setlocal
cd /d "%~dp0\.."

call scripts\build_exe.bat
if errorlevel 1 exit /b 1

set "DIST_DIR=dist\SalesPVRP"
set "RELEASE_DIR=release\SalesPVRP"

if not exist "%DIST_DIR%\SalesPVRP.exe" (
    echo Missing built executable: %DIST_DIR%\SalesPVRP.exe
    exit /b 1
)

if exist "%RELEASE_DIR%" rmdir /s /q "%RELEASE_DIR%"
mkdir "%RELEASE_DIR%"
xcopy "%DIST_DIR%" "%RELEASE_DIR%\" /E /I /Y >nul

copy /Y "config.yaml" "%RELEASE_DIR%\config.yaml" >nul
mkdir "%RELEASE_DIR%\data" >nul 2>nul
if exist "data\input_clients_template.xlsx" copy /Y "data\input_clients_template.xlsx" "%RELEASE_DIR%\data\input_clients_template.xlsx" >nul
if exist "data\sample_clients.xlsx" copy /Y "data\sample_clients.xlsx" "%RELEASE_DIR%\data\sample_clients.xlsx" >nul
copy /Y "README_USER.md" "%RELEASE_DIR%\README_USER.md" >nul

mkdir "%RELEASE_DIR%\output" >nul 2>nul
mkdir "%RELEASE_DIR%\output\maps" >nul 2>nul
mkdir "%RELEASE_DIR%\output\logs" >nul 2>nul
mkdir "%RELEASE_DIR%\output\runs" >nul 2>nul
mkdir "%RELEASE_DIR%\cache" >nul 2>nul
mkdir "%RELEASE_DIR%\cache\osrm_matrices" >nul 2>nul
mkdir "%RELEASE_DIR%\cache\candidate_routes" >nul 2>nul
mkdir "%RELEASE_DIR%\cache\route_costs" >nul 2>nul
mkdir "%RELEASE_DIR%\logs" >nul 2>nul

echo Release prepared: %RELEASE_DIR%
endlocal
