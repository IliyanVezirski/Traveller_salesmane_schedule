@echo off
setlocal
cd /d "%~dp0\.."
python main.py --input data\sample_clients.xlsx --config config.yaml --output output\test_run
endlocal
