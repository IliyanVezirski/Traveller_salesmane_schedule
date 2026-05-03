@echo off
setlocal
cd /d "%~dp0\.."

if exist build rmdir /s /q build
if exist dist rmdir /s /q dist

echo Cleaned build and dist folders.
endlocal
