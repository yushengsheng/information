@echo off
setlocal
cd /d "%~dp0"

powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0stop_monitor_ui.ps1"
set "exit_code=%errorlevel%"

if not "%exit_code%"=="0" (
  echo.
  echo Stop script returned exit code %exit_code%.
)

exit /b %exit_code%
