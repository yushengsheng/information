@echo off
setlocal
cd /d "%~dp0"

echo [0/2] Stopping previous UI instance if it is still running...
call "%~dp0stop_monitor_ui.bat" >nul 2>nul

echo [1/2] Installing or updating dependencies...
python -m pip install -r requirements.txt
if errorlevel 1 (
  echo Dependency installation failed.
  pause
  exit /b 1
)

echo [2/2] Starting local UI...
python app.py
if errorlevel 1 (
  echo UI exited with an error.
  pause
  exit /b 1
)
