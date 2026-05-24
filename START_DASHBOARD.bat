@echo off
setlocal
title Polymarket Paper Console
cd /d "%~dp0"

where python >nul 2>nul
if errorlevel 1 (
  echo Python was not found on PATH.
  echo Install Python or add it to PATH, then run this file again.
  pause
  exit /b 1
)

echo Starting Polymarket Paper Console...
echo.
echo The dashboard will open in your browser.
echo Use the Close App button in the dashboard when finished.
echo.
python paper_bot.py dashboard
endlocal
