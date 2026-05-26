@echo off
setlocal enabledelayedexpansion

set PROJECT_DIR=C:\SOLAR\solar_ingestion
set PYTHON_EXE=%PROJECT_DIR%\.venv\Scripts\python.exe
set LOG_DIR=%PROJECT_DIR%\logs\solaredge

for /f %%i in ('powershell -NoProfile -Command "Get-Date -Format yyyyMMdd"') do set LOG_DATE=%%i
for /f %%i in ('powershell -NoProfile -Command "Get-Date -Format HHmmss"') do set LOG_TIME=%%i

set LOG_FILE=%LOG_DIR%\solaredge_sitepower_%LOG_DATE%.log

cd /d %PROJECT_DIR%

echo ================================================== >> "%LOG_FILE%"
echo [%DATE% %TIME%] START SOLAREDGE SITEPOWER ALL >> "%LOG_FILE%"

"%PYTHON_EXE%" -m scripts.run_solaredge_all_active_ingest ^
  --endpoint sitePower ^
  --window-minutes 60 ^
  --lag-minutes 30 ^
  --sleep-seconds 3 ^
  --stop-on-error >> "%LOG_FILE%" 2>&1

set EXIT_CODE=%ERRORLEVEL%

echo [%DATE% %TIME%] END SOLAREDGE SITEPOWER ALL exit_code=%EXIT_CODE% >> "%LOG_FILE%"
echo. >> "%LOG_FILE%"

exit /b %EXIT_CODE%