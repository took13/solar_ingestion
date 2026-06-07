@echo off
setlocal EnableExtensions EnableDelayedExpansion

REM SolarToPI Milestone 23 controlled scheduler wrapper
REM Runs realtime cycle with 30-second inter-step delay.
REM Scope: scheduler wrapper only. Does not change API pacing or schedule interval.

set "SCRIPT_DIR=%~dp0"
for %%I in ("%SCRIPT_DIR%..\..") do set "PROJECT_DIR=%%~fI"
set "PYTHON_EXE=%PROJECT_DIR%\.venv\Scripts\python.exe"
set "LOG_DIR=%PROJECT_DIR%\logs"
set "LOCK_FILE=%LOG_DIR%\realtime_cycle_timed_30s.lock"
set "LATEST_LOG=%LOG_DIR%\realtime_cycle_timed_30s_latest.log"

if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

for /f %%i in ('powershell -NoProfile -Command "Get-Date -Format yyyyMMdd_HHmmss"') do set "TS=%%i"
set "RUN_LOG=%LOG_DIR%\realtime_cycle_timed_30s_%TS%.log"

if exist "%LOCK_FILE%" (
    echo [%DATE% %TIME%] SKIP: lock exists: %LOCK_FILE%>> "%LATEST_LOG%"
    echo Another realtime cycle appears to be running. Skip this run.
    exit /b 0
)

echo started_at=%DATE% %TIME%> "%LOCK_FILE%"
echo project_dir=%PROJECT_DIR%>> "%LOCK_FILE%"
echo log_file=%RUN_LOG%>> "%LOCK_FILE%"

echo ================================================================================>> "%RUN_LOG%"
echo [CMD] START SolarToPI realtime cycle timed 30s>> "%RUN_LOG%"
echo [CMD] PROJECT_DIR=%PROJECT_DIR%>> "%RUN_LOG%"
echo [CMD] PYTHON_EXE=%PYTHON_EXE%>> "%RUN_LOG%"
echo [CMD] RUN_LOG=%RUN_LOG%>> "%RUN_LOG%"
echo [CMD] LOCK_FILE=%LOCK_FILE%>> "%RUN_LOG%"
echo ================================================================================>> "%RUN_LOG%"

if not exist "%PYTHON_EXE%" (
    echo [CMD] ERROR: Python executable not found: %PYTHON_EXE%>> "%RUN_LOG%"
    set "RUN_EXIT=9009"
    goto cleanup
)

pushd "%PROJECT_DIR%"
"%PYTHON_EXE%" -m scripts.run_realtime_cycle_timed --delay-seconds 30 --lookback-minutes 60 >> "%RUN_LOG%" 2>&1
set "RUN_EXIT=!ERRORLEVEL!"
popd

echo ================================================================================>> "%RUN_LOG%"
echo [CMD] END exit_code=!RUN_EXIT! finished_at=%DATE% %TIME%>> "%RUN_LOG%"
echo ================================================================================>> "%RUN_LOG%"

:cleanup
copy /Y "%RUN_LOG%" "%LATEST_LOG%" >nul 2>&1
if exist "%LOCK_FILE%" del /Q "%LOCK_FILE%" >nul 2>&1
exit /b !RUN_EXIT!
