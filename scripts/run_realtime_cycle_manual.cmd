@echo off
setlocal EnableExtensions EnableDelayedExpansion

REM ==================================================
REM SolarToPI - Manual Staggered Realtime Cycle
REM Safe simple version
REM ==================================================

set "APP_DIR=C:\SOLAR\solar_ingestion"
set "LOG_DIR=%APP_DIR%\logs"
set "LOCK_FILE=%LOG_DIR%\realtime_cycle.lock"
set "LOG_FILE=%LOG_DIR%\realtime_cycle_latest.log"
set "EXIT_CODE=0"

cd /d "%APP_DIR%"
if errorlevel 1 (
    echo [FAILED] Cannot change directory to %APP_DIR%
    exit /b 1
)

if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

REM ==================================================
REM Anti-overlap lock
REM ==================================================

if exist "%LOCK_FILE%" (
    echo Another realtime cycle is already running. Lock file exists: %LOCK_FILE%
    echo If no task/process is running, delete this file manually:
    echo del "%LOCK_FILE%"
    exit /b 2
)

echo %date% %time% > "%LOCK_FILE%"

echo. >> "%LOG_FILE%"
echo ================================================== >> "%LOG_FILE%"
echo Realtime cycle started at %date% %time% >> "%LOG_FILE%"
echo Working directory: %CD% >> "%LOG_FILE%"
echo Lock file: %LOCK_FILE% >> "%LOG_FILE%"
echo ================================================== >> "%LOG_FILE%"

REM ==================================================
REM Activate venv
REM ==================================================

if not exist "%APP_DIR%\.venv\Scripts\activate.bat" (
    echo [FAILED] Virtual environment not found: %APP_DIR%\.venv\Scripts\activate.bat >> "%LOG_FILE%"
    set "EXIT_CODE=1"
    goto cleanup
)

call "%APP_DIR%\.venv\Scripts\activate.bat" >> "%LOG_FILE%" 2>&1
if errorlevel 1 (
    echo [FAILED] Activate venv failed >> "%LOG_FILE%"
    set "EXIT_CODE=1"
    goto cleanup
)

REM ==================================================
REM STEP 1 - Plant realtime
REM ==================================================

echo. >> "%LOG_FILE%"
echo [STEP 1] Plant realtime >> "%LOG_FILE%"
python -m scripts.run_pipeline_plant_realtime >> "%LOG_FILE%" 2>&1
if errorlevel 1 (
    echo [FAILED] Plant realtime failed >> "%LOG_FILE%"
    set "EXIT_CODE=1"
    goto cleanup
)

timeout /t 60 /nobreak > nul

REM ==================================================
REM STEP 2 - Critical device realtime devType 10/17
REM ==================================================

echo. >> "%LOG_FILE%"
echo [STEP 2] Critical device realtime devType 10/17 >> "%LOG_FILE%"
python -m scripts.run_pipeline_critical_realtime >> "%LOG_FILE%" 2>&1
if errorlevel 1 (
    echo [FAILED] Critical device realtime failed >> "%LOG_FILE%"
    set "EXIT_CODE=1"
    goto cleanup
)

timeout /t 60 /nobreak > nul

REM ==================================================
REM STEP 3 - Inverter realtime controlled target
REM ==================================================

echo. >> "%LOG_FILE%"
echo [STEP 3] Inverter realtime controlled target >> "%LOG_FILE%"
python -m scripts.run_inverter_realtime_job >> "%LOG_FILE%" 2>&1
if errorlevel 1 (
    echo [FAILED] Inverter realtime failed >> "%LOG_FILE%"
    set "EXIT_CODE=1"
    goto cleanup
)

REM ==================================================
REM STEP 4 - Postprocess normalize + mart load
REM ==================================================

echo. >> "%LOG_FILE%"
echo [STEP 4] Realtime postprocess normalize + mart load >> "%LOG_FILE%"
python -m scripts.run_realtime_postprocess --lookback-minutes 60 >> "%LOG_FILE%" 2>&1
if errorlevel 1 (
    echo [FAILED] Realtime postprocess failed >> "%LOG_FILE%"
    set "EXIT_CODE=1"
    goto cleanup
)

echo. >> "%LOG_FILE%"
echo Realtime cycle finished successfully at %date% %time% >> "%LOG_FILE%"
set "EXIT_CODE=0"

:cleanup
echo. >> "%LOG_FILE%"
echo Cleaning up lock file at %date% %time% >> "%LOG_FILE%"

if exist "%LOCK_FILE%" del "%LOCK_FILE%"

if "%EXIT_CODE%"=="0" (
    echo Completed. Log: %LOG_FILE%
) else (
    echo Failed with exit code %EXIT_CODE%. Log: %LOG_FILE%
)

endlocal
exit /b %EXIT_CODE%