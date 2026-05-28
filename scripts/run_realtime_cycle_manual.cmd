@echo off
setlocal enabledelayedexpansion

cd /d C:\SOLAR\solar_ingestion

set LOG_DIR=C:\SOLAR\solar_ingestion\logs
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

for /f "tokens=1-4 delims=/ " %%a in ("%date%") do set TODAY=%%d%%b%%c
for /f "tokens=1-3 delims=:." %%a in ("%time%") do set NOW=%%a%%b%%c
set NOW=%NOW: =0%

set LOG_FILE=%LOG_DIR%\realtime_cycle_%TODAY%_%NOW%.log

echo ================================================== >> "%LOG_FILE%"
echo Realtime cycle started at %date% %time% >> "%LOG_FILE%"
echo ================================================== >> "%LOG_FILE%"

call .venv\Scripts\activate.bat

echo [STEP 1] Plant realtime >> "%LOG_FILE%"
python -m scripts.run_pipeline_plant_realtime >> "%LOG_FILE%" 2>&1
if errorlevel 1 (
    echo [FAILED] Plant realtime failed >> "%LOG_FILE%"
    exit /b 1
)

timeout /t 60 /nobreak > nul

echo [STEP 2] Critical device realtime devType 10/17 >> "%LOG_FILE%"
python -m scripts.run_pipeline_critical_realtime >> "%LOG_FILE%" 2>&1
if errorlevel 1 (
    echo [FAILED] Critical device realtime failed >> "%LOG_FILE%"
    exit /b 1
)

timeout /t 60 /nobreak > nul

echo [STEP 3] Inverter realtime controlled target >> "%LOG_FILE%"
python -m scripts.run_inverter_realtime_job >> "%LOG_FILE%" 2>&1
if errorlevel 1 (
    echo [FAILED] Inverter realtime failed >> "%LOG_FILE%"
    exit /b 1
)

echo ================================================== >> "%LOG_FILE%"
echo Realtime cycle finished at %date% %time% >> "%LOG_FILE%"
echo Log: %LOG_FILE% >> "%LOG_FILE%"
echo ================================================== >> "%LOG_FILE%"

echo Completed. Log: %LOG_FILE%

endlocal
exit /b 0