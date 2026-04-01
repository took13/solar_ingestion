@echo off
setlocal

set "BASE_DIR=C:\SOLAR_V2\solar_ingestion"
set "PYTHON_EXE=%BASE_DIR%\.venv\Scripts\python.exe"
set "LOG_DIR=%BASE_DIR%\logs"
set "LOG_FILE=%LOG_DIR%\online.log"

if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

echo ================================================== >> "%LOG_FILE%"
echo START %date% %time% >> "%LOG_FILE%"
echo BASE_DIR=%BASE_DIR% >> "%LOG_FILE%"
echo PYTHON_EXE=%PYTHON_EXE% >> "%LOG_FILE%"

cd /d "%BASE_DIR%"
echo CURRENT_DIR=%cd% >> "%LOG_FILE%"

if not exist "%PYTHON_EXE%" (
    echo ERROR: python.exe not found >> "%LOG_FILE%"
    exit /b 1
)

"%PYTHON_EXE%" -m scripts.run_job --job dev_history_online >> "%LOG_FILE%" 2>&1
set "EXITCODE=%ERRORLEVEL%"

echo EXITCODE=%EXITCODE% >> "%LOG_FILE%"
echo END %date% %time% >> "%LOG_FILE%"

exit /b %EXITCODE%