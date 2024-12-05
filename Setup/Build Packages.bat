@echo off
setlocal

rem sets the PROJ_DIR variable to the parent directory and VIRTUAL_ENV to python executable
FOR %%A IN ("%~dp0.") DO set "PROJ_DIR=%%~dpA"
set VIRTUAL_ENV=%PROJ_DIR%\venv\scripts\python.exe

rem Check if Python is installed
python --version >nul 2>nul
if errorlevel 1 (
    echo Python is not installed or not found in PATH.
    pause
    rem exit /b 1
)

rem Check if the virtual environment python executable exists
if not exist "%VIRTUAL_ENV%" (
    echo Virtual environment not found. Please create it first and provide the correct path to your virtual environment's python.exe file.
    pause
    rem exit /b 1
)

cd "%PROJ_DIR%"
"%VIRTUAL_ENV%" -m pip install --upgrade build
"%VIRTUAL_ENV%" -m build

pause
endlocal