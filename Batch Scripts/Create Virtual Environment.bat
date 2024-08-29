@echo off
setlocal

rem sets the PROJ_DIR variable to the parent directory. 
FOR %%A IN ("%~dp0.") DO set "PROJ_DIR=%%~dpA"
set VENV_NAME=env_executor

rem Check if Python is installed
python --version >nul 2>nul
if errorlevel 1 (
    echo Python is not installed or not found in PATH. Please install python! & echo Exiting.
    pause & exit /b 1
)

rem Check if the virtual environment python executable exists
if exist "%PROJ_DIR%\%VENV_NAME%\scripts\python.exe" (
    echo Virtual environment exists already. & echo Exiting. 
    pause & exit /b 1
)

rem Create virtual environment
echo Attempting to create virtual enviornment in %PROJ_DIR%
echo Please wait....
cd "%PROJ_DIR%"& python -m venv %VENV_NAME%
echo virtual enviornment created.

rem Install dependencies into environment
echo Attempting to install dependencies to virtual enviornment...
cd %VENV_NAME%/Scripts 
call activate.bat 
cd ../../Setup
python -m pip install -r requirements.txt
echo Finished installing dependencies. & echo Completed set-up process. 

pause
endlocal