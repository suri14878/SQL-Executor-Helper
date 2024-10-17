@echo off
setlocal

rem sets the PROJ_DIR variable to the parent directory and VIRTUAL_ENV to python executable
FOR %%A IN ("%~dp0.") DO set "PROJ_DIR=%%~dpA"
set VIRTUAL_ENV=%PROJ_DIR%\env_executor\scripts\python.exe

cd "%PROJ_DIR%"
"%VIRTUAL_ENV%" -m pip install --upgrade build
"%VIRTUAL_ENV%" -m build

echo Finished Packaging.

pause
endlocal
