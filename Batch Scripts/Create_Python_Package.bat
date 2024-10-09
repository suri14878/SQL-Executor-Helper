@echo off
setlocal

cd ..
pip wheel .
cd batchScript

echo Finished Packaging.

pause
endlocal
