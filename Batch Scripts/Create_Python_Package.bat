@echo off
setlocal

cd ..
pip install setuptools wheel
python setup.py sdist bdist_wheel
cd batchScript

echo Finished Packaging.

pause
endlocal
