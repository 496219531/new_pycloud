@echo off
setlocal

set "SCRIPT_DIR=%~dp0"
for %%I in ("%SCRIPT_DIR%..") do set "REPO_ROOT=%%~fI"

if defined PYTHONPATH (
    set "PYTHONPATH=%REPO_ROOT%\src;%PYTHONPATH%"
) else (
    set "PYTHONPATH=%REPO_ROOT%\src"
)

if not defined PYCLOUD_HOME set "PYCLOUD_HOME=%REPO_ROOT%"

python -m pycloud_parallel.controlplane.ctl %*
exit /b %ERRORLEVEL%
