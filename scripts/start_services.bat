@echo off
setlocal EnableExtensions
rem Thin wrapper around the installed/local pycloudctl CLI.

for %%I in ("%~dp0.") do set "SCRIPT_DIR=%%~fI"
for %%I in ("%SCRIPT_DIR%\..") do set "REPO_ROOT=%%~fI"

if defined PYTHONPATH (
    set "PYTHONPATH=%REPO_ROOT%\src;%PYTHONPATH%"
) else (
    set "PYTHONPATH=%REPO_ROOT%\src"
)

if not defined PYCLOUD_HOME set "PYCLOUD_HOME=%REPO_ROOT%"

python -m pycloud_parallel.controlplane.ctl %*
set "EXIT_CODE=%ERRORLEVEL%"
endlocal & exit /b %EXIT_CODE%
