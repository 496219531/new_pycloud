@echo off
setlocal EnableExtensions

if not defined PYTHON_BIN set "PYTHON_BIN=python"
set "DRY_RUN=0"
set "NO_BUILD=0"

:parse_args
if "%~1"=="" goto after_args
if "%~1"=="--dry-run" (
    set "DRY_RUN=1"
    shift
    goto parse_args
)
if "%~1"=="--no-build" (
    set "NO_BUILD=1"
    shift
    goto parse_args
)
if "%~1"=="-h" goto usage
if "%~1"=="--help" goto usage
echo Unknown argument: %~1 1>&2
goto usage_error

:after_args
if exist "%CD%\pyproject.toml" (
    set "REPO_ROOT=%CD%"
) else (
    for %%I in ("%~dp0\..") do set "REPO_ROOT=%%~fI"
)

set "HELPER=%TEMP%\pycloud_bump_and_build_%RANDOM%.py"
> "%HELPER%" echo from pathlib import Path
>> "%HELPER%" echo import re
>> "%HELPER%" echo import sys
>> "%HELPER%" echo.
>> "%HELPER%" echo root = Path(sys.argv[1])
>> "%HELPER%" echo dry_run = sys.argv[2] == "1"
>> "%HELPER%" echo pyproject_path = root / "pyproject.toml"
>> "%HELPER%" echo init_path = root / "src" / "pycloud_parallel" / "__init__.py"
>> "%HELPER%" echo pyproject = pyproject_path.read_text(encoding="utf-8")
>> "%HELPER%" echo init_py = init_path.read_text(encoding="utf-8")
>> "%HELPER%" echo pyproject_match = re.search(r'^version\s*=\s*"([^"]+)"\s*$', pyproject, re.MULTILINE)
>> "%HELPER%" echo init_match = re.search(r'^__version__\s*=\s*"([^"]+)"\s*$', init_py, re.MULTILINE)
>> "%HELPER%" echo if pyproject_match is None:
>> "%HELPER%" echo     raise SystemExit("could not find project.version in pyproject.toml")
>> "%HELPER%" echo if init_match is None:
>> "%HELPER%" echo     raise SystemExit("could not find __version__ in src/pycloud_parallel/__init__.py")
>> "%HELPER%" echo current = pyproject_match.group(1)
>> "%HELPER%" echo if current != init_match.group(1):
>> "%HELPER%" echo     raise SystemExit(f"version mismatch: pyproject.toml={current} src/pycloud_parallel/__init__.py={init_match.group(1)}")
>> "%HELPER%" echo match = re.fullmatch(r"(\d+)\.(\d+)\.(\d+)", current)
>> "%HELPER%" echo if match is None:
>> "%HELPER%" echo     raise SystemExit(f"unsupported version format: {current!r}")
>> "%HELPER%" echo major, minor, patch = (int(part) for part in match.groups())
>> "%HELPER%" echo next_version = f"{major}.{minor}.{patch + 1}"
>> "%HELPER%" echo print(f"Current version: {current}")
>> "%HELPER%" echo print(f"Next version:    {next_version}")
>> "%HELPER%" echo if dry_run:
>> "%HELPER%" echo     raise SystemExit(0)
>> "%HELPER%" echo pyproject_updated = re.sub(r'(^version\s*=\s*")([^"]+)("\s*$)', rf'\g<1>{next_version}\g<3>', pyproject, count=1, flags=re.MULTILINE)
>> "%HELPER%" echo init_updated = re.sub(r'(^__version__\s*=\s*")([^"]+)("\s*$)', rf'\g<1>{next_version}\g<3>', init_py, count=1, flags=re.MULTILINE)
>> "%HELPER%" echo pyproject_path.write_text(pyproject_updated, encoding="utf-8")
>> "%HELPER%" echo init_path.write_text(init_updated, encoding="utf-8")
>> "%HELPER%" echo print(f"Updated version files to {next_version}")
>> "%HELPER%" echo (root / ".pycloud_next_version").write_text(next_version, encoding="utf-8")

"%PYTHON_BIN%" "%HELPER%" "%REPO_ROOT%" "%DRY_RUN%"
set "EXIT_CODE=%ERRORLEVEL%"
del "%HELPER%" >nul 2>nul
if not "%EXIT_CODE%"=="0" exit /b %EXIT_CODE%
if "%DRY_RUN%"=="1" exit /b 0

set /p NEXT_VERSION=<"%REPO_ROOT%\.pycloud_next_version"
del "%REPO_ROOT%\.pycloud_next_version" >nul 2>nul

if "%NO_BUILD%"=="1" exit /b 0

pushd "%REPO_ROOT%" || exit /b 1
"%PYTHON_BIN%" -m build
set "EXIT_CODE=%ERRORLEVEL%"
if not "%EXIT_CODE%"=="0" (
    popd
    exit /b %EXIT_CODE%
)
echo Build complete.
dir /b dist | findstr /C:"%NEXT_VERSION%" >nul 2>nul
popd
exit /b 0

:usage
echo Usage:
echo   scripts\bump_and_build.bat [--dry-run] [--no-build]
echo.
echo Options:
echo   --dry-run   Print the current and next version without modifying files
echo   --no-build  Update the version but skip python -m build
echo   -h, --help  Show this help
echo.
echo Environment:
echo   PYTHON_BIN  Python executable to use (default: python)
exit /b 0

:usage_error
call :usage
exit /b 1
