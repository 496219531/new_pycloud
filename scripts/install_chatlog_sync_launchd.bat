@echo off
setlocal EnableExtensions

for %%I in ("%~dp0.") do set "SCRIPT_DIR=%%~fI"
set "TARGET_ROOT=%~1"
if not defined TARGET_ROOT set "TARGET_ROOT=%USERPROFILE%\.codex\chat_backup"
set "LINK_WORKSPACE=%~2"
set "RUNNER_DIR=%USERPROFILE%\.codex\bin"
set "RUNNER=%RUNNER_DIR%\sync_codex_chat_logs.py"

mkdir "%TARGET_ROOT%\chat_logs" >nul 2>nul
mkdir "%RUNNER_DIR%" >nul 2>nul
copy /Y "%SCRIPT_DIR%\sync_codex_chat_logs.py" "%RUNNER%" >nul
if errorlevel 1 exit /b %ERRORLEVEL%

set "TASK_NAME=CodexChatlogSync"
for %%I in ("%RUNNER%") do set "RUNNER_TASK_PATH=%%~sI"
for %%I in ("%TARGET_ROOT%") do set "TARGET_TASK_PATH=%%~sI"

schtasks /Delete /TN "%TASK_NAME%" /F >nul 2>nul
schtasks /Create /TN "%TASK_NAME%" /SC MINUTE /MO 1 /TR "python %RUNNER_TASK_PATH% --workspace %TARGET_TASK_PATH%" /F
if errorlevel 1 exit /b %ERRORLEVEL%
schtasks /Run /TN "%TASK_NAME%" >nul 2>nul

if defined LINK_WORKSPACE (
    mkdir "%LINK_WORKSPACE%\chat_logs" >nul 2>nul
    if exist "%LINK_WORKSPACE%\chat_logs\auto_sessions" rmdir "%LINK_WORKSPACE%\chat_logs\auto_sessions" >nul 2>nul
    mklink /D "%LINK_WORKSPACE%\chat_logs\auto_sessions" "%TARGET_ROOT%\chat_logs\sessions" >nul 2>nul
)

echo Installed and started: %TASK_NAME%
echo Runner: %RUNNER%
echo ArchiveRoot: %TARGET_ROOT%
if defined LINK_WORKSPACE echo Symlink: %LINK_WORKSPACE%\chat_logs\auto_sessions -^> %TARGET_ROOT%\chat_logs\sessions
