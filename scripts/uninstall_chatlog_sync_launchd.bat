@echo off
setlocal EnableExtensions

set "TARGET_ROOT=%~1"
if not defined TARGET_ROOT set "TARGET_ROOT=%USERPROFILE%\.codex\chat_backup"
set "LINK_WORKSPACE=%~2"
set "TASK_NAME=CodexChatlogSync"

schtasks /End /TN "%TASK_NAME%" >nul 2>nul
schtasks /Delete /TN "%TASK_NAME%" /F >nul 2>nul

if defined LINK_WORKSPACE (
    if exist "%LINK_WORKSPACE%\chat_logs\auto_sessions" rmdir "%LINK_WORKSPACE%\chat_logs\auto_sessions" >nul 2>nul
)

echo Uninstalled: %TASK_NAME%
