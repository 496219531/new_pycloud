@echo off
setlocal EnableExtensions

echo === PyCloud service restart ===
echo.

echo [1] Stopping existing services...
for /f "tokens=2 delims=," %%P in ('wmic process where "CommandLine like '%%pycloud_parallel.controlplane.server%%' and not CommandLine like '%%wmic%%'" get ProcessId /format:csv 2^>nul ^| findstr /R "[0-9]"') do (
    taskkill /F /PID %%P >nul 2>nul
)
timeout /t 2 /nobreak >nul

echo [2] Checking ports...
for %%P in (50051 50061 50062) do (
    netstat -ano | findstr /R /C:":%%P .*LISTENING" >nul
    if errorlevel 1 (
        echo   OK port %%P is free
    ) else (
        echo   Port %%P is still in use; waiting...
        timeout /t 2 /nobreak >nul
    )
)
echo.

echo [3] Cleaning temporary logs...
del /Q "%TEMP%\pycloud_*.log" >nul 2>nul
echo   Cleanup complete
echo.

echo [4] Starting services...
start "pycloud-infocenter" /B python -m pycloud_parallel.controlplane.server --role controlplane --bind 0.0.0.0:50051 --log-level INFO > "%TEMP%\pycloud_infocenter.log" 2>&1
start "pycloud-node-1" /B python -m pycloud_parallel.controlplane.server --role nodecontrol --bind 0.0.0.0:50061 --node-id node-1 --worker-capacity 4 --queue-capacity 1000 --service-http-bind 127.0.0.1:18081 --target 127.0.0.1:50051 --advertise-addr 127.0.0.1:50061 --node-tags compute --log-level INFO > "%TEMP%\pycloud_node1.log" 2>&1
start "pycloud-node-2" /B python -m pycloud_parallel.controlplane.server --role nodecontrol --bind 0.0.0.0:50062 --node-id node-2 --worker-capacity 4 --queue-capacity 1000 --service-http-bind 127.0.0.1:18082 --target 127.0.0.1:50051 --advertise-addr 127.0.0.1:50062 --node-tags compute --log-level INFO > "%TEMP%\pycloud_node2.log" 2>&1
echo   Start commands submitted
echo.

echo [5] Waiting for services...
timeout /t 5 /nobreak >nul

echo [6] Verifying InfoCenter...
python -c "import json,urllib.request; data=json.load(urllib.request.urlopen('http://127.0.0.1:50051/nodes', timeout=5)); print('  InfoCenter OK'); print('  Registered nodes:', len(data.get('nodes', [])))"
if errorlevel 1 echo   InfoCenter did not respond
echo.

echo === Restart complete ===
echo Logs:
echo   InfoCenter: %TEMP%\pycloud_infocenter.log
echo   Node-1:     %TEMP%\pycloud_node1.log
echo   Node-2:     %TEMP%\pycloud_node2.log
