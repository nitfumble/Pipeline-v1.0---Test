@echo off
setlocal EnableExtensions
set WSL_UTF8=1

REM Strip the trailing backslash from this script's own folder path -- a
REM trailing backslash right before a closing quote gets misread as an
REM escaped quote by wsl.exe's argument parsing. (This + wsl.exe defaulting
REM to UTF-16 output were the two bugs in the previous version.)
set "HEREDIR=%~dp0"
if "%HEREDIR:~-1%"=="\" set "HEREDIR=%HEREDIR:~0,-1%"

where wsl.exe >nul 2>nul
if errorlevel 1 (
    echo WSL doesn't seem to be installed on this machine.
    echo Open PowerShell as Administrator and run:  wsl --install
    echo Then restart your computer and run this shortcut again.
    pause
    exit /b 1
)

for /f "delims=" %%i in ('wsl.exe wslpath "%HEREDIR%"') do set "WSLPATH=%%i"

if "%WSLPATH%"=="" (
    echo Couldn't translate the project path for WSL. Diagnostic info:
    wsl.exe --status
    pause
    exit /b 1
)

wsl.exe bash -lc "cd '%WSLPATH%' && chmod +x start.sh && ./start.sh"
if errorlevel 1 (
    echo.
    echo Something went wrong starting the server -- see the output above.
    pause
)
