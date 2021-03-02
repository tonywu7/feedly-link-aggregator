@echo off
setlocal
setlocal enabledelayedexpansion
echo Installing dependencies...
echo.

python3 -c "exit(65)" > NUL 2>&1
if %ERRORLEVEL% neq 65 (
    echo Error: Python 3 not found.
    echo Install Python 3 from Microsoft Store, or from https://www.python.org/downloads/release/python-386/
    pause
    exit 1
)

echo Installing Twisted...

python3 -m pip install Twisted[windows_platform] > NUL 2>&1

if %ERRORLEVEL% neq 0 (
    echo Error: failed to install Twisted.
    echo Retrying with pre-built binaries...
    echo.

    reg Query "HKLM\Hardware\Description\System\CentralProcessor\0" | find /i "x86" > NUL && set OSVER=32 || set OSVER=_amd64
    for /f "delims=" %%i in ('python3 -c "import sys; v = sys.version_info; ver = \"cp\" + str(v[0]) + str(v[1]); print(ver + \"-\" + ver + \"m\" if ver == \"cp37\" else ver + \"-\" + ver)"') do set PYVER=%%i

    set ERROR=0
    set WHEEL=Twisted-20.3.0-!PYVER!-win!OSVER!.whl
    set URL=https://github.com/tonywu7/feedly-link-aggregator/raw/vendor/_wheels/!WHEEL!
    echo Downloading !URL!
    powershell -Command "(New-Object Net.WebClient).DownloadFile('!URL!', '%CD%\!WHEEL!')"

    if !ERRORLEVEL! neq 0 (
        echo Fatal: No pre-built binary exists for this Python/Windows version.
        set ERROR=1
    )

    if !ERROR! == 0 (
        echo Installing...
        python3 -m pip install "%CD%\!WHEEL!" > NUL 2>&1
    )

    if !ERRORLEVEL! neq 0 (
        echo Fatal: failed to install Twisted using pre-built binaries.
        echo You must compile Twisted yourself.
        echo.
        set ERROR=1
    )

    del "%CD%\!WHEEL!"
    if !ERROR! == 1 (
        pause
        exit 1
    )
)

echo.
echo Installing dependencies...

python3 -m pip install Twisted[windows_platform] > NUL 2>&1
python3 -m pip install -r requirements.txt

echo.
echo Successfully installed dependencies.

echo.
pause
