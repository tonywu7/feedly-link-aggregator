@echo off
setlocal
echo Installing dependencies...
echo.

where "python3" > NUL 2>&1
if %ERRORLEVEL% neq 0 (
    echo Error: Python 3 not found.
    echo Install Python 3 from Microsoft Store, or from https://www.python.org/downloads/release/python-386/
)

echo Installing Twisted...

python3 -m pip install Twisted[windows_platform]

if %ERRORLEVEL% neq 0 (
    echo Error: failed to install Twisted.
    echo Retrying with pre-built binaries...
    echo.

    reg Query "HKLM\Hardware\Description\System\CentralProcessor\0" | find /i "x86" > NUL && set OSVER=32 || set OSVER=_amd64
    for /f "delims=" %%i in ('python3 -c "import sys; v = sys.version_info; print(\"cp\" + str(v[0]) + str(v[1]))"') do set PYVER=%%i

    set WHEEL=_wheels\Twisted-20.3.0-%PYVER%-%PYVER%-win%OSVER%.whl

    if not exist "%WHEEL%" (
        echo Cannot find %WHEEL%
        echo This Python version is not supported.
        pause
        exit 1
    )

    python3 -m pip install %WHEEL%

    if %ERRORLEVEL% neq 0 (
        echo Fatal: failed to install Twisted using pre-built binaries.
        echo You must compile Twisted yourself.
        echo.
        pause
        exit 1
    )
)

echo.
echo Installing dependencies...

python3 -m pip install -r requirements.txt

echo.
echo Successfully installed dependencies.

echo.
pause
