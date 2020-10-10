@echo off
setlocal
setlocal enabledelayedexpansion

where "python3" > NUL 2>&1
if %ERRORLEVEL% neq 0 (
    echo Error: Python 3 not found.
    echo Install Python 3 from Microsoft Store, or from https://www.python.org/downloads/release/python-386/
    pause
    exit 1
)

python3 -c "import scrapy" > NUL 2>&1
if %ERRORLEVEL% neq 0 (
    set INSTALL=no
    echo Error: Dependencies are not installed.
    set /p INSTALL=Would you like to install all dependencies? ^(yes/[no]^)^ 
    if /i "!INSTALL:~0,1!" neq "y" exit 1
    call .\install.bat
)

python3 -m scrapy wizard
pause
