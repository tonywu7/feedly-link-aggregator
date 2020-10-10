#!/bin/bash
python3 -c 'exit(65)' > /dev/null 2>&1
if [[ $? != 65 ]]; then
    echo 'Error: cannot find Python 3 executable.'
    echo 'You will need at least Python 3.7+ to run this program.'
    echo
    echo 'Install it using your package manager (Homebrew recommended on macOS),'
    echo 'or download it from https://www.python.org/downloads/'
    exit 1
fi

python3 -c 'import scrapy' > /dev/null 2>&1

if [[ $? != 0 ]]; then
    echo 'Error: Dependencies are not installed.'
    while true; do
        read -p 'Would you like to install all dependencies? (yes/no) ' yn
        case $yn in
            [Yy]* ) python3 -m pip install -r requirements.txt; break;;
            [Nn]* ) exit 1;;
            * ) echo "Please answer yes or no.";;
        esac
    done
fi

python3 -m scrapy wizard