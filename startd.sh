#!/bin/bash
# this script starts the app without the terminal.
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$DIR"
nohup python3 main.py >/dev/null 2>&1 &
exit