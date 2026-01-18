#!/bin/bash
echo "StarScheduler Dependency Installer"
clear
# we dont have to do anything fancy because windows kinda sucks. assume the user has qt6 support
echo "Installing Paramiko 3.5.1..."
pip install paramiko==3.5.1
echo "Installing Coloredlogs..."
pip install coloredlogs
echo "All dependencies installed."
exit 0