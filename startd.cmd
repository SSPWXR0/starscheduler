REM this command starts the app without a terminal window
@echo off
cd /d %~dp0
start "" python main.py
exit /b 0
REM balls