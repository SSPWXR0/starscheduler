title StarScheduler Dependency Installer
@echo off
cls
REM are we on windows 7
ver | find "6.1." > nul
if %errorlevel%==0 (
    echo Detected Windows 7
    echo Installing PyQt5 for Windows 7
    pip install PyQt5
) else (
    pip install PyQt5
)

echo Do you have an IntelliStar? (Y/n)
set /p intellistar=
if /I "%intellistar%" (
    echo I asked you that for no reason. Installing Paramiko 3.5.1 instead of the latest even if you do not need IntelliStar support...
    pip install paramiko==3.5.1
)

echo Installing coloredlogs...
pip install coloredlogs
echo enjoy the terminal rainbow spit lol
echo Finished installing all Python packages.
exit /b 0