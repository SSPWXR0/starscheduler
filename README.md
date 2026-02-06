# StarScheduler

### A Python program to generate and schedule IntelliStar 1/2 presentations with emphasis on maximum control.

StarScheduler is a comprehensive control and scheduling application designed for hobbyists and enthusiasts managing WeatherSTAR and IntelliStar systems. It allows for precise scheduling, manual triggering, and fleet management of various Star systems using multiple protocols.

## Features

*   **Multi-System Support**: Compatible with IntelliStar 1, IntelliStar 2 (HD, Jr, xD) and custom systems.
*   **Flexible Scheduling**: Create events based on minute intervals, 10-minute blocks, hourly, or daily schedules.
*   **Multiple Protocols**:
    *   **SSH**: Standard secure remote control. Leveraging `exec.exe` on the I2 and `runomni` on the I1
    *   **Subprocess**: Local execution (e.g., for `exec.exe` on the I2).
    *   **UDP Multicast**: Experimental support for network ingest via `MsgIngestor` on the I2 and `receiverd` on the I1. It... doesn't actually work.
    *   **Telnet**: Fast, but insecure protocol for use with WeatherStar XL systems (commands currently not implemented), OR I1s and I2s with a telnet server enabled.
*   **Quick Time Event**: Immediately execute specific presentation flavors or commands on one or multiple clients.
*   **Dashboard**: Real-time monitoring of system status, uptime, and next scheduled events.
*   **Modern UI**: Built with PyQt6 with a dark theme connection manager.

## Requirements

*   Python 3.8+ minimum, Python 3.13+ recommended. Some patching required for older versions.
*   See `requirements.txt` for python packages.

**Note on Paramiko**: If using IntelliStar 1 (i1) systems, `paramiko` version must be <= 3.5.1 due to compatibility issues with newer cryptographic defaults.

## Installation

1.  Clone the repository:
    ```bash
    git clone https://github.com/SSPWXR0/starscheduler.git
    cd starscheduler
    ```

2.  Install dependencies:
    ```bash
    pip install -r requirements.txt
    ```

## Usage

Run the main application:

- **All systems:**
  ```bash
  python main.py
  ```
Note: If you have multiple Python versions installed, please install dependencies via `python3.x -m pip install -r requirements.txt`,
Then run the application with `python3.x main.py`  where X is your intended version number.

### Command Line Arguments

*   `--no-gui` (`-n`): Run in headless mode (scheduler only).
*   `--test-outputs` (`-t`): Test connection to all configured output clients and exit.
*   `--force-qt5-compat` (`-q`): Force Qt5 compatibility mode (automatically enabled on Windows 7).

## Configuration

*   **Clients**: Add and configure your Star systems via the "Clients" tab in the GUI. Configuration is saved to `user/config.json`.
*   **Schedule**: Manage events via the "Scheduler" tab. Timetables are saved to `user/timetable.xml`.

## Disclaimer

This software is a hobby project intended for use with legacy/decommissioned hardware. It is not affiliated with The Weather Channel or its parent companies.