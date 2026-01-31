import os
import random
import sys
import asyncio
import uuid
import hashlib
from tkinter import font
import coloredlogs, logging
import paramiko
import platform
import re
from io import StringIO
import provision
import json
import argparse
import xml.etree.ElementTree as ET
import atexit
import threading
import time
import queue
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from datetime import datetime, timedelta
from typing import Optional, Any, Dict, List, Set, Callable

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.events import (
    EVENT_JOB_EXECUTED, EVENT_JOB_ERROR, EVENT_JOB_MISSED,
    JobExecutionEvent
)
try:
    from tzlocal import get_localzone
    local_timezone = get_localzone()
except ImportError:
    local_timezone = None

if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

def generate_action_id(data: Dict) -> str:
    parts = []
    keys = ['client_id', 'action', 'flavor', 'presentation_id', 'duration', 'logo', 'command', 'su', 'ldl_state']
    for k in keys:
        val = str(data.get(k, '')).strip()
        parts.append(f"{k}:{val}")
    s = "|".join(parts)
    return hashlib.md5(s.encode('utf-8')).hexdigest()

def get_optimal_thread_count(max_threads: int = 4, scale_factor: float = 0.5) -> int:
    try:
        cpu_count = os.cpu_count() or 2
        optimal = int(min(cpu_count * scale_factor, max_threads))
        return max(1, optimal)
    except Exception:
        return 2

_perf_config = {
    'maxThreads': 4,
    'schedulerPollIntervalMs': 100,
    'cacheUpdateIntervalSec': 5
}

def load_performance_config(config: dict) -> None:
    global _perf_config
    perf = config.get('system', {}).get('performance', {})
    _perf_config.update({
        'maxThreads': perf.get('maxThreads', 4),
        'schedulerPollIntervalMs': perf.get('schedulerPollIntervalMs', 100),
        'cacheUpdateIntervalSec': perf.get('cacheUpdateIntervalSec', 5)
    })
    provision.configure_executor(_perf_config['maxThreads'])
    logger.info(f"Performance config loaded: maxThreads={_perf_config['maxThreads']}, "
                f"pollInterval={_perf_config['schedulerPollIntervalMs']}ms")

def get_perf_config() -> dict:
    return _perf_config.copy()

connected_outputs = 0
connected_outputs_data = []
output_widget = None
original_stdout = sys.stdout
original_stderr = sys.stderr
window_width = 1396
window_height = 960

STAR_NAMES = {
    "i1": "IntelliStar 1",
    "i2hd": "IntelliStar 2 HD",
    "i2jr": "IntelliStar 2 Jr",
    "i2xd": "IntelliStar 2 XD",
    "wsxl": "WeatherSTAR XL",
    "ws4000": "WeatherSTAR 4000",
    "custom": "Custom System"
}
def load_qt_modules():
    global QtWidgets, QtGui, QtCore
    force_qt5 = '-q' in sys.argv or '--force-qt5-compat' in sys.argv
    if platform.system() == 'Windows' and platform.release() == '7':
        force_qt5 = True
        
    if force_qt5:
        try:
            import PyQt5.QtWidgets as QtWidgets
            import PyQt5.QtGui as QtGui
            from PyQt5.QtGui import QFontDatabase, QFont
            import PyQt5.QtCore as QtCore
            return
        except ImportError:
            pass

    try:
        import PyQt6.QtWidgets as QtWidgets
        import PyQt6.QtGui as QtGui
        from PyQt6.QtGui import QFontDatabase, QFont
        import PyQt6.QtCore as QtCore
    except ImportError:
        try:
            import PyQt5.QtWidgets as QtWidgets
            import PyQt5.QtGui as QtGui
            from PyQt5.QtGui import QFontDatabase, QFont
            import PyQt5.QtCore as QtCore
        except ImportError:
            print("CRITICAL: Failed to load PyQt6 or PyQt5.")
            print("Do you even have this installed? :/")
            sys.exit(1)

load_qt_modules()

logger = logging.getLogger("starscheduler")
coloredlogs.install(level='DEBUG', logger=logger)

if paramiko.__version__ > '3.5.1':
    logger.warning("Paramiko version is higher than 3.5.1. This will NOT be compatible with the IntelliStar 1 (i1) system!")

if platform.system() == 'Windows' and platform.release() == '7':
    os.environ['PARAMIKO_USE_LEGACY_RNG'] = 'yes'
    logger.info("Windows 7 detected. Using legacy RNG for Paramiko. Using QT5 for GUI.")

dark_stylesheet = """
    QMainWindow {
        background-color: #1e1e1e;
        color: #ffffff;
        font-family: 'Host Grotesk', sans-serif;
    }
    QPushButton {
        background-color: #1c395c;
        border: 1px solid #87CEEB;
        color: #e0e0e0;
        border-radius: 4px;
        font-size: 12px;
        }
    QPushButton:hover { background-color: #143048; }
    QWidget {
        background-color: #1e1e1e;
        color: #ffffff;
        font-family: 'Host Grotesk', sans-serif;
    }
    QTabWidget::pane {
            border: 1px solid #404040;
        }
    QTabBar::tab {
                background-color: #2d2d2d;
                color: #ffffff;
                padding: 5px 15px;
                border: 1px solid #404040;
            }
            QTabBar::tab:selected {
                background-color: #404040;
                color: #ffffff;
            }
            QLabel {
                color: #ffffff;
            }
            QLineEdit {
                background-color: #2d2d2d;
                color: #ffffff;
                border: 2px solid #404040;
                padding: 5px;
            }
            QPushButton {
                background-color: #404040;
                color: #ffffff;
                border: 4px solid #505050;
                padding: 5px 15px;
                border-radius: 3px;
            }
            QPushButton:hover {
                background-color: #505050;
            }
            QPushButton:pressed {
                background-color: #303030;
            }
            QScrollBar:vertical {
                border: none;
                background: #1e1e1e;
                width: 10px;
                margin: 0px 0px 0px 0px;
            }
            QScrollBar::handle:vertical {
                background: #404040;
                min-height: 20px;
                border-radius: 5px;
            }
            QScrollBar::handle:vertical:hover {
                background: #505050;
            }
            QScrollBar::add-line:vertical {
                height: 0px;
                subcontrol-position: bottom;
                subcontrol-origin: margin;
            }
            QScrollBar::sub-line:vertical {
                height: 0px;
                subcontrol-position: top;
                subcontrol-origin: margin;
            }
            QScrollBar:horizontal {
                border: none;
                background: #1e1e1e;
                height: 10px;
                margin: 0px 0px 0px 0px;
            }
            QScrollBar::handle:horizontal {
                background: #404040;
                min-width: 20px;
                border-radius: 5px;
            }
            QScrollBar::handle:horizontal:hover {
                background: #505050;
            }
            QScrollBar::add-line:horizontal {
                width: 0px;
                subcontrol-position: right;
                subcontrol-origin: margin;
            }
            QScrollBar::sub-line:horizontal {
                width: 0px;
                subcontrol-position: left;
                subcontrol-origin: margin;
            }
        """

class ConnectionThread(QtCore.QThread):
    def __init__(self, controller, scheduler=None):
        super().__init__()
        self.controller = controller
        self.scheduler = scheduler
        self.loop = None
        self._stop_requested = False

    def run(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        try:
            tasks = [self.controller.get_all_output_clients()]
            self.loop.run_until_complete(asyncio.gather(*tasks))
        except Exception as e:
            logger.error(f"ConnectionThread error: {e}")
        finally:
            try:
                pending = asyncio.all_tasks(self.loop)
                for task in pending:
                    task.cancel()
                if pending:
                    self.loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            except Exception as e:
                logger.debug(f"Error during ConnectionThread cleanup: {e}")
            finally:
                self.loop.close()
                self.loop = None
    
    def stop(self):
        self._stop_requested = True
        if self.loop and self.loop.is_running():
            self.loop.call_soon_threadsafe(self.loop.stop)

class OutputCapture(QtCore.QObject):
    text_written = QtCore.pyqtSignal(str)
    def __init__(self, is_stderr=False):
        super().__init__()
        self.is_stderr = is_stderr
        self.original = original_stderr if is_stderr else original_stdout
    def write(self, text):
        self.original.write(text)
        self.text_written.emit(text)
    def flush(self):
        self.original.flush()
    def isatty(self):
        return True

def ansi_to_html(text):
    ANSI_COLORS = {
        '30': "#5A5A5A", '31': '#ff5555', '32': '#50fa7b', '33': '#f1fa8c',
        '34': '#bd93f9', '35': '#ff79c6', '36': '#8be9fd', '37': '#f8f8f2',
        '90': '#6272a4', '91': '#ff6e6e', '92': '#69ff94', '93': '#ffffa5',
        '94': '#d6acff', '95': '#ff92df', '96': '#a4ffff', '97': '#ffffff',
    }

    text = text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    
    parts = re.split(r'\x1b\[([0-9;]*)m', text)
    
    html = []
    current_style = []

    for i in range(0, len(parts), 2):
        content = parts[i]
        if content:
             style_str = ";".join(current_style)
             if style_str:
                 html.append(f'<span style="{style_str}">{content}</span>')
             else:
                 html.append(content)
        
        if i + 1 < len(parts):
            code_seq = parts[i+1]
            codes = code_seq.split(';')
            for code in codes:
                if code == '0' or code == '':
                    current_style = []
                elif code == '1':
                    if "font-weight:bold" not in current_style:
                        current_style.append("font-weight:bold")
                elif code in ANSI_COLORS:
                    current_style = [s for s in current_style if not s.startswith("color:")]
                    current_style.append(f"color:{ANSI_COLORS[code]}")
    return "".join(html).replace("\n", "<br>")

class ClientDialog(QtWidgets.QDialog):
    def __init__(self, parent=None, client_data=None):
        super().__init__(parent)
        self.client_data = client_data or {}
        self.mode = "Edit" if self.client_data else "Add"
        self.setWindowTitle(f"{self.mode} Output Client")
        self.setModal(True)
        self.resize(550, 650)
        self._setup_ui()
        self._populate_fields()

    def _setup_ui(self):
        self.setStyleSheet("""
            QDialog {
                background-color: #1e1e1e;
            }
            QLabel {
                color: #e0e0e0;
                font-size: 13px;
                font-weight: 500;
            }
            QLineEdit, QComboBox {
                background-color: #2b2b2b;
                color: #ffffff;
                border: 1px solid #3e3e3e;
                padding: 6px;
                border-radius: 4px;
                font-size: 13px;
            }
            QLineEdit:focus, QComboBox:focus {
                border: 1px solid #007acc;
                background-color: #323232;
            }
            QGroupBox {
                border: 1px solid #3e3e3e;
                border-radius: 6px;
                margin-top: 24px;
                font-size: 14px;
                font-weight: bold;
                color: #aaa;
            }
            QGroupBox::title {
                subcontrol-origin: margin;
                subcontrol-position: top left;
                padding: 0 10px;
                left: 10px;
            }
        """)
        
        layout = QtWidgets.QVBoxLayout(self)
        layout.setSpacing(15)
        layout.setContentsMargins(20, 20, 20, 20)
        
        header_lbl = QtWidgets.QLabel(f"{self.mode} Client System")
        header_lbl.setStyleSheet("font-size: 18px; font-weight: bold; color: white; margin-bottom: 5px;")
        layout.addWidget(header_lbl)
        self.form_widget = QtWidgets.QWidget()
        self.form = QtWidgets.QFormLayout(self.form_widget)
        self.form.setVerticalSpacing(10)
        self.form.setHorizontalSpacing(15)
        self.form.setContentsMargins(0, 0, 0, 0)
        self.star_type_combo = QtWidgets.QComboBox()
        valid_types = [k for k in sorted(STAR_NAMES.keys()) if k not in ["wsxl", "ws4000"]]
        for k in valid_types:
            self.star_type_combo.addItem(f"{STAR_NAMES[k]} ({k})", k)
        self.star_type_combo.currentTextChanged.connect(self._on_type_changed)
        self.form.addRow("System Type:", self.star_type_combo)
        self.protocol_combo = QtWidgets.QComboBox()
        self.protocol_combo.addItem("SSH (Standard)", "ssh")
        self.protocol_combo.addItem("Subprocess (Local)", "subprocess")
        self.protocol_combo.addItem("UDP Multicast (Experimental)", "udp")
        self.protocol_combo.addItem("Telnet (Experimental)", "telnet")
        self.protocol_combo.currentIndexChanged.connect(self._on_protocol_changed)
        self.form.addRow("Connection Protocol:", self.protocol_combo)
        layout.addWidget(self.form_widget)
        self.conn_group = QtWidgets.QGroupBox("Connection Details")
        self.conn_layout = QtWidgets.QFormLayout(self.conn_group)
        self.conn_layout.setVerticalSpacing(10)
        self.conn_layout.setHorizontalSpacing(15)
        self.conn_layout.setContentsMargins(15, 20, 15, 15)
        self.host_edit = QtWidgets.QLineEdit()
        self.host_edit.setPlaceholderText("e.g. 192.168.1.50")
        self.conn_layout.addRow("Hostname / IP:", self.host_edit)
        self.port_edit = QtWidgets.QLineEdit()
        self.conn_layout.addRow("Port:", self.port_edit)
        self.user_edit = QtWidgets.QLineEdit()
        self.user_edit.setPlaceholderText("SSH Username")
        self.conn_layout.addRow("Username:", self.user_edit)
        self.pass_edit = QtWidgets.QLineEdit()
        self.pass_edit.setEchoMode(QtWidgets.QLineEdit.EchoMode.Password)
        self.pass_edit.setPlaceholderText("SSH Password")
        self.conn_layout.addRow("Password:", self.pass_edit)
        self.su_edit = QtWidgets.QLineEdit()
        self.su_edit.setPlaceholderText("e.g. dgadmin (Required for i1)")
        self.conn_layout.addRow("Substitute User (su):", self.su_edit)
        layout.addWidget(self.conn_group)
        layout.addStretch()

        footnote_text = (
            "- Subprocess can be used with exec.exe, which is available on all IntelliStar 2 systems\n"
            "- SSH is supported on all Star systems.\n"
            "- Although UDP communication is currently experimental, it should be able to work on all Star systems.\n"
            "- Telnet is supported on WeatherStar XL, IntelliStar, IntelliStar 2s running Windows 7 or a third-party Telnet server otherwise."
        )
        footnote_label = QtWidgets.QLabel(footnote_text)
        footnote_label.setStyleSheet("color: #888; font-size: 11px; font-style: italic; margin: 10px 0;")
        footnote_label.setWordWrap(True)
        layout.addWidget(footnote_label)
        btn_layout = QtWidgets.QHBoxLayout()
        btn_layout.addStretch()
        self.cancel_btn = QtWidgets.QPushButton("Cancel")
        self.cancel_btn.setCursor(QtCore.Qt.CursorShape.PointingHandCursor)
        self.cancel_btn.clicked.connect(self.reject)
        
        self.save_btn = QtWidgets.QPushButton("Save Configuration")
        self.save_btn.setCursor(QtCore.Qt.CursorShape.PointingHandCursor)
        self.save_btn.clicked.connect(self.accept)
        
        btn_layout.addWidget(self.cancel_btn)
        btn_layout.addWidget(self.save_btn)
        layout.addLayout(btn_layout)

    def _on_type_changed(self):
        self._on_protocol_changed()

    def _on_protocol_changed(self):
        proto = self.protocol_combo.currentData()
        star = self.star_type_combo.currentData()
        is_ssh = (proto == "ssh")
        is_udp = (proto == "udp")
        is_sub = (proto == "subprocess")
        is_telnet = (proto == "telnet")
        
        has_creds = is_ssh or is_telnet
        
        self.conn_group.setVisible(not is_sub)
        if not is_sub:
            self.user_edit.setVisible(has_creds)
            self.conn_layout.labelForField(self.user_edit).setVisible(has_creds)
            self.pass_edit.setVisible(has_creds)
            self.conn_layout.labelForField(self.pass_edit).setVisible(has_creds)
            is_i1 = (star == "i1")
            show_su = is_i1 and has_creds
            self.su_edit.setVisible(show_su)
            if self.conn_layout.labelForField(self.su_edit):
                self.conn_layout.labelForField(self.su_edit).setVisible(show_su)
            current_port = self.port_edit.text()
            if is_udp and (current_port == "22" or current_port == "23" or not current_port):
                self.port_edit.setText("7787")
            elif is_telnet and (current_port == "22" or current_port == "7787" or not current_port):
                self.port_edit.setText("23")
            elif is_ssh and (current_port == "7787" or current_port == "23" or not current_port):
                self.port_edit.setText("22")
            if is_udp and not self.host_edit.text():
                self.host_edit.setText("224.1.1.77")

    def _populate_fields(self):
        self._on_type_changed()
        self._on_protocol_changed()
        if not self.client_data:
            return
        star = self.client_data.get('star', 'i2')
        idx = self.star_type_combo.findData(star)
        if idx >= 0:
            self.star_type_combo.setCurrentIndex(idx)
            
        proto = self.client_data.get('protocol', 'ssh')
        idx = self.protocol_combo.findData(proto)
        if idx >= 0:
            self.protocol_combo.setCurrentIndex(idx)
            
        creds = self.client_data.get('credentials', {})
        self.host_edit.setText(creds.get('hostname', ''))
        self.port_edit.setText(str(creds.get('port', 22)))
        self.user_edit.setText(creds.get('user', ''))
        self.pass_edit.setText(creds.get('password', ''))
        self.su_edit.setText(creds.get('su') or '')

    def get_data(self):
        star = self.star_type_combo.currentData()
        host = self.host_edit.text()
        if self.client_data and 'id' in self.client_data:
            cid = self.client_data['id']
        else:
            cid = f"{star}_{host.replace('.', '_')}"
        return {
            "id": cid,
            "star": star,
            "displayName": STAR_NAMES.get(star, "Unknown"),
            "protocol": self.protocol_combo.currentData(),
            "credentials": {
                "hostname": host,
                "port": int(self.port_edit.text()) if self.port_edit.text().isdigit() else 22,
                "user": self.user_edit.text(),
                "password": self.pass_edit.text(),
                "su": self.su_edit.text() if self.su_edit.text() else None
            }
        }

class ClientActionCard(QtWidgets.QFrame):
    delete_clicked = QtCore.pyqtSignal()
    
    def __init__(self, parent=None, clients=None, client_id=None, config=None, controller=None):
        super().__init__(parent)
        self.clients = clients or []
        self.config = config or {}
        self.controller = controller
        
        self.setObjectName("ActionCard")
        self.setStyleSheet("""
            QFrame#ActionCard {
                background-color: #242424;
                border: 1px solid #383838;
                border-radius: 6px;
            }
            QLabel {
                border: none;
                color: #bbb;
                font-weight: 500;
                font-size: 13px;
            }
            QComboBox, QLineEdit {
                background-color: #181818;
                border: 1px solid #404040;
                border-radius: 3px;
                padding: 4px 8px;
                color: #f0f0f0;
                font-size: 13px;
            }
            QComboBox:hover, QLineEdit:hover {
                border: 1px solid #555;
            }
            QComboBox:focus, QLineEdit:focus {
                border: 1px solid #4a90e2;
                background-color: #111;
            }
        """)
        
        self.layout_main = QtWidgets.QVBoxLayout(self)
        self.layout_main.setSpacing(2)
        self.layout_main.setContentsMargins(5, 5, 5, 5)
        self.setMaximumHeight(120)
        self.setMinimumHeight(32)
        self.setup_header(client_id)
        self.param_stack = QtWidgets.QStackedWidget()
        self.param_stack.setStyleSheet("border: none; background: transparent;")
        self.layout_main.addWidget(self.param_stack)
        self.setup_param_pages()
        self.setup_offset_row()
        self.set_initial_state()

    def setup_header(self, client_id):
        header_layout = QtWidgets.QHBoxLayout()
        header_layout.setContentsMargins(0, 0, 0, 0)
        header_layout.setSpacing(4)
        header_layout.addWidget(QtWidgets.QLabel("Client:"))
        self.client_combo = QtWidgets.QComboBox()
        self.setMaximumHeight(120)
        self.setMinimumHeight(32)
        self.client_combo.setSizePolicy(QtWidgets.QSizePolicy.Policy.Expanding, QtWidgets.QSizePolicy.Policy.Fixed)
        self.client_combo.setFixedHeight(24)
        sorted_clients = sorted(self.clients, key=lambda x: x.get('displayName') or x.get('id', ''))
        
        idx_to_select = 0
        for i, c in enumerate(sorted_clients):
            c_name = c.get('displayName') or c.get('id', '')
            self.client_combo.addItem(c_name, c)
            if c.get('id') == client_id:
                idx_to_select = i
                
        self.client_combo.setCurrentIndex(idx_to_select)
        self.client_combo.currentIndexChanged.connect(self.on_client_changed)
        header_layout.addWidget(self.client_combo)
        self.action_combo = QtWidgets.QComboBox()
        self.action_combo.setMinimumWidth(120)
        self.action_combo.setFixedHeight(24)
        self.action_combo.setSizePolicy(QtWidgets.QSizePolicy.Policy.Expanding, QtWidgets.QSizePolicy.Policy.Fixed)
        self.action_combo.currentTextChanged.connect(self.on_action_changed)
        header_layout.addWidget(self.action_combo)
        header_layout.addStretch()
        del_btn = QtWidgets.QPushButton("x")
        del_btn.setFixedSize(32, 32)
        del_btn.setCursor(QtCore.Qt.CursorShape.PointingHandCursor)
        del_btn.setToolTip("Remove Action")
        del_btn.clicked.connect(self.delete_clicked.emit)
        header_layout.addWidget(del_btn)
        self.layout_main.addLayout(header_layout)

    def on_client_changed(self):
        self.update_action_options()

    def update_action_options(self):
        data = self.client_combo.currentData()
        if not data: return
        star = data.get('star', 'i2').lower()
        is_i1 = ('i1' in star) and not ('i2' in star)
        current = self.action_combo.currentText()
        self.action_combo.blockSignals(True)
        self.action_combo.clear()
        if is_i1:
            self.action_combo.addItems([
                "LoadRun", "LDL (On/Off)", "Custom Command"
            ])
        else:
            self.action_combo.addItems([
                "LoadRun", "Cancel", "Custom Command"
            ])
        idx = self.action_combo.findText(current)
        if idx >= 0:
            self.action_combo.setCurrentIndex(idx)
        else:
            self.action_combo.setCurrentIndex(0)
        self.action_combo.blockSignals(False)
        self.on_action_changed(self.action_combo.currentText())

    def setup_param_pages(self):
        self.param_stack.addWidget(QtWidgets.QWidget()) 
        self.page_i2_load = QtWidgets.QWidget()
        self.ui_i2_load = {}
        lay = QtWidgets.QGridLayout(self.page_i2_load)
        lay.setContentsMargins(0,0,0,0)
        lay.setSpacing(4)
        lay.setColumnStretch(1, 1)
        lay.setColumnStretch(3, 1)
        l = QtWidgets.QLabel("Flavor:")
        l.setStyleSheet("color: #bbb; font-size: 11px;")
        lay.addWidget(l, 0, 0)
        e = QtWidgets.QLineEdit()
        e.setFixedHeight(24)
        self.ui_i2_load["flavor"] = e
        lay.addWidget(e, 0, 1)
        l = QtWidgets.QLabel("PresID:")
        l.setStyleSheet("color: #bbb; font-size: 11px;")
        lay.addWidget(l, 0, 2)
        e = QtWidgets.QLineEdit()
        e.setFixedHeight(24)
        self.ui_i2_load["pres_id"] = e
        lay.addWidget(e, 0, 3)
        l = QtWidgets.QLabel("Dur (s):")
        l.setStyleSheet("color: #bbb; font-size: 11px;")
        lay.addWidget(l, 1, 0)
        e = QtWidgets.QLineEdit()
        e.setFixedHeight(24)
        self.ui_i2_load["duration"] = e
        lay.addWidget(e, 1, 1)
        l = QtWidgets.QLabel("Logo:")
        l.setStyleSheet("color: #bbb; font-size: 11px;")
        lay.addWidget(l, 1, 2)
        e = QtWidgets.QLineEdit()
        e.setFixedHeight(24)
        self.ui_i2_load["logo"] = e
        lay.addWidget(e, 1, 3)
        self.param_stack.addWidget(self.page_i2_load)
        self.page_i1_load = QtWidgets.QWidget()
        self.ui_i1_load = {}
        lay = QtWidgets.QHBoxLayout(self.page_i1_load)
        lay.setContentsMargins(0,0,0,0)
        lay.setSpacing(10)
        self.add_field(lay, "Flavor:", "flavor", self.ui_i1_load)
        lay.addStretch()
        self.param_stack.addWidget(self.page_i1_load)
        self.page_custom = QtWidgets.QWidget()
        self.ui_custom = {}
        lay = QtWidgets.QHBoxLayout(self.page_custom)
        lay.setContentsMargins(0,0,0,0)
        lay.setSpacing(10)
        self.add_field(lay, "Command:", "command", self.ui_custom, width=250)
        self.add_field(lay, "SU User:", "su", self.ui_custom)
        self.param_stack.addWidget(self.page_custom)
        
        self.page_cancel = QtWidgets.QWidget()
        self.ui_cancel = {}
        lay = QtWidgets.QHBoxLayout(self.page_cancel)
        lay.setContentsMargins(0,0,0,0)
        lay.setSpacing(10)
        self.add_field(lay, "PresID:", "pres_id", self.ui_cancel)
        lay.addStretch()
        self.param_stack.addWidget(self.page_cancel)

        self.page_ldl = QtWidgets.QWidget()
        self.ui_ldl = {}
        lay = QtWidgets.QHBoxLayout(self.page_ldl)
        lay.setContentsMargins(0,0,0,0)
        lay.setSpacing(10)
        lay.addWidget(QtWidgets.QLabel("State:"))
        self.ui_ldl['state'] = QtWidgets.QComboBox()
        self.ui_ldl['state'].addItems(["On (1)", "Off (0)"])
        lay.addWidget(self.ui_ldl['state'])
        lay.addStretch()
        self.param_stack.addWidget(self.page_ldl)

    def setup_offset_row(self):
        offset_layout = QtWidgets.QHBoxLayout()
        offset_layout.setContentsMargins(0, 0, 0, 0)
        offset_layout.setSpacing(8)
        
        self.separate_chk = QtWidgets.QCheckBox("Send separate Load/Run?")
        self.separate_chk.setStyleSheet("color: #bbb; font-size: 11px;")
        self.separate_chk.stateChanged.connect(self._on_separate_changed)
        offset_layout.addWidget(self.separate_chk)
        
        self.load_offset_label = QtWidgets.QLabel("Load Offset (s):")
        self.load_offset_label.setStyleSheet("color: #888; font-size: 11px;")
        self.load_offset_label.setVisible(False)
        offset_layout.addWidget(self.load_offset_label)
        
        self.load_offset_spin = QtWidgets.QSpinBox()
        self.load_offset_spin.setRange(-59, 59)
        self.load_offset_spin.setValue(-20)
        self.load_offset_spin.setFixedWidth(60)
        self.load_offset_spin.setFixedHeight(22)
        self.load_offset_spin.setVisible(False)
        offset_layout.addWidget(self.load_offset_spin)
        
        self.run_offset_label = QtWidgets.QLabel("Run Offset (s):")
        self.run_offset_label.setStyleSheet("color: #888; font-size: 11px;")
        self.run_offset_label.setVisible(False)
        offset_layout.addWidget(self.run_offset_label)
        
        self.run_offset_spin = QtWidgets.QSpinBox()
        self.run_offset_spin.setRange(-59, 59)
        self.run_offset_spin.setValue(-12)
        self.run_offset_spin.setFixedWidth(60)
        self.run_offset_spin.setFixedHeight(22)
        self.run_offset_spin.setVisible(False)
        offset_layout.addWidget(self.run_offset_spin)
        
        offset_layout.addStretch()
        self.layout_main.addLayout(offset_layout)
        
    def _on_separate_changed(self, state):
        if hasattr(QtCore.Qt.CheckState, 'Checked'):
            checked_val = QtCore.Qt.CheckState.Checked
            if hasattr(checked_val, 'value'):
                checked_val = checked_val.value
        else:
            checked_val = QtCore.Qt.Checked
        visible = (state == checked_val) if isinstance(state, int) else (state == QtCore.Qt.CheckState.Checked)
        self.load_offset_label.setVisible(visible)
        self.load_offset_spin.setVisible(visible)
        self.run_offset_label.setVisible(visible)
        self.run_offset_spin.setVisible(visible)

    def add_field(self, layout, label, key, store, width=None):
        lbl = QtWidgets.QLabel(label)
        lbl.setStyleSheet("color: #bbb; margin-right: 2px; font-size: 11px;")
        layout.addWidget(lbl)
        le = QtWidgets.QLineEdit()
        le.setFixedHeight(24)
        if width: le.setMinimumWidth(width)
        store[key] = le
        layout.addWidget(le)

    def on_action_changed(self, action):
        data = self.client_combo.currentData()
        star = data.get('star', 'i2').lower() if data else 'i2'
        is_i1 = ('i1' in star) and not ('i2' in star)
        
        show_offset = (action == "LoadRun")
        self.separate_chk.setVisible(show_offset)
        if not show_offset:
            self.separate_chk.setChecked(False)
        
        if action == "Custom Command":
            self.param_stack.setCurrentWidget(self.page_custom)
        elif is_i1:
            if action == "LDL (On/Off)":
                self.param_stack.setCurrentWidget(self.page_ldl)
            else:
                self.param_stack.setCurrentWidget(self.page_i1_load)
        else:
            if action == "LoadRun":
                self.param_stack.setCurrentWidget(self.page_i2_load)
            elif action == "Cancel":
                self.param_stack.setCurrentWidget(self.page_cancel)
            else:
                self.param_stack.setCurrentWidget(self.param_stack.widget(0))

    def set_initial_state(self):
        self.update_action_options()
        action = self.config.get('action', 'LoadRun')
        if action in ('Load', 'Run'):
            action = 'LoadRun'
        if not self.config.get('action') and self.config.get('flavor'):
             action = "LoadRun"
        idx = self.action_combo.findText(action)
        if idx >= 0:
            self.action_combo.setCurrentIndex(idx)
        if 'flavor' in self.ui_i2_load: self.ui_i2_load['flavor'].setText(self.config.get('flavor', ''))
        if 'pres_id' in self.ui_i2_load: self.ui_i2_load['pres_id'].setText(self.config.get('presentation_id', '1'))
        if 'duration' in self.ui_i2_load: self.ui_i2_load['duration'].setText(self.config.get('duration', '60'))
        if 'logo' in self.ui_i2_load: self.ui_i2_load['logo'].setText(self.config.get('logo', ''))
        if 'flavor' in self.ui_i1_load: self.ui_i1_load['flavor'].setText(self.config.get('flavor', ''))
        if 'command' in self.ui_custom: self.ui_custom['command'].setText(self.config.get('command', ''))
        if 'su' in self.ui_custom: self.ui_custom['su'].setText(self.config.get('su', ''))
        if 'pres_id' in self.ui_cancel: self.ui_cancel['pres_id'].setText(self.config.get('presentation_id', '1'))
        if 'state' in self.ui_ldl:
            val = self.config.get('ldl_state', '1')
            self.ui_ldl['state'].setCurrentIndex(0 if val == '1' else 1)

        self.load_offset_spin.setValue(int(self.config.get('load_offset', -20)))
        self.run_offset_spin.setValue(int(self.config.get('run_offset', -12)))
        
        separate = self.config.get('separate_load_run', False)
        self.separate_chk.setChecked(separate)

    def get_config(self):
        client_data = self.client_combo.currentData()
        if not client_data: return None
        cid = client_data.get('id')
        action = self.action_combo.currentText()
        cfg = {
            'action': action,
            'flavor': '', 'presentation_id': '', 'duration': '', 'logo': '', 
            'command': '', 'su': '', 'ldl_state': '',
            'separate_load_run': False, 'load_offset': -20, 'run_offset': -12
        }
        w = self.param_stack.currentWidget()
        if w == self.page_i2_load:
            cfg['flavor'] = self.ui_i2_load['flavor'].text()
            cfg['presentation_id'] = self.ui_i2_load['pres_id'].text()
            cfg['duration'] = self.ui_i2_load['duration'].text()
            cfg['logo'] = self.ui_i2_load['logo'].text()
        elif w == self.page_i1_load:
            cfg['flavor'] = self.ui_i1_load['flavor'].text()
        elif w == self.page_custom:
            cfg['command'] = self.ui_custom['command'].text()
            cfg['su'] = self.ui_custom['su'].text()
        elif w == self.page_cancel:
            cfg['presentation_id'] = self.ui_cancel['pres_id'].text()
        elif w == self.page_ldl:
            cfg['ldl_state'] = '1' if self.ui_ldl['state'].currentIndex() == 0 else '0'
        
        if action == "LoadRun" and self.separate_chk.isChecked():
            cfg['separate_load_run'] = True
            cfg['load_offset'] = self.load_offset_spin.value()
            cfg['run_offset'] = self.run_offset_spin.value()
        
        return cid, cfg

    def set_data(self, data):
        self.config = data
        cid = data.get('client_id')
        idx = -1
        for i in range(self.client_combo.count()):
             c = self.client_combo.itemData(i)
             if c and c.get('id') == cid:
                 idx = i
                 break
        if idx >= 0:
            self.client_combo.setCurrentIndex(idx)
        self.set_initial_state()

    def get_data(self):
        res = self.get_config()
        if not res: return {}
        cid, cfg = res
        cfg['client_id'] = cid
        if self.config.get('action_guid'):
            cfg['action_guid'] = self.config.get('action_guid')
        return cfg

class ClientCard(QtWidgets.QFrame):
    def __init__(self, client, edit_callback=None, delete_callback=None, parent=None):
        super().__init__(parent)
        self.client = client
        self.edit_callback = edit_callback
        self.delete_callback = delete_callback
        self.protocol = client.get("protocol", "IP over Avian Carriers")
        self.port = client.get("credentials", {}).get("port", 22)
        self.star_type = client.get("star", "unknown")
        self.user = client.get("credentials", {}).get("user", "rai")
        creds = client.get("credentials") or {}
        self.hostname = creds.get("hostname", "unknown")
        self.port = creds.get("port", 22)
        self.last_seen = "Never"
        self.setObjectName("CardFrame")
        self.setFixedHeight(82)
        self._setup_ui()
        self.update_status()

    def _setup_ui(self):
        self.setStyleSheet("""
            QFrame#CardFrame {
                background-color: #2b2d31;
                border: 1px solid #87CEEB;
                border-radius: 6px;
            }
            QFrame#CardFrame:hover {
                background-color: #323639;
                border: 1px solid #87CEEB;
            }
            QLabel {
                background: transparent;
            }
        """)
        card_layout = QtWidgets.QHBoxLayout()
        card_layout.setContentsMargins(15, 8, 15, 8)
        card_layout.setSpacing(18)
        icon_path = os.path.join(os.path.dirname(__file__), "img", f"{self.star_type}.png")
        icon_label = QtWidgets.QLabel()
        icon_label.setFixedSize(48, 48)
        icon_label.setStyleSheet("border: none; background: transparent;")
        if os.path.exists(icon_path):
             pixmap = QtGui.QPixmap(icon_path)
             if not pixmap.isNull():
                scaled = pixmap.scaled(48, 48, QtCore.Qt.AspectRatioMode.KeepAspectRatio, QtCore.Qt.TransformationMode.SmoothTransformation)
                icon_label.setPixmap(scaled)
        else:
             icon_label.setText("?")
             icon_label.setStyleSheet("color: #cccccc; font-size: 24px; font-weight: bold; border: none; background: transparent;")
        icon_label.setAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)
        icon_container = QtWidgets.QFrame()
        icon_container.setFixedSize(64, 64)
        icon_container.setStyleSheet("""
            background-color: #1f1f1f;
            border-radius: 8px;
            border: 1px solid #333;
        """)
        icon_container_layout = QtWidgets.QVBoxLayout()
        icon_container_layout.setContentsMargins(0,0,0,0)
        icon_container_layout.setAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)
        icon_container_layout.addWidget(icon_label)
        icon_container.setLayout(icon_container_layout)
        card_layout.addWidget(icon_container)
        info_layout = QtWidgets.QVBoxLayout()
        info_layout.setSpacing(2)
        info_layout.setAlignment(QtCore.Qt.AlignmentFlag.AlignVCenter)
        name_row = QtWidgets.QHBoxLayout()
        name_row.setSpacing(8)
        name_row.setAlignment(QtCore.Qt.AlignmentFlag.AlignLeft)
        name_lbl = QtWidgets.QLabel(STAR_NAMES.get(self.star_type, f"System {self.star_type}"))
        name_lbl.setStyleSheet("font-size: 15px; font-weight: 700; color: #e1e1e1; border: none; background: transparent;")
        name_row.addWidget(name_lbl)
        proto_lbl = QtWidgets.QLabel(f"{self.protocol.upper()}")
        proto_lbl.setStyleSheet("""
            background-color: #383838;
            color: #cccccc;
            border-radius: 3px;
            padding: 2px 6px;
            font-size: 10px;
            font-weight: 600;
            border: 1px solid #768fad;
        """)
        name_row.addWidget(proto_lbl)
        info_layout.addLayout(name_row)
        if self.protocol.lower() == "udp":
            host_lbl = QtWidgets.QLabel(f"{self.star_type.upper()} | @{self.hostname}:{self.port}")
        elif self.protocol.lower() == "ssh" or self.protocol.lower() == "telnet":
            host_lbl = QtWidgets.QLabel(f"{self.star_type.upper()} | {self.user}@{self.hostname}:{self.port}")
        elif self.protocol.lower() == "subprocess":
            host_lbl = QtWidgets.QLabel(f"{self.star_type.upper()} | C:\\Program Files (x86)\\TWC\\I2\\exec.exe")
        else:
            host_lbl = QtWidgets.QLabel(f"{self.star_type.upper()} | Unknown transport protocol.")
        host_lbl.setStyleSheet("font-size: 12px; color: #888888; border: none; background: transparent;")
        info_layout.addWidget(host_lbl)
        id_lbl = QtWidgets.QLabel(f"ID: {self.client.get('id', 'N/A')}")
        id_lbl.setStyleSheet("font-size: 10px; color: #666666; border: none; background: transparent;")
        info_layout.addWidget(id_lbl)
        card_layout.addLayout(info_layout)
        card_layout.addStretch()
        status_layout = QtWidgets.QVBoxLayout()
        status_layout.setAlignment(QtCore.Qt.AlignmentFlag.AlignVCenter | QtCore.Qt.AlignmentFlag.AlignRight)
        status_layout.setSpacing(4)
        self.status_lbl = QtWidgets.QLabel("Offline")
        status_layout.addWidget(self.status_lbl)
        self.last_ping_lbl = QtWidgets.QLabel(f"{self.last_seen}")
        self.last_ping_lbl.setStyleSheet("font-size: 11px; color: #666; border: none; background: transparent;")
        self.last_ping_lbl.setAlignment(QtCore.Qt.AlignmentFlag.AlignRight)
        status_layout.addWidget(self.last_ping_lbl)
        card_layout.addLayout(status_layout)
        v_sep = QtWidgets.QFrame()
        v_sep.setFrameShape(QtWidgets.QFrame.Shape.VLine)
        v_sep.setFixedHeight(40)
        v_sep.setStyleSheet("background-color: #3e3e3e; border: none;")
        card_layout.addWidget(v_sep)
        actions_layout = QtWidgets.QHBoxLayout()
        actions_layout.setSpacing(8)
        self.edit_btn = QtWidgets.QPushButton("Edit")
        self.edit_btn.setCursor(QtCore.Qt.CursorShape.PointingHandCursor)
        self.edit_btn.setFixedSize(60, 32)
        if self.edit_callback:
            self.edit_btn.clicked.connect(lambda: self.edit_callback(self.client))
        self.del_btn = QtWidgets.QPushButton("x")
        self.del_btn.setCursor(QtCore.Qt.CursorShape.PointingHandCursor)
        self.del_btn.setFixedSize(32, 32)

        if self.delete_callback:
            self.del_btn.clicked.connect(lambda: self.delete_callback(self.client))

        actions_layout.addWidget(self.edit_btn)
        actions_layout.addWidget(self.del_btn)
        card_layout.addLayout(actions_layout)
        self.setLayout(card_layout)

    def update_status(self):
        isOnline = False
        ping_time = None
        client_id = self.client.get('id')

        registry = provision.get_connection_registry()
        if registry and client_id:
            isOnline = registry.is_client_connected(client_id)
            if isOnline:
                session_info = registry.get_session(client_id)
                if session_info:
                    ping_time = datetime.fromtimestamp(session_info.last_activity).strftime("%H:%M:%S")

        if not registry and connected_outputs_data:
             for co in connected_outputs_data:
                 if co.get('hostname') == self.hostname or co.get('id') == client_id:
                     isOnline = True
                     ping_time = co.get('last_ping')
                     break
        
        if isOnline and ping_time:
            self.last_seen = ping_time
        self.last_ping_lbl.setText(f"Seen: {self.last_seen}")
        status_color = "#4cc786" if isOnline else "#b53e3e"
        status_text = "● Online" if isOnline else "○ Offline"
        self.status_lbl.setText(status_text)
        self.status_lbl.setStyleSheet(f"color: {status_color}; font-weight: 600; font-size: 12px; border: none; background: transparent;")
        self.status_lbl.setAlignment(QtCore.Qt.AlignmentFlag.AlignRight)

class EventDialog(QtWidgets.QDialog):
    def __init__(self, parent=None, event_data=None, clients=None, all_events=None):
        super().__init__(parent)
        self.setWindowTitle("Event Editor")
        self.resize(800, 960)
        self.event_data = event_data or {}
        self.clients = clients or []
        self.all_events = all_events or []
        self.original_name = self.event_data.get('DisplayName')
        self._setup_ui()
        self._populate_fields()
    def _on_event_selected(self, text):
        if text == "<New Event>":
            self.event_data = {}
            self.original_name = None
        else:
            found = next((e for e in self.all_events if e.get('DisplayName') == text), None)
            if found:
                self.event_data = found
                self.original_name = found.get('DisplayName')
        self._populate_fields()
    def _setup_ui(self):
        layout = QtWidgets.QVBoxLayout(self)
        scroll = QtWidgets.QScrollArea()
        scroll.setWidgetResizable(True)
        content = QtWidgets.QWidget()
        self.form = QtWidgets.QFormLayout(content)
        
        self.event_selector = QtWidgets.QComboBox()
        self.event_selector.addItem("<New Event>")
        if self.all_events:
            for e in self.all_events:
                self.event_selector.addItem(e.get('DisplayName', 'Un-named'))
        if self.original_name:
            self.event_selector.setCurrentText(self.original_name)
        self.event_selector.currentTextChanged.connect(self._on_event_selected)
        self.form.addRow("Edit Event:", self.event_selector)
        sep = QtWidgets.QFrame()
        sep.setFrameShape(QtWidgets.QFrame.Shape.HLine)
        sep.setFrameShadow(QtWidgets.QFrame.Shadow.Sunken)
        self.form.addRow(sep)
        self.category_combo = QtWidgets.QComboBox()
        self.category_combo.addItems(["Cue Presentation", "Custom Command", "Cancel Presentation"])
        self.category_combo.setHidden(True)
        self.form.addRow("Category:", self.category_combo)
        self.name_edit = QtWidgets.QLineEdit()
        self.form.addRow("Display Name:", self.name_edit)
        self.target_id_edit = QtWidgets.QLineEdit()
        self.form.addRow("Presentation ID:", self.target_id_edit)
        self.enabled_chk = QtWidgets.QCheckBox("Enabled")
        self.form.addRow("", self.enabled_chk)
        self.runstartup_chk = QtWidgets.QCheckBox("Run on Startup")
        self.form.addRow("", self.runstartup_chk)
        self.custom_cmd_edit = QtWidgets.QLineEdit()
        self.form.addRow("Custom Command:", self.custom_cmd_edit)
        self.min_interval_edit = QtWidgets.QLineEdit()
        self.min_interval_edit.setPlaceholderText("e.g. 15 (optional)")
        self.form.addRow("Minute Interval:", self.min_interval_edit)
        self.ten_min_group = QtWidgets.QButtonGroup(self)
        self.ten_min_group.setExclusive(False)
        tm_layout = QtWidgets.QHBoxLayout()
        self.tm_checks = []
        for tm in ["00", "10", "20", "30", "40", "50"]:
            chk = QtWidgets.QCheckBox(tm)
            self.ten_min_group.addButton(chk)
            tm_layout.addWidget(chk)
            self.tm_checks.append(chk)
        tm_select_all = QtWidgets.QPushButton("Select All")
        tm_select_all.clicked.connect(lambda: self._select_all_checks(self.tm_checks))
        tm_layout.addWidget(tm_select_all)
        self.form.addRow("Ten Minute Interval:", tm_layout)
        self.hours_edit = QtWidgets.QLineEdit()
        self.hours_edit.setPlaceholderText("e.g. 9, 13, 17 (24h format, comma separated)")
        self.form.addRow("Hours:", self.hours_edit)
        self.days_list = QtWidgets.QListWidget()
        self.days_list.setFixedHeight(96)
        days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        for d in days:
            item = QtWidgets.QListWidgetItem(d)
            item.setFlags(item.flags() | QtCore.Qt.ItemFlag.ItemIsUserCheckable)
            item.setCheckState(QtCore.Qt.CheckState.Unchecked)
            self.days_list.addItem(item)
        days_layout = QtWidgets.QVBoxLayout()
        days_layout.addWidget(self.days_list)
        days_select_all = QtWidgets.QPushButton("Select All")
        days_select_all.clicked.connect(self._select_all_days)
        days_layout.addWidget(days_select_all)
        self.form.addRow("Days:", days_layout)
        self.weeks_layout = QtWidgets.QHBoxLayout()
        self.week_checks = []
        for w in ["1", "2", "3", "4", "5"]:
            chk = QtWidgets.QCheckBox(f"Week {w}")
            self.weeks_layout.addWidget(chk)
            self.week_checks.append(chk)
        weeks_select_all = QtWidgets.QPushButton("Select All")
        weeks_select_all.clicked.connect(lambda: self._select_all_checks(self.week_checks))
        self.weeks_layout.addWidget(weeks_select_all)
        self.form.addRow("Weeks:", self.weeks_layout)
        self.months_layout = QtWidgets.QGridLayout()
        self.month_checks = []
        month_names = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"]
        for i, m in enumerate(month_names):
            chk = QtWidgets.QCheckBox(m)
            row = i // 4
            col = i % 4
            self.months_layout.addWidget(chk, row, col)
            self.month_checks.append(chk)
        months_select_all = QtWidgets.QPushButton("Select All")
        months_select_all.clicked.connect(lambda: self._select_all_checks(self.month_checks))
        self.months_layout.addWidget(months_select_all, 3, 0, 1, 4)
        self.form.addRow("Months:", self.months_layout)
        self.mapping_widget = QtWidgets.QWidget()
        mapping_layout = QtWidgets.QVBoxLayout(self.mapping_widget)
        mapping_layout.setContentsMargins(0,0,0,0)
        self.cards_layout = QtWidgets.QVBoxLayout()
        self.cards_layout.setSpacing(2)
        self.cards_layout.setContentsMargins(0,0,0,0)
        self.cards_layout.addStretch() 
        cards_scroll = QtWidgets.QScrollArea()
        cards_scroll.setWidgetResizable(True)
        cards_scroll.setMinimumHeight(400)
        cards_scroll.setStyleSheet("QScrollArea { border: 1px solid #333; background: #1a1a1a; border-radius: 4px; }")
        cards_content = QtWidgets.QWidget()
        cards_content.setObjectName("cardsContent")
        cards_content.setStyleSheet("#cardsContent { background: transparent; }")
        cards_content.setLayout(self.cards_layout)
        cards_scroll.setWidget(cards_content)
        mapping_layout.addWidget(cards_scroll)
        btn_layout = QtWidgets.QHBoxLayout()
        self.add_client_btn = QtWidgets.QPushButton("+ Add Client Action")
        self.add_client_btn.clicked.connect(self._add_action_card)
        self.add_client_btn.setFixedHeight(32)
        btn_layout.addWidget(self.add_client_btn)
        btn_layout.addStretch()
        mapping_layout.addLayout(btn_layout)
        self.form.addRow("Client Actions:", self.mapping_widget)
        self.card_widgets = []
        self.client_select_widget = QtWidgets.QWidget()
        client_select_layout = QtWidgets.QVBoxLayout(self.client_select_widget)
        client_select_layout.setContentsMargins(0,0,0,0)
        self.client_list = QtWidgets.QListWidget()
        self.client_list.setSelectionMode(QtWidgets.QAbstractItemView.SelectionMode.NoSelection)
        self.client_list.setMaximumHeight(64)
        for client in self.clients:
             cid = client.get('id', 'unknown')
             name = client.get('displayName') or client.get('star') or cid
             item = QtWidgets.QListWidgetItem(f"{name} ({cid})")
             item.setData(QtCore.Qt.ItemDataRole.UserRole, cid)
             item.setFlags(item.flags() | QtCore.Qt.ItemFlag.ItemIsUserCheckable)
             item.setCheckState(QtCore.Qt.CheckState.Unchecked)
             self.client_list.addItem(item)
        client_select_layout.addWidget(self.client_list)
        self.form.addRow("Clients:", self.client_select_widget)
        scroll.setWidget(content)
        layout.addWidget(scroll)
        btn_box = QtWidgets.QDialogButtonBox(QtWidgets.QDialogButtonBox.StandardButton.Save | QtWidgets.QDialogButtonBox.StandardButton.Cancel)
        self.apply_btn = btn_box.addButton("Apply", QtWidgets.QDialogButtonBox.ButtonRole.ApplyRole)
        btn_box.accepted.connect(self.accept)
        btn_box.rejected.connect(self.reject)
        self.apply_btn.clicked.connect(self._on_apply)
        layout.addWidget(btn_box)
    
    def _on_apply(self):
        if self.parent() and hasattr(self.parent(), '_handle_event_dialog_apply'):
             self.parent()._handle_event_dialog_apply(self)

    def _refresh_clients(self):
        if self.controller and self.controller.scheduler and self.controller.scheduler.loop and self.controller.scheduler.loop.is_running():
            asyncio.run_coroutine_threadsafe(
                self.controller.get_all_output_clients(),
                self.controller.scheduler.loop
            )
            QtWidgets.QMessageBox.information(self, "Refreshing", "Client enumeration started.")
        elif self.controller:
            t = threading.Thread(target=self._refresh_clients_sync)
            t.start()
            QtWidgets.QMessageBox.information(self, "Refreshing", "Client enumeration started.")

    def _refresh_clients_sync(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(self.controller.get_all_output_clients())
        finally:
            loop.close()

    def _select_all_checks(self, check_list):
        all_checked = all(chk.isChecked() for chk in check_list)
        for chk in check_list:
            chk.setChecked(not all_checked)

    def _select_all_days(self):
        all_checked = True
        for i in range(self.days_list.count()):
            if self.days_list.item(i).checkState() != QtCore.Qt.CheckState.Checked:
                all_checked = False
                break
        new_state = QtCore.Qt.CheckState.Unchecked if all_checked else QtCore.Qt.CheckState.Checked
        for i in range(self.days_list.count()):
            self.days_list.item(i).setCheckState(new_state)
            
    def _update_visibility(self):
        cat = self.category_combo.currentText()
        show_cue = (cat == "Cue Presentation")
        show_cmd = (cat == "Custom Command")
        show_cancel = (cat == "Cancel Presentation")
        self.custom_cmd_edit.setVisible(show_cmd)
        self.form.labelForField(self.custom_cmd_edit).setVisible(show_cmd)
        self.target_id_edit.setVisible(show_cancel)
        self.form.labelForField(self.target_id_edit).setVisible(show_cancel)
        self.mapping_widget.setVisible(show_cue)
        self.form.labelForField(self.mapping_widget).setVisible(show_cue)
        self.client_select_widget.setVisible(not show_cue)
        self.form.labelForField(self.client_select_widget).setVisible(not show_cue)

    def _populate_fields(self):
        if not self.event_data:
            self.category_combo.setCurrentText('Cue Presentation')
            self.name_edit.clear()
            self.target_id_edit.clear()
            self.enabled_chk.setChecked(True)
            self.runstartup_chk.setChecked(False)
            self.min_interval_edit.clear()
            self.custom_cmd_edit.clear()
            for c in list(self.card_widgets):
                self._remove_card(c)
            self.hours_edit.clear()
            for chk in self.tm_checks: chk.setChecked(False)
            for chk in self.week_checks: chk.setChecked(False)
            for chk in self.month_checks: chk.setChecked(False)
            for i in range(self.days_list.count()): self.days_list.item(i).setCheckState(QtCore.Qt.CheckState.Unchecked)
            for i in range(self.client_list.count()): self.client_list.item(i).setCheckState(QtCore.Qt.CheckState.Unchecked)
            self._update_visibility()
            return
        self.category_combo.setCurrentText(self.event_data.get('Category', 'Cue Presentation'))
        self.name_edit.setText(self.event_data.get('DisplayName', ''))
        self.target_id_edit.setText(self.event_data.get('TargetID', ''))
        self.enabled_chk.setChecked(self.event_data.get('Enabled', True))
        self.runstartup_chk.setChecked(self.event_data.get('RunAtStartup', False))
        self.min_interval_edit.setText(self.event_data.get('MinuteInterval', ''))
        self.custom_cmd_edit.setText(self.event_data.get('CustomCommand', ''))
        tm_vals = self.event_data.get('TenMinuteInterval', [])
        for chk in self.tm_checks:
            chk.setChecked(chk.text() in tm_vals)
        w_vals = self.event_data.get('Weeks', [])
        for i, chk in enumerate(self.week_checks):
            chk.setChecked(str(i+1) in w_vals)
        m_vals = self.event_data.get('Months', [])
        for i, chk in enumerate(self.month_checks):
            chk.setChecked(str(i+1) in m_vals)
        event_days = self.event_data.get('Days', [])
        for i in range(self.days_list.count()):
            item = self.days_list.item(i)
            state = QtCore.Qt.CheckState.Checked if item.text() in event_days else QtCore.Qt.CheckState.Unchecked
            item.setCheckState(state)
        event_clients = self.event_data.get('clients', [])
        for i in range(self.client_list.count()):
            item = self.client_list.item(i)
            cid = item.data(QtCore.Qt.ItemDataRole.UserRole)
            state = QtCore.Qt.CheckState.Checked if cid in event_clients else QtCore.Qt.CheckState.Unchecked
            item.setCheckState(state)

        hours_data = self.event_data.get('Hours', [])
        h_strs = []
        for h in hours_data:
            val = int(h.get('hour', 0))
            period = h.get('period', 'AM')
            if period == 'PM' and val != 12:
                val += 12
            elif period == 'AM' and val == 12:
                val = 0
            h_strs.append(str(val))
        self.hours_edit.setText(", ".join(h_strs))

        for c in list(self.card_widgets):
            self._remove_card(c)
        
        c_configs = self.event_data.get('client_config', {})
        
        if c_configs:
             for key, conf in c_configs.items():
                 real_cid = conf.get('client_id') or key
                 data = {
                     'client_id': real_cid,
                     'action_guid': key,
                     'action': conf.get('action', 'LoadRun'),
                     'flavor': conf.get('flavor', ''),
                     'presentation_id': conf.get('presentation_id', ''),
                     'duration': conf.get('duration', ''),
                     'logo': conf.get('logo', ''),
                     'command': conf.get('command', ''),
                     'su': conf.get('su', ''),
                     'ldl_state': conf.get('ldl_state', '1'),
                     'separate_load_run': conf.get('separate_load_run', False),
                     'load_offset': conf.get('load_offset', -20),
                     'run_offset': conf.get('run_offset', -12)
                 }
                 self._add_action_card(data)
        else:
             flavors = self.event_data.get('flavor', {})
             if flavors:
                for k, v in flavors.items():
                    pid = self.event_data.get('TargetID', '')
                    dur = '60'
                    data = {
                        'client_id': k,
                        'action': 'Load Presentation',
                        'flavor': v,
                        'presentation_id': pid,
                        'duration': dur
                    }
                    self._add_action_card(data)
        
        self._update_visibility()

    def _add_action_card(self, initial_data=None):
        card = ClientActionCard(parent=self, clients=self.clients)
        if initial_data:
            card.set_data(initial_data)

        card.delete_clicked.connect(lambda: self._remove_card(card))
        
        self.cards_layout.insertWidget(self.cards_layout.count()-1, card)
        self.card_widgets.append(card)

    def _remove_card(self, card):
        if card in self.card_widgets:
            self.card_widgets.remove(card)
            self.cards_layout.removeWidget(card)
            card.deleteLater()

    def _update_row_params(self, row):
        pass

    def _parse_hours(self, text):
        res = []
        if not text: return res
        parts = [p.strip() for p in text.split(',')]
        for p in parts:
            if not p.isdigit(): continue
            h = int(p)
            if 0 <= h <= 23:
                period = 'AM'
                h_12 = h
                if h == 0:
                    h_12 = 12
                    period = 'AM'
                elif h == 12:
                    h_12 = 12
                    period = 'PM'
                elif h > 12:
                    h_12 = h - 12
                    period = 'PM'
                res.append({'hour': str(h_12), 'period': period})
        return res

    def get_data(self):
        weeks = []
        for i, chk in enumerate(self.week_checks):
            if chk.isChecked(): weeks.append(str(i+1))
        months = []
        for i, chk in enumerate(self.month_checks):
            if chk.isChecked(): months.append(str(i+1))
        tm = []
        for chk in self.tm_checks:
            if chk.isChecked(): tm.append(chk.text())
        flavor_map = {}
        client_configs = {}
        client_keys = []
        
        cat = self.category_combo.currentText()
        if cat == "Cue Presentation":
            for card in self.card_widgets:
                c_data = card.get_data()
                cid = c_data.get('client_id')
                if not cid: continue

                action_guid = generate_action_id(c_data)
                c_data['action_guid'] = action_guid
                
                client_configs[action_guid] = c_data
                if cid not in client_keys:
                    client_keys.append(cid)

                if c_data.get('flavor'):
                    flavor_map[cid] = c_data['flavor']
                
        else:
            for i in range(self.client_list.count()):
                item = self.client_list.item(i)
                if item.checkState() == QtCore.Qt.CheckState.Checked:
                    cid = item.data(QtCore.Qt.ItemDataRole.UserRole)
                    client_keys.append(cid)

        data = {
            'Category': self.category_combo.currentText(),
            'DisplayName': self.name_edit.text(),
            'TargetID': self.target_id_edit.text(),
            'Enabled': self.enabled_chk.isChecked(),
            'RunAtStartup': self.runstartup_chk.isChecked(),
            'CustomCommand': self.custom_cmd_edit.text(),
            'MinuteInterval': self.min_interval_edit.text(),
            'TenMinuteInterval': tm,
            'Weeks': weeks,
            'Months': months,
            'Days': [],
            'clients': client_keys,
            'Hours': self._parse_hours(self.hours_edit.text()),
            'flavor': flavor_map,
            'client_config': client_configs
        }
        
        for i in range(self.days_list.count()):
            item = self.days_list.item(i)
            if item.checkState() == QtCore.Qt.CheckState.Checked:
                data['Days'].append(item.text())
        return data

class CommandThread(QtCore.QThread):
    def __init__(self, coro):
        super().__init__()
        self.coro = coro

    def run(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
             loop.run_until_complete(self.coro)
        finally:
             loop.close()

class QuickTimeEventTab(QtWidgets.QWidget):
    def __init__(self, controller):
        super().__init__()
        self.controller = controller
        self.clients = self.controller.get_configured_clients()
        self.card_widgets = []
        self._setup_ui()

    def _setup_ui(self):
        layout = QtWidgets.QVBoxLayout(self)
        form_widget = QtWidgets.QWidget()
        self.form = QtWidgets.QFormLayout(form_widget)
        self.target_id_edit = QtWidgets.QLineEdit()
        self.target_id_edit.setVisible(False)
        self.custom_cmd_edit = QtWidgets.QLineEdit()
        self.custom_cmd_edit.setVisible(False)
        self.length_edit = QtWidgets.QLineEdit()
        self.length_edit.setText("60")
        self.length_edit.setVisible(False)
        self.client_select_widget = QtWidgets.QWidget()
        self.client_select_widget.setVisible(False)
        self.client_list = QtWidgets.QListWidget()
        self.mapping_widget = QtWidgets.QWidget()
        mapping_layout = QtWidgets.QVBoxLayout(self.mapping_widget)
        mapping_layout.setContentsMargins(0,0,0,0)
        self.cards_layout = QtWidgets.QVBoxLayout()
        self.cards_layout.setSpacing(4)
        self.cards_layout.addStretch()
        cards_scroll = QtWidgets.QScrollArea()
        cards_scroll.setWidgetResizable(True)
        cards_scroll.setMinimumHeight(400)
        cards_scroll.setStyleSheet("QScrollArea { border: 1px solid #444; background: #222; }")
        cards_content = QtWidgets.QWidget()
        cards_content.setLayout(self.cards_layout)
        cards_scroll.setWidget(cards_content)
        mapping_layout.addWidget(cards_scroll)
        btn_layout = QtWidgets.QHBoxLayout()
        self.add_client_btn = QtWidgets.QPushButton("+ Add Client Action")
        self.add_client_btn.clicked.connect(lambda: self._add_action_card())
        self.add_client_btn.setFixedHeight(32)
        self.add_client_btn.setCursor(QtCore.Qt.CursorShape.PointingHandCursor)
        btn_layout.addWidget(self.add_client_btn)
        btn_layout.addStretch()
        mapping_layout.addLayout(btn_layout)
        self.form.addRow("Client Actions:", self.mapping_widget)

        layout.addWidget(form_widget)
        
        self.exec_btn = QtWidgets.QPushButton("Execute")
        self.exec_btn.setFixedHeight(36)
        layout.addWidget(self.exec_btn)
        layout.addStretch()
        self._add_action_card({'star': 'i2xd', 'flavor': 'domestic/V'})

    def _add_action_card(self, initial_data=None):
        if initial_data and 'star' in initial_data and 'client_id' not in initial_data:
            target_star = initial_data['star']
            for c in self.clients:
                if c.get('star') == target_star:
                     initial_data = dict(initial_data)
                     initial_data['client_id'] = c.get('id')
                     break
        
        card = ClientActionCard(parent=self, clients=self.clients)
        if initial_data:
             if 'action' not in initial_data:
                 initial_data['action'] = "LoadRun"
             card.set_data(initial_data)
        
        card.delete_clicked.connect(lambda: self._remove_card(card))
        self.cards_layout.insertWidget(self.cards_layout.count()-1, card)
        self.card_widgets.append(card)

    def _remove_card(self, card):
        if card in self.card_widgets:
            self.card_widgets.remove(card)
            self.cards_layout.removeWidget(card)
            card.deleteLater()

    def _update_visibility(self):
        self.target_id_edit.setVisible(False)
        self.form.labelForField(self.target_id_edit).setVisible(False)
        self.custom_cmd_edit.setVisible(False)
        self.form.labelForField(self.custom_cmd_edit).setVisible(False)
        self.length_edit.setVisible(False)
        self.form.labelForField(self.length_edit).setVisible(False)
        self.mapping_widget.setVisible(True)
        self.form.labelForField(self.mapping_widget).setVisible(True)
        self.client_select_widget.setVisible(False)
        self.form.labelForField(self.client_select_widget).setVisible(False)

    def _execute(self):
        cat = "Cue Presentation"
        
        client_configs = {}
        target_clients = [] 
        
        for card in self.card_widgets:
            c_data = card.get_data()
            cid = c_data.get('client_id')
            if cid:
                action_guid = c_data.get('action_guid')
                if not action_guid:
                    action_guid = generate_action_id(c_data)
                    c_data['action_guid'] = action_guid
            
                client_configs[action_guid] = c_data
                if cid not in target_clients:
                    target_clients.append(cid)
        
        if not target_clients:
            logger.warning("No clients selected for Quick Time Event execution")
            return
        
        target_id = self.target_id_edit.text()
        custom_cmd = self.custom_cmd_edit.text()
        length = 60
        t = threading.Thread(
            target=self._run_batch_logic_sync,
            args=(cat, target_clients, client_configs, target_id, custom_cmd, length),
            daemon=True
        )
        t.start()
        
    async def _run_batch_logic_async(self, cat, target_clients, client_configs, target_id, custom_cmd, length):
        clients = self.controller.get_configured_clients()
        logger.info("Executing Quick Time Event batch...")
        
        client_map = {}
        for c in clients:
             if c.get('id'): client_map[c.get('id')] = c
             if c.get('star'): client_map[c.get('star')] = c

        tasks = []
        for key, conf in client_configs.items():
            cid = conf.get('client_id') or key
            client = client_map.get(cid)
            if not client:
                continue

            task = asyncio.create_task(
                self._execute_single_client_async(client, cat, conf, target_id, custom_cmd, length)
            )
            tasks.append(task)
        
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        logger.info("Quick Time Event batch completed.")
    
    async def _execute_single_client_async(self, client, cat, conf, target_id, custom_cmd, length):
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            provision._get_executor(), 
            partial(self._execute_single_client_sync_wrapper, client, cat, conf, target_id, custom_cmd, length)
        )

    def _execute_single_client_sync_wrapper(self, client, cat, conf, target_id, custom_cmd, length):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
             loop.run_until_complete(
                 self._execute_single_client_implementation(client, cat, conf, target_id, custom_cmd, length)
             )
        finally:
             loop.close()

    async def _execute_single_client_implementation(self, client, cat, conf, target_id, custom_cmd, length):
        cid = client.get('id') or client.get('star')
        creds = client.get('credentials', {})
        hostname = creds.get('hostname')
        user = creds.get('user')
        password = creds.get('password')
        port = creds.get('port', 22)
        star_type = client.get('star', 'unknown')
        protocol = client.get('protocol', 'ssh')
        
        action = conf.get('action', 'LoadRun')
        flavor = conf.get('flavor', '')
        pres_id = conf.get('presentation_id', '') or target_id
        duration_seconds = int(conf.get('duration', length)) if str(conf.get('duration', length)).isdigit() else 60
        duration = duration_seconds * 30
        ldl_state = conf.get('ldl_state', '1')
        cmd = conf.get('command', '') or custom_cmd
        
        separate_load_run = conf.get('separate_load_run', False)
        load_offset = int(conf.get('load_offset', -20))
        run_offset = int(conf.get('run_offset', -12))
        
        is_i1 = (star_type == 'i1')
        su = creds.get('su', 'dgadmin') if is_i1 else creds.get('su', None)
        
        def log_result(res, cmd_info, color):
            output = f"[COMMAND] {cmd_info}\n"
            if res and isinstance(res, tuple) and len(res) == 2:
                stdout, stderr = res
                if stdout.strip():
                    output += f"[STDOUT]\n{stdout}\n"
                if stderr.strip():
                    output += f"[STDERR]\n{stderr}\n"
            self.controller.client_manager.log_output(cid, output, color)
        
        try:
            if action == "LoadRun" and separate_load_run:
                cmd_info_load = f"{protocol.upper()} {'i1' if is_i1 else 'i2'} Load (Quick) pres={pres_id if pres_id else '1'}"
                res_load = None
                
                if is_i1:
                    if protocol == 'ssh':
                        res_load = await provision.ssh_load_i1_pres(
                            hostname=hostname, user=user, password=password, port=port,
                            flavor=flavor, PresentationId=(pres_id or 'local'), su=su
                        )
                    elif protocol == 'telnet':
                        telnet_port = int(port) if port else 23
                        res_load = await provision.telnet_load_i1_pres(
                            hostname=hostname, port=telnet_port,
                            flavor=flavor, PresentationId=(pres_id or 'local'),
                            user=user, password=password, su=su
                        )
                else:
                    if protocol == 'ssh':
                        res_load = await provision.ssh_load_i2_pres(
                            hostname=hostname, user=user, password=password, port=port,
                            flavor=flavor, PresentationId=(pres_id or '1'), duration=duration, su=su
                        )
                    elif protocol == 'telnet':
                        telnet_port = int(port) if port else 23
                        res_load = await provision.telnet_load_i2_pres(
                            hostname=hostname, port=telnet_port,
                            flavor=flavor, PresentationId=(pres_id or '1'), duration=duration,
                            user=user, password=password
                        )
                    elif protocol == 'udp':
                        udp_port = int(port) if port else 7787
                        await provision.execute_udp_load_i2_pres(
                            hostname=hostname, port=udp_port,
                            flavor=flavor, PresentationId=(pres_id or '1'), duration=duration
                        )
                    elif protocol == 'subprocess':
                        res_load = await provision.subproc_load_i2_pres(
                            flavor=flavor, PresentationId=(pres_id or '1'), duration=duration
                        )
                
                log_result(res_load, cmd_info_load)

                delay = abs(run_offset)
                if delay > 0:
                    description = f"Waiting {delay}s because of 'Separate Load/Run' offset..."
                    self.controller.client_manager.log_output(cid, f"[INFO] {description}\n")
                    await asyncio.sleep(delay)
                
                cmd_info_run = f"{protocol.upper()} {'i1' if is_i1 else 'i2'} Run (Quick) pres={pres_id if pres_id else '1'}"
                res_run = None
                
                if is_i1:
                    if protocol == 'ssh':
                        res_run = await provision.ssh_run_i1_pres(
                            hostname=hostname, user=user, password=password, port=port,
                            flavor=flavor, PresentationId=(pres_id or 'local'), su=su
                        )
                    elif protocol == 'telnet':
                        telnet_port = int(port) if port else 23
                        res_run = await provision.telnet_run_i1_pres(
                            hostname=hostname, port=telnet_port,
                            flavor=flavor, PresentationId=(pres_id or 'local'),
                            user=user, password=password, su=su
                        )
                else:
                    if protocol == 'ssh':
                        res_run = await provision.ssh_run_i2_pres(
                            hostname=hostname, user=user, password=password, port=port,
                            PresentationId=(pres_id or '1'), su=su
                        )
                    elif protocol == 'telnet':
                        telnet_port = int(port) if port else 23
                        res_run = await provision.telnet_run_i2_pres(
                            hostname=hostname, port=telnet_port,
                            PresentationId=(pres_id or '1'),
                            user=user, password=password
                        )
                    elif protocol == 'udp':
                        udp_port = int(port) if port else 7787
                        await provision.execute_udp_run_i2_pres(
                            hostname=hostname, port=udp_port, PresentationId=(pres_id or '1')
                        )
                    elif protocol == 'subprocess':
                        res_run = await provision.subproc_run_i2_pres(PresentationId=(pres_id or '1'))
                
                log_result(res_run, cmd_info_run)
                return

            if cat == "Custom Command" or action == "Custom Command":
                cmd_info = f"{protocol.upper()} Custom: {cmd[:50]}..." if len(cmd) > 50 else f"{protocol.upper()} Custom: {cmd}"
                res = None
                if protocol == 'ssh':
                    res = await provision.execute_ssh_command(
                        hostname=hostname, user=user, password=password, port=port,
                        command=cmd, su=su
                    )
                elif protocol == 'telnet':
                    telnet_port = int(port) if port else 23
                    res = await provision.execute_telnet_command(
                        hostname=hostname, port=telnet_port, command=cmd
                    )
                elif protocol == 'udp':
                    udp_port = int(port) if port else 7787
                    await provision.execute_udp_message(
                        hostname=hostname, port=udp_port, message=cmd
                    )
                log_result(res, cmd_info)
                return

            if cat == "Cancel Presentation" or action == "Cancel":
                final_id = pres_id if pres_id else ('local' if is_i1 else '1')
                cmd_info = f"{protocol.upper()} Cancel pres_id={final_id}"
                res = None
                if protocol == 'ssh':
                    if star_type.startswith('i2'):
                        res = await provision.execute_ssh_command(
                            hostname=hostname, user=user, password=password, port=port,
                            command=f'"{provision.i2exec}" cancelPres(PresentationId="{final_id}")', su=su
                        )
                elif protocol == 'telnet':
                    telnet_port = int(port) if port else 23
                    if star_type.startswith('i2'):
                        res = await provision.telnet_cancel_i2_pres(
                            hostname=hostname, port=telnet_port,
                            PresentationId=final_id, user=user, password=password
                        )
                elif protocol == 'udp':
                    udp_port = int(port) if port else 7787
                    if star_type.startswith('i2'):
                        await provision.execute_udp_cancel_i2_pres(
                            hostname=hostname, port=udp_port, PresentationId=final_id
                        )
                elif protocol == 'subprocess':
                    res = await provision.subproc_cancel_i2_pres(PresentationId=final_id)
                log_result(res, cmd_info)
                return
            
            if is_i1 and action == "LDL (On/Off)":
                target_state = int(ldl_state) if str(ldl_state).isdigit() else 1
                cmd_info = f"{protocol.upper()} i1 LDL Toggle state={target_state}"
                res = None
                if protocol == 'ssh':
                    res = await provision.ssh_toggleldl_i1(
                        hostname=hostname, user=user, password=password, port=port,
                        state=target_state, su=su
                    )
                elif protocol == 'telnet':
                    telnet_port = int(port) if port else 23
                    res = await provision.telnet_toggleldl_i1(
                        hostname=hostname, port=telnet_port,
                        state=target_state, su=su, user=user, password=password
                    )
                log_result(res, cmd_info)
                return

            final_id = pres_id if pres_id else ('local' if is_i1 else '1')
            i_type = "i1" if is_i1 else "i2"
            cmd_info = f"{protocol.upper()} {i_type} {action} pres={final_id}"
            res = None
            
            if is_i1:
                if action == "LoadRun":
                    if protocol == 'ssh':
                        res = await provision.ssh_loadrun_i1_pres(
                            hostname=hostname, user=user, password=password, port=port,
                            flavor=flavor, PresentationId=final_id, su=su
                        )
                    elif protocol == 'telnet':
                        telnet_port = int(port) if port else 23
                        res = await provision.telnet_loadrun_i1_pres(
                            hostname=hostname, port=telnet_port,
                            flavor=flavor, PresentationId=final_id,
                            user=user, password=password, su=su
                        )
                elif action == "Load":
                    if protocol == 'ssh':
                        res = await provision.ssh_load_i1_pres(
                            hostname=hostname, user=user, password=password, port=port,
                            flavor=flavor, PresentationId=final_id, su=su
                        )
                    elif protocol == 'telnet':
                        telnet_port = int(port) if port else 23
                        res = await provision.telnet_load_i1_pres(
                            hostname=hostname, port=telnet_port,
                            flavor=flavor, PresentationId=final_id,
                            user=user, password=password, su=su
                        )
                elif action == "Run":
                    if protocol == 'ssh':
                        res = await provision.ssh_run_i1_pres(
                            hostname=hostname, user=user, password=password, port=port,
                            flavor=flavor, PresentationId=final_id, su=su
                        )
                    elif protocol == 'telnet':
                        telnet_port = int(port) if port else 23
                        res = await provision.telnet_run_i1_pres(
                            hostname=hostname, port=telnet_port,
                            flavor=flavor, PresentationId=final_id,
                            user=user, password=password, su=su
                        )
            else:
                if action == "LoadRun":
                    if protocol == 'ssh':
                        res = await provision.ssh_loadrun_i2_pres(
                            hostname=hostname, user=user, password=password, port=port,
                            flavor=flavor, PresentationId=final_id, duration=duration, su=su
                        )
                    elif protocol == 'telnet':
                        telnet_port = int(port) if port else 23
                        res = await provision.telnet_loadrun_i2_pres(
                            hostname=hostname, port=telnet_port,
                            flavor=flavor, PresentationId=final_id, duration=duration,
                            user=user, password=password
                        )
                    elif protocol == 'udp':
                        udp_port = int(port) if port else 7787
                        await provision.execute_udp_load_i2_pres(
                            hostname=hostname, port=udp_port,
                            flavor=flavor, PresentationId=final_id, duration=duration
                        )
                        await provision.execute_udp_run_i2_pres(
                            hostname=hostname, port=udp_port, PresentationId=final_id
                        )
                        cmd_info = f"UDP i2 LoadRun pres={final_id} (Load+Run)"
                    elif protocol == 'subprocess':
                        res = await provision.subproc_loadrun_i2_pres(
                            flavor=flavor, PresentationId=final_id, duration=duration
                        )
                elif action == "Load":
                    if protocol == 'ssh':
                        res = await provision.ssh_load_i2_pres(
                            hostname=hostname, user=user, password=password, port=port,
                            flavor=flavor, PresentationId=final_id, duration=duration, su=su
                        )
                    elif protocol == 'telnet':
                        telnet_port = int(port) if port else 23
                        res = await provision.telnet_load_i2_pres(
                            hostname=hostname, port=telnet_port,
                            flavor=flavor, PresentationId=final_id, duration=duration,
                            user=user, password=password
                        )
                    elif protocol == 'udp':
                        udp_port = int(port) if port else 7787
                        await provision.execute_udp_load_i2_pres(
                            hostname=hostname, port=udp_port,
                            flavor=flavor, PresentationId=final_id, duration=duration
                        )
                    elif protocol == 'subprocess':
                        res = await provision.subproc_load_i2_pres(
                            flavor=flavor, PresentationId=final_id, duration=duration
                        )
                elif action == "Run":
                    if protocol == 'ssh':
                        res = await provision.ssh_run_i2_pres(
                            hostname=hostname, user=user, password=password, port=port,
                            PresentationId=final_id, su=su
                        )
                    elif protocol == 'telnet':
                        telnet_port = int(port) if port else 23
                        res = await provision.telnet_run_i2_pres(
                            hostname=hostname, port=telnet_port,
                            PresentationId=final_id,
                            user=user, password=password
                        )
                    elif protocol == 'udp':
                        udp_port = int(port) if port else 7787
                        await provision.execute_udp_run_i2_pres(
                            hostname=hostname, port=udp_port, PresentationId=final_id
                        )
                    elif protocol == 'subprocess':
                        res = await provision.subproc_run_i2_pres(PresentationId=final_id)
            
            log_result(res, cmd_info)
                    
        except Exception as e:
            logger.error(f"[{cid}] Quick Time Event error: {e}")
    
    def _run_batch_logic_sync(self, cat, target_clients, client_configs, target_id, custom_cmd, length):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(
                self._run_batch_logic_async(cat, target_clients, client_configs, target_id, custom_cmd, length)
            )
        finally:
            loop.close()


class HomeTab(QtWidgets.QWidget):
    def __init__(self, controller, parent_ui):
        super().__init__()
        self.controller = controller
        self.parent_ui = parent_ui 
        self.scheduler = None 
        
        self.setStyleSheet("""
            QWidget {
                background-color: #1e1e1e; 
                color: #e0e0e0;
                font-family: 'Host Grotesk', sans-serif;
            }
            QListWidget {
                background-color: #252525;
                color: #cccccc;
                border-right: 1px solid #333333;
                outline: none;
                font-size: 14px;
            }
            QListWidget::item {
                padding: 15px;
                border-bottom: 1px solid #333333;
            }
            QListWidget::item:selected {
                background-color: #333333;
                color: #ffffff;
                border-left: 4px solid #3b82f6; 
            }
            QListWidget::item:hover {
                background-color: #2a2a2a;
            }
            QTableWidget {
                background-color: #1e1e1e;
                border: 1px solid #333333;
                gridline-color: #333333;
            }
            QHeaderView::section {
                background-color: #2d2d2d;
                color: #ffffff;
                padding: 6px;
                border: 1px solid #333333;
                font-weight: bold;
            }
            QTableWidget::item {
                color: #e0e0e0;
                padding: 5px;
            }
            QLabel {
                color: #e0e0e0;
            }
            QScrollArea {
                background-color: transparent;
                border: none;
            }
            QScrollBar:vertical {
                background: #1e1e1e;
                width: 10px;
            }
            QScrollBar::handle:vertical {
                background: #444;
                border-radius: 5px;
            }
        """)

        layout = QtWidgets.QHBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)
        self.sidebar_list = QtWidgets.QListWidget()
        self.sidebar_list.setFixedWidth(250)
        self.sidebar_list.addItem("Overview")
        self.sidebar_list.addItem("Clients")
        self.sidebar_list.addItem("Logs")
        self.sidebar_list.setCurrentRow(0)
        self.sidebar_list.currentRowChanged.connect(self.change_page)
        layout.addWidget(self.sidebar_list)
        self.stack = QtWidgets.QStackedWidget()
        layout.addWidget(self.stack)
        self.overview_page = self.create_overview_page()
        self.stack.addWidget(self.overview_page)
        self.clients_page = self.create_clients_page()
        self.stack.addWidget(self.clients_page)
        self.logs_page = self.create_logs_page()
        self.stack.addWidget(self.logs_page)
        self.controller.log_proxy.log_received.connect(self.append_log)
        self.start_time = datetime.now()
        self.timer = QtCore.QTimer()
        self.timer.timeout.connect(self.update_dashboard)
        self.timer.start(2000)
        self.client_cards = []

    def set_scheduler(self, scheduler_instance):
        self.scheduler = scheduler_instance

    def create_logs_page(self):
        page = QtWidgets.QWidget()
        page.setStyleSheet("background-color: #1e1e1e;")
        layout = QtWidgets.QHBoxLayout(page)
        self.log_client_list = QtWidgets.QListWidget()
        self.log_client_list.setFixedWidth(200)
        self.log_client_list.currentRowChanged.connect(self.change_log_view)
        layout.addWidget(self.log_client_list)
        self.log_stack = QtWidgets.QStackedWidget()
        layout.addWidget(self.log_stack)
        self.log_terminals = {}
        self.log_clients_map = []
        self.refresh_log_clients()
        
        return page

    def refresh_log_clients(self):
        self.log_client_list.clear()
        for i in reversed(range(self.log_stack.count())):
            self.log_stack.widget(i).deleteLater()
        self.log_terminals = {}
        self.log_clients_map = []
        
        clients = self.controller.get_configured_clients()
        for client in clients:
            name = client.get('displayName', client.get('id'))
            cid = client.get('id')
            self.log_client_list.addItem(name)
            self.log_clients_map.append(cid)
            term = QtWidgets.QTextEdit()
            term.setReadOnly(True)
            term.setStyleSheet("""
                QTextEdit {
                    background-color: #0d0d0d;
                    color: #00ff00;
                    font-family: 'Courier New', monospace;
                    font-size: 11px;
                    border: 1px solid #333;
                }
            """)
            self.log_stack.addWidget(term)
            self.log_terminals[cid] = term
            existing = self.controller.client_manager.logs.get(cid, [])
            for line in existing:
                term.append(line)

        if self.log_stack.count() > 0:
            self.log_client_list.setCurrentRow(0)

    def change_log_view(self, row):
        if row >= 0 and row < self.log_stack.count():
            self.log_stack.setCurrentIndex(row)
            
    def append_log(self, client_id, text):
        if client_id in self.log_terminals:
            term = self.log_terminals[client_id]
            html_text = ansi_to_html(text)
            term.append(html_text)
            term.moveCursor(QtGui.QTextCursor.MoveOperation.End)

    def change_page(self, row):
        self.stack.setCurrentIndex(row)
        if row == 1:
            self.refresh_clients_list()
        elif row == 2:
            if self.log_client_list.count() != len(self.controller.config.get('outputs', [])):
                self.refresh_log_clients()

    def create_overview_page(self):
        page = QtWidgets.QWidget()
        page.setStyleSheet("background-color: #1e1e1e;") 
        layout = QtWidgets.QVBoxLayout(page)
        layout.setContentsMargins(20, 20, 40, 20)
        layout.setSpacing(25)
        title_box = QtWidgets.QWidget()
        title_layout = QtWidgets.QVBoxLayout(title_box)
        title_layout.setContentsMargins(0,0,0,10)
        
        title = QtWidgets.QLabel("System Overview")
        title.setStyleSheet("font-size: 26px; font-weight: bold; color: #ffffff;")
        title_layout.addWidget(title)
        layout.addWidget(title_box)

        sched_label = QtWidgets.QLabel("Scheduler Activity")
        sched_label.setStyleSheet("font-size: 16px; font-weight: 600; color: #a0a0a0; margin-bottom: 5px;")
        layout.addWidget(sched_label)

        self.sched_table = QtWidgets.QTableWidget()
        self.sched_table.setColumnCount(2)
        self.sched_table.setHorizontalHeaderLabels(["Metric", "Value"])
        self.sched_table.horizontalHeader().setSectionResizeMode(0, QtWidgets.QHeaderView.ResizeMode.ResizeToContents)
        self.sched_table.horizontalHeader().setSectionResizeMode(1, QtWidgets.QHeaderView.ResizeMode.Stretch)
        self.sched_table.verticalHeader().setVisible(False)
        self.sched_table.setEditTriggers(QtWidgets.QAbstractItemView.EditTrigger.NoEditTriggers)
        self.sched_table.setSelectionMode(QtWidgets.QAbstractItemView.SelectionMode.NoSelection)
        self.sched_table.setRowCount(7)
        self.sched_table.setSizePolicy(QtWidgets.QSizePolicy.Policy.Expanding, QtWidgets.QSizePolicy.Policy.Expanding)
        layout.addWidget(self.sched_table)

        watch_label = QtWidgets.QLabel("System Watch Points")
        watch_label.setStyleSheet("font-size: 16px; font-weight: 600; color: #a0a0a0; margin-bottom: 5px;")
        layout.addWidget(watch_label)
        
        self.watch_table = QtWidgets.QTableWidget()
        self.watch_table.setColumnCount(2)
        self.watch_table.setHorizontalHeaderLabels(["Metric", "Value"])
        self.watch_table.horizontalHeader().setSectionResizeMode(0, QtWidgets.QHeaderView.ResizeMode.ResizeToContents)
        self.watch_table.horizontalHeader().setSectionResizeMode(1, QtWidgets.QHeaderView.ResizeMode.Stretch)
        self.watch_table.verticalHeader().setVisible(False)
        self.watch_table.setEditTriggers(QtWidgets.QAbstractItemView.EditTrigger.NoEditTriggers)
        self.watch_table.setSizePolicy(QtWidgets.QSizePolicy.Policy.Expanding, QtWidgets.QSizePolicy.Policy.Expanding)
        layout.addWidget(self.watch_table)
        self.app_start_time = time.time()
        
        return page

    def create_clients_page(self):
        page = QtWidgets.QWidget()
        page.setStyleSheet("background-color: #1e1e1e;") 
        layout = QtWidgets.QVBoxLayout(page)
        layout.setContentsMargins(20, 20, 40, 20) 
        layout.setSpacing(20)
        header = QtWidgets.QHBoxLayout()
        header.setSpacing(15)
        title_section = QtWidgets.QVBoxLayout()
        title_section.setSpacing(4)
        title = QtWidgets.QLabel("Clients")
        title.setStyleSheet("font-size: 26px; font-weight: bold; color: #ffffff;")
        subtitle = QtWidgets.QLabel("Manage your connected STAR systems")
        subtitle.setStyleSheet("font-size: 13px; color: #888888; font-weight: 400;")
        title_section.addWidget(title)
        title_section.addWidget(subtitle)
        header.addLayout(title_section)
        header.addStretch()
        btn_layout = QtWidgets.QHBoxLayout()
        add_btn = QtWidgets.QPushButton("+ Add Client")
        add_btn.setCursor(QtCore.Qt.CursorShape.PointingHandCursor)
        add_btn.clicked.connect(self._open_add_client_dialog)
        header.addWidget(add_btn)
        refresh_btn = QtWidgets.QPushButton("⟳ Refresh")
        refresh_btn.setCursor(QtCore.Qt.CursorShape.PointingHandCursor)
        refresh_btn.clicked.connect(self._trigger_ping)
        header.addWidget(refresh_btn)
        layout.addLayout(header)
        separator = QtWidgets.QFrame()
        separator.setFrameShape(QtWidgets.QFrame.Shape.HLine)
        separator.setStyleSheet("background-color: #2d2d2d; max-height: 1px;")
        layout.addWidget(separator)
        scroll = QtWidgets.QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QtWidgets.QFrame.Shape.NoFrame)
        scroll.setStyleSheet("""
            QScrollArea {
                background: transparent;
                border: none;
            }
            QScrollBar:vertical {
                background: #1e1e1e;
                width: 10px;
                margin: 0px;
            }
            QScrollBar::handle:vertical {
                background: #424242;
                border-radius: 5px;
                min-height: 40px;
            }
            QScrollBar::handle:vertical:hover {
                background: #505050;
            }
            QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {
                height: 0px;
            }
        """)
        
        self.clients_content_widget = QtWidgets.QWidget()
        self.clients_content_layout = QtWidgets.QVBoxLayout(self.clients_content_widget)
        self.clients_content_layout.setContentsMargins(0, 10, 0, 10)
        self.clients_content_layout.setSpacing(0)
        self.clients_list_layout = QtWidgets.QVBoxLayout()
        self.clients_list_layout.setAlignment(QtCore.Qt.AlignmentFlag.AlignTop)
        self.clients_list_layout.setSpacing(10)
        self.empty_state_widget = self.create_empty_state_widget()
        self.clients_content_layout.addLayout(self.clients_list_layout)
        self.clients_content_layout.addWidget(self.empty_state_widget)
        self.clients_content_layout.addStretch()
        scroll.setWidget(self.clients_content_widget)
        layout.addWidget(scroll)
        return page

    def create_empty_state_widget(self):
        empty_widget = QtWidgets.QWidget()
        empty_layout = QtWidgets.QVBoxLayout(empty_widget)
        empty_layout.setAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)
        empty_layout.setSpacing(15)
        empty_layout.setContentsMargins(40, 60, 40, 60)
        
        icon_label = QtWidgets.QLabel("Disconnected")
        icon_label.setStyleSheet("color: #444; font-size: 24px; font-weight: bold;")
        icon_label.setAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)
        
        title_label = QtWidgets.QLabel("No Clients Configured")
        title_label.setStyleSheet("font-size: 16px; font-weight: 600; color: #888;")
        title_label.setAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)
        
        desc_label = QtWidgets.QLabel("Add a client system to get started.")
        desc_label.setStyleSheet("font-size: 13px; color: #666;")
        desc_label.setAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)
        
        empty_layout.addWidget(icon_label)
        empty_layout.addWidget(title_label)
        empty_layout.addWidget(desc_label)
        
        return empty_widget

    def _save_config(self):
        try:
            with open('user/config.json', 'w') as f:
                json.dump(self.controller.config, f, indent=4)
        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "Error", f"Failed to save config: {e}")

    def _open_add_client_dialog(self):
        dlg = ClientDialog(self)
        if dlg.exec():
            data = dlg.get_data()
            self.controller.config["outputs"].append(data)
            self._save_config()
            self.refresh_clients_list()
            
    def _open_edit_client_dialog(self, client):
        dlg = ClientDialog(self, client)
        if dlg.exec():
            new_data = dlg.get_data()
            original_id = client.get("id")
            for i, c in enumerate(self.controller.config["outputs"]):
                if c.get("id") == original_id:
                    self.controller.config["outputs"][i] = new_data
                    break
            self._save_config()
            self.refresh_clients_list()
            
    def _delete_client_confirm(self, client):
        reply = QtWidgets.QMessageBox.question(
            self, 'Confirm Deletion',
            f"Are you sure you want to remove {client.get('displayName', 'this client')}?",
            QtWidgets.QMessageBox.StandardButton.Yes | QtWidgets.QMessageBox.StandardButton.No, 
            QtWidgets.QMessageBox.StandardButton.No
        )
        if reply == QtWidgets.QMessageBox.StandardButton.Yes:
            self.controller.config["outputs"] = [
                c for c in self.controller.config["outputs"] 
                if c.get("id") != client.get("id")
            ]
            self._save_config()
            self.refresh_clients_list()
    
    def refresh_clients_list(self):
        while self.clients_list_layout.count():
            item = self.clients_list_layout.takeAt(0)
            w = item.widget()
            if w: w.deleteLater()
            
        clients = self.controller.get_configured_clients()
        self.client_cards = []
        
        if len(clients) == 0:
            self.empty_state_widget.setVisible(True)
        elif self.stack.currentIndex() == 1:
            self.empty_state_widget.setVisible(False)
            for i, client in enumerate(clients):
                card = ClientCard(
                    client, 
                    edit_callback=self._open_edit_client_dialog,
                    delete_callback=self._delete_client_confirm
                )
                self.clients_list_layout.addWidget(card)
                self.client_cards.append(card)
                if i % 10 == 9:
                    QtWidgets.QApplication.processEvents()
        else:
            self.empty_state_widget.setVisible(False)
            for i, client in enumerate(clients):
                card = ClientCard(client)
                self.clients_list_layout.addWidget(card)
                self.client_cards.append(card)
                if i % 10 == 9:
                    QtWidgets.QApplication.processEvents()

    def update_dashboard(self):
        if self.scheduler:
            self.scheduler._update_countdown()
        self.update_tables()
        clients = self.controller.config.get('outputs', [])
        if len(clients) != len(self.client_cards):
             self.refresh_clients_list()
        if self.stack.currentIndex() == 1:
             for card in self.client_cards:
                 card.update_status()

    def _trigger_ping(self):
        if self.controller.scheduler and self.controller.scheduler.loop and self.controller.scheduler.loop.is_running():
            asyncio.run_coroutine_threadsafe(self.controller.get_all_output_clients(), self.controller.scheduler.loop)
        else:
            thread = threading.Thread(target=self._trigger_ping_sync, daemon=True)
            thread.start()
    
    def _trigger_ping_sync(self):
        asyncio.run(self.controller.get_all_output_clients())

    def update_tables(self):
        last_event_name = getattr(self.scheduler, 'last_event_name', "None")
        last_offset = getattr(self.scheduler, 'last_event_offset', 0.0)
        next_name = getattr(self.scheduler, 'next_event_name', "None")
        next_time = getattr(self.scheduler, 'next_event_time', "N/A")
        countdown = getattr(self.scheduler, 'next_event_countdown', "00:00:00")
        warnings_count = getattr(self.scheduler, 'total_client_warnings', 0)
        
        if self.scheduler:
            all_events = self.scheduler.grab_all_events()
            total_events = len(all_events)
        else:
            total_events = 0

        self.sched_table.setItem(0, 0, QtWidgets.QTableWidgetItem("Last Event Name(s)"))
        self.sched_table.setItem(0, 1, QtWidgets.QTableWidgetItem(str(last_event_name)))

        self.sched_table.setItem(1, 0, QtWidgets.QTableWidgetItem("Last Event Offset"))
        self.sched_table.setItem(1, 1, QtWidgets.QTableWidgetItem(f"{last_offset:+.2f}s"))
        
        self.sched_table.setItem(2, 0, QtWidgets.QTableWidgetItem("Next Event Name"))
        self.sched_table.setItem(2, 1, QtWidgets.QTableWidgetItem(next_name))
        
        self.sched_table.setItem(3, 0, QtWidgets.QTableWidgetItem("Next Event Time"))
        self.sched_table.setItem(3, 1, QtWidgets.QTableWidgetItem(next_time))
        
        self.sched_table.setItem(4, 0, QtWidgets.QTableWidgetItem("Countdown"))
        self.sched_table.setItem(4, 1, QtWidgets.QTableWidgetItem(countdown))
        
        self.sched_table.setItem(5, 0, QtWidgets.QTableWidgetItem("Client Warnings"))
        self.sched_table.setItem(5, 1, QtWidgets.QTableWidgetItem(str(warnings_count)))
        
        self.sched_table.setItem(6, 0, QtWidgets.QTableWidgetItem("Total Events"))
        self.sched_table.setItem(6, 1, QtWidgets.QTableWidgetItem(str(total_events)))

        clients = self.controller.config.get('outputs', [])
        ssh_count = 0
        sub_count = 0
        udp_count = 0
        tel_count = 0
        
        for client in clients:
            proto = client.get('protocol', 'ssh')
            if proto == 'ssh': ssh_count += 1
            elif proto == 'subprocess': sub_count += 1
            elif proto == 'udp': udp_count += 1
            elif proto == 'telnet': tel_count += 1

        stats = getattr(self.controller, 'stats', {})
        total_threads = threading.active_count()
        global connected_outputs
        uptime_str = "0:00:00"
        if hasattr(self, 'app_start_time'):
            uptime_str = str(timedelta(seconds=int(time.time() - self.app_start_time)))

        watch_points = [
            ("System Uptime", uptime_str),
            ("Active Threads", str(total_threads)),
            ("Online Clients", f"{connected_outputs} / {len(clients)}" if clients else "0"),
            ("SSH Configured", str(ssh_count)),
            ("Subprocess Clients", str(sub_count)),
            ("UDP Clients", str(udp_count)),
            ("Telnet Clients", str(tel_count)),
            ("Last Sent SSH", stats.get("ssh_last", "None")),
            ("Last Sent Subprocess", stats.get("sub_last", "None")),
            ("Last Sent UDP", stats.get("udp_last", "None")),
            ("Last Sent Telnet", stats.get("telnet_last", "why")),
        ]
        self.watch_table.setRowCount(len(watch_points))
        for i, (metric, value) in enumerate(watch_points):
            self.watch_table.setItem(i, 0, QtWidgets.QTableWidgetItem(metric))
            self.watch_table.setItem(i, 1, QtWidgets.QTableWidgetItem(value))

class SchedulerTab(QtWidgets.QWidget):
    def __init__(self, controller):
        super().__init__()
        self.controller = controller
        self.scheduler = EventSchedulerEngine(self.controller)
        self.scheduler.start()
        self.last_highlighted_row = -1
        self._setup_ui()
        self.refresh_grid()
        self.update_timer = QtCore.QTimer(self)
        self.update_timer.timeout.connect(self.update_current_time_indicator)
        self.update_timer.start(250)

    def _setup_ui(self):
        layout = QtWidgets.QVBoxLayout(self)
        controls_layout = QtWidgets.QHBoxLayout()
        self.view_selector = QtWidgets.QComboBox()
        self.view_selector.addItems(["Minute Interval", "10 Minute Interval", "Hourly", "Daily"])
        self.view_selector.currentTextChanged.connect(self.refresh_grid)
        self.view_selector.setCurrentText("Minute Interval") 
        self.day_selector = QtWidgets.QComboBox()
        self.day_selector.addItems(["Today", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"])
        self.day_selector.setCurrentText("Today") 
        self.day_selector.currentTextChanged.connect(self.refresh_grid)
        controls_layout.addWidget(QtWidgets.QLabel("Resolution:"))
        controls_layout.addWidget(self.view_selector)
        controls_layout.addSpacing(20)
        controls_layout.addWidget(QtWidgets.QLabel("Day Context:"))
        controls_layout.addWidget(self.day_selector)
        controls_layout.addStretch()
        self.refresh_btn = QtWidgets.QPushButton("⟳ Refresh Timetable")
        self.refresh_btn.setCursor(QtCore.Qt.CursorShape.PointingHandCursor)
        self.refresh_btn.clicked.connect(self.refresh_grid)
        controls_layout.addWidget(self.refresh_btn)
        self.edit_events_btn = QtWidgets.QPushButton("+ Add/Edit Event")
        self.edit_events_btn.setCursor(QtCore.Qt.CursorShape.PointingHandCursor)
        self.edit_events_btn.clicked.connect(self.open_edit_events_dialog)
        controls_layout.addWidget(self.edit_events_btn)
        layout.addLayout(controls_layout)
        self.table = QtWidgets.QTableWidget()
        self.table.setEditTriggers(QtWidgets.QAbstractItemView.EditTrigger.NoEditTriggers)
        self.table.setSelectionMode(QtWidgets.QAbstractItemView.SelectionMode.SingleSelection)
        self.table.setContextMenuPolicy(QtCore.Qt.ContextMenuPolicy.CustomContextMenu)
        self.table.customContextMenuRequested.connect(self.open_context_menu)
        self.table.setStyleSheet("""
            QTableWidget {
                background-color: #1e1e1e;
                gridline-color: #333333;
                border: 1px solid #333333;
            }
            QHeaderView::section {
                background-color: #2d2d2d;
                color: #dddddd;
                padding: 4px;
                border: 1px solid #333333;
            }
            QTableWidget::item {
                padding: 5px;
            }
            QTableWidget::item:selected {
                background-color: #2a2d3e;
            }
        """)
        self.table.horizontalHeader().setSectionResizeMode(QtWidgets.QHeaderView.ResizeMode.Stretch)
        self.table.verticalHeader().setSectionResizeMode(QtWidgets.QHeaderView.ResizeMode.Fixed)
        self.table.verticalHeader().setDefaultSectionSize(30)
        
        layout.addWidget(self.table)

    def _is_event_in_slot(self, event, row_data, client, day_context):
        event_clients = event.get('clients', [])
        if not event_clients and event.get('Category') == 'Cue Presentation' and event.get('flavor'):
             event_clients = list(event.get('flavor', {}).keys())

        client_id = client.get("id", "")
        if event_clients:
            c_name = client.get("displayName", "")
            c_star = client.get("star", "")
            match_client = False
            for ec in event_clients:
                if ec == client_id or ec == c_name or ec == c_star:
                    match_client = True
                    break
            if not match_client:
                return False
        event_days = event.get('Days', [])
        current_view_day = day_context
        if current_view_day == "Today":
             current_view_day = QtCore.QDateTime.currentDateTime().toString("dddd")
        
        if event_days and current_view_day not in event_days:
            if row_data.get('type') == 'day':
                 if row_data['day'] not in event_days:
                     return False
            else:
                 return False
        
        if row_data.get('type') == 'time':
            row_h = row_data['h']
            row_m = row_data['m']
            view_mode = self.view_selector.currentText()

            event_hours = event.get('Hours', [])
            if event_hours:
                match_hour = False
                for eh in event_hours:
                    h_val = int(eh['hour'])
                    period = eh['period']
                    
                    target_hours = []
                    if period == "AM/PM":
                        if h_val == 12:
                            target_hours.append(0)
                            target_hours.append(12)
                        elif h_val == 0:
                            target_hours.append(0)
                            target_hours.append(12)
                        else:
                            target_hours.append(h_val)
                            target_hours.append(h_val + 12)
                            
                    elif period == 'PM':
                        if h_val != 12:
                            target_hours.append(h_val + 12)
                        else:
                            target_hours.append(12)
                            
                    elif period == 'AM':
                        if h_val == 12:
                            target_hours.append(0)
                        else:
                            target_hours.append(h_val)
                    
                    if row_h in target_hours:
                        match_hour = True
                        break
                if not match_hour:
                    return False

            tm_vals = event.get('TenMinuteInterval', [])
            m_val = event.get('MinuteInterval')
            
            try:
                offset = int(m_val) if (m_val and str(m_val).strip()) else 0
            except:
                offset = 0

            bases = [int(x) for x in tm_vals] if tm_vals else [0]
            
            allowed_minutes = []
            for b in bases:
                val = b + offset
                if 0 <= val < 60:
                    allowed_minutes.append(val)
            
            match_minute = False
            
            if view_mode == "Minute Interval":
                if row_m in allowed_minutes: 
                    match_minute = True
            
            elif view_mode == "10 Minute Interval":
                for am in allowed_minutes:
                    if row_m <= am < row_m + 10:
                        match_minute = True
                        break
                        
            elif view_mode == "Hourly":
                if allowed_minutes:
                    match_minute = True

            if not match_minute:
                return False

        elif row_data.get('type') == 'day':
            pass

        return True

    def refresh_grid(self):
        self.table.clear()
        self.table.setRowCount(0)
        self.table.setColumnCount(0)
        self.last_highlighted_row = -1
        
        view_mode = self.view_selector.currentText()
        clients = self.controller.get_configured_clients()
        all_events = self.scheduler.grab_all_events()

        col_labels = []
        self.col_map = {}
        
        for i, client in enumerate(clients):
            name = client.get("displayName", client.get("star", "Unknown"))
            col_labels.append(name)
            self.col_map[i] = client
            
        self.table.setColumnCount(len(col_labels))
        self.table.setHorizontalHeaderLabels(col_labels)

        row_labels = []
        self.row_map = []
        
        def fmt_12h(h, m):
            period = "AM"
            h_12 = h
            if h == 0:
                h_12 = 12
            elif h == 12:
                period = "PM"
            elif h > 12:
                h_12 = h - 12
                period = "PM"
            return f"{h_12:02d}:{m:02d} {period}"

        now = QtCore.QDateTime.currentDateTime()
        current_day_name = now.toString("dddd")
        showing_today = (self.day_selector.currentText() == "Today") or (self.day_selector.currentText() == current_day_name)
        current_h = now.time().hour()
        current_m = now.time().minute()

        if view_mode == "Minute Interval":
            start_total_mins = 0
            if showing_today:
                cur_total = current_h * 60 + current_m
                start_total_mins = max(0, cur_total - 3)

            for total_m in range(start_total_mins, 1440):
                h = total_m // 60
                m = total_m % 60
                time_str = fmt_12h(h, m)
                row_labels.append(time_str)
                self.row_map.append({'type': 'time', 'h': h, 'm': m})

        elif view_mode == "10 Minute Interval":
            start_idx = 0
            if showing_today:
                cur_total = current_h * 60 + current_m
                cur_idx = cur_total // 10
                start_idx = max(0, cur_idx - 3)

            for idx in range(start_idx, 144):
                total_m = idx * 10
                h = total_m // 60
                m = total_m % 60
                time_str = fmt_12h(h, m)
                row_labels.append(time_str)
                self.row_map.append({'type': 'time', 'h': h, 'm': m})
                    
        elif view_mode == "Hourly":
            start_h = 0
            if showing_today:
                start_h = max(0, current_h - 3)
            for h in range(start_h, 24):
                time_str = fmt_12h(h, 0)
                row_labels.append(time_str)
                self.row_map.append({'type': 'time', 'h': h, 'm': 0})
     
        elif view_mode == "Daily":
            row_labels = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
            for day in row_labels:
                self.row_map.append({'type': 'day', 'day': day})

        self.table.setRowCount(len(row_labels))
        self.table.setVerticalHeaderLabels(row_labels)

        current_day_context = self.day_selector.currentText()
        
        self.table.setUpdatesEnabled(False)
        
        for r in range(self.table.rowCount()):
            row_data = self.row_map[r]
            
            is_10_min = False
            if row_data.get('type') == 'time' and row_data.get('m', -1) % 10 == 0:
                 is_10_min = True

            if is_10_min:
                 bg_color = QtGui.QColor("#2C2C2C")
            elif r % 2 == 0:
               bg_color = QtGui.QColor("#222222")
            else: 
               bg_color = QtGui.QColor("#1E1E1E")
            
            for c in range(self.table.columnCount()):
                client = self.col_map[c]

                matches = []
                for event in all_events:
                    if self._is_event_in_slot(event, row_data, client, current_day_context):
                        matches.append(event)
                
                item = QtWidgets.QTableWidgetItem("")
                item.setBackground(bg_color)
                
                if matches:
                    item.setBackground(QtGui.QColor("#3A4E3A"))
                    item.setData(QtCore.Qt.ItemDataRole.UserRole, True)
                    container = QtWidgets.QWidget()
                    container.setStyleSheet("background: transparent;")
                    layout = QtWidgets.QHBoxLayout(container)
                    layout.setContentsMargins(1, 1, 1, 1)
                    layout.setSpacing(1)
                    
                    for event_obj in matches:
                        name = event_obj.get('DisplayName', 'Event') or "Event"
                        lbl = QtWidgets.QLabel(name)
                        lbl.setAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)
                        lbl.setStyleSheet("""
                            background-color: transparent; 
                            border-radius: 3px; 
                            color: white; 
                            font-size: 11px; 
                            padding: 2px;
                            border: 1px solid #4A5E4A;
                        """)
                        lbl.setToolTip(name)
                        layout.addWidget(lbl)
                        
                    self.table.setCellWidget(r, c, container)
                else:
                    item.setData(QtCore.Qt.ItemDataRole.UserRole, False)
                
                self.table.setItem(r, c, item)
            
            if r % 50 == 0:
                QtWidgets.QApplication.processEvents()
        
        self.table.setUpdatesEnabled(True)
        self.update_current_time_indicator()

    def update_current_time_indicator(self):
        if not hasattr(self, 'row_map') or not self.row_map:
            return
        now = QtCore.QDateTime.currentDateTime()
        current_day_name = now.toString("dddd")
        current_h = now.time().hour()
        current_m = now.time().minute()
        
        selected_day_context = self.day_selector.currentText()
        resolution = self.view_selector.currentText()
        should_check = False
        
        if selected_day_context == "Today":
             should_check = True
        elif selected_day_context == current_day_name:
             should_check = True
        elif resolution == "Daily":
             should_check = True

        target_row = -1
        
        if should_check:
            for r, row_data in enumerate(self.row_map):
                if row_data.get('type') == 'day':
                    if row_data['day'] == current_day_name:
                        target_row = r
                        break
                elif row_data.get('type') == 'time':
                    row_h = row_data['h']
                    row_m = row_data['m']

                    if resolution == "Minute Interval":
                        if row_h == current_h and row_m == current_m:
                            target_row = r
                            break
                    elif resolution == "10 Minute Interval":
                        if row_h == current_h and row_m <= current_m < row_m + 10:
                            target_row = r
                            break
                    elif resolution == "Hourly":
                        if row_h == current_h:
                            target_row = r
                            break

        row_changed = (target_row != self.last_highlighted_row)

        if row_changed and self.last_highlighted_row != -1 and self.last_highlighted_row < self.table.rowCount():
             prev_row_data = self.row_map[self.last_highlighted_row]
             is_10_min = False
             if prev_row_data.get('type') == 'time' and prev_row_data.get('m', -1) % 10 == 0:
                 is_10_min = True

             if is_10_min:
                 zebra_color = QtGui.QColor("#2C2C2C")
             elif self.last_highlighted_row % 2 == 0:
                 zebra_color = QtGui.QColor("#222222")
             else:
                 zebra_color = QtGui.QColor("#1E1E1E")

             event_color = QtGui.QColor("#3A4E3A")
             
             for c in range(self.table.columnCount()):
                 item = self.table.item(self.last_highlighted_row, c)
                 if item:
                     if item.data(QtCore.Qt.ItemDataRole.UserRole):
                         item.setBackground(event_color)
                     else:
                         item.setBackground(zebra_color)

        if target_row != -1:
             base_color = QtGui.QColor("#065F46")
             progress_color = QtGui.QColor("#022c22")
             current_s = now.time().second()
             current_ms = now.time().msec()
             ratio = (current_s + current_ms / 1000.0) / 60.0
             
             gradient = QtGui.QLinearGradient(0, 0, 1, 0)
             gradient.setCoordinateMode(QtGui.QGradient.CoordinateMode.ObjectBoundingMode)
             
             gradient.setColorAt(0, progress_color)
             if ratio > 0:
                 gradient.setColorAt(ratio, progress_color)
             if ratio < 1.0:
                 gradient.setColorAt(min(1.0, ratio + 0.001), base_color)
                 gradient.setColorAt(1, base_color)
             
             prog_brush = QtGui.QBrush(gradient)
             
             for c in range(self.table.columnCount()):
                 item = self.table.item(target_row, c)
                 if item:
                     item.setBackground(prog_brush)
             if row_changed:
                 scroll_row = max(0, target_row - 3)
                 item_to_scroll_to = self.table.item(scroll_row, 0)
                 if item_to_scroll_to:
                     self.table.scrollToItem(item_to_scroll_to, QtWidgets.QAbstractItemView.ScrollHint.PositionAtTop)
                 self.table.clearSelection()
        
        self.last_highlighted_row = target_row

    def open_add_event_dialog(self):
        clients = self.controller.get_configured_clients()
        all_events = self.scheduler.grab_all_events()
        dialog = EventDialog(self, clients=clients, all_events=all_events)
        if dialog.exec() == QtWidgets.QDialog.DialogCode.Accepted:
            self._handle_event_dialog_accept(dialog)

    def open_edit_events_dialog(self):
        self.open_add_event_dialog()
    
    def _handle_event_dialog_accept(self, dialog):
        data = dialog.get_data()
        original_name = dialog.original_name
        
        success = False
        if original_name:
             if self.scheduler.edit_event(original_name, data):
                 success = True
                 dialog.original_name = data.get('DisplayName')
        else:
             if self.scheduler.write_event(data):
                 success = True
                 dialog.original_name = data.get('DisplayName')
        
        if success:
             self.refresh_grid()
    
    def _handle_event_dialog_apply(self, dialog):
        data = dialog.get_data()
        original_name = dialog.original_name
        load_offset = data.get('LoadOffset', 0)
        run_offset = data.get('RunOffset', 0)
        
        success = False
        fail_reason = "Ensure all fields are valid."
        if original_name:
             if self.scheduler.edit_event(original_name, data):
                 success = True
                 dialog.original_name = data.get('DisplayName')
        else:
             if self.scheduler.write_event(data):
                 success = True
                 dialog.original_name = data.get('DisplayName')
        
        if load_offset > run_offset:
            fail_reason = "LOAD offset cannot occur after run offset. Please adjust LOAD offset to be lower than RUN offset."
            success = False

        if success:
             self.refresh_grid()
             all_events = self.scheduler.grab_all_events()
             dialog.all_events = all_events
             current_text = dialog.event_selector.currentText()
             dialog.event_selector.blockSignals(True)
             dialog.event_selector.clear()
             dialog.event_selector.addItem("<New Event>")
             sorted_evs = sorted(all_events, key=lambda x: x.get('DisplayName', ''))
             for e in sorted_evs:
                dialog.event_selector.addItem(e.get('DisplayName', 'Un-named'))
             dialog.event_selector.setCurrentText(data.get('DisplayName', current_text))
             dialog.event_selector.blockSignals(False)

             QtWidgets.QMessageBox.information(dialog, "Success", "Event applied!")
        else:
             QtWidgets.QMessageBox.warning(dialog, "Error", "Failed to apply event: " + fail_reason)

    def open_context_menu(self, position):
        item = self.table.itemAt(position)
        
        event_names = []
        widget = None
        if item:
             widget = self.table.cellWidget(item.row(), item.column())
        
        if widget:
            layout = widget.layout()
            if layout:
                for i in range(layout.count()):
                    w = layout.itemAt(i).widget()
                    if isinstance(w, QtWidgets.QLabel):
                        text = w.text()
                        if text:
                            event_names.append(text)
        elif item:
            text = item.text().strip()
            if text:
                event_names.extend([t.strip() for t in text.split("\n") if t.strip()])
        
        if not event_names:
            return
        
        menu = QtWidgets.QMenu()
        
        for name in event_names:
            menu.addAction(f"Force Execute '{name}'", lambda checked=False, n=name: self.force_execute_event(n))
            menu.addSeparator()
            menu.addAction(f"Edit '{name}'", lambda checked=False, n=name: self.edit_event(n))
            menu.addAction(f"Delete '{name}'", lambda checked=False, n=name: self.delete_event(n))
            if len(event_names) > 1:
                menu.addSeparator()

        menu.exec(self.table.viewport().mapToGlobal(position))

    def force_execute_event(self, display_name):
        all_events = self.scheduler.grab_all_events()
        event_data = next((e for e in all_events if e.get('DisplayName') == display_name), None)
        
        if not event_data:
            QtWidgets.QMessageBox.warning(self, "Error", f"Event '{display_name}' not found.")
            return

        reply = QtWidgets.QMessageBox.question(self, "Confirm Force Execute", 
                                             f"Are you sure you want to immediately execute '{display_name}' on all mapped clients?",
                                             QtWidgets.QMessageBox.StandardButton.Yes | QtWidgets.QMessageBox.StandardButton.No)
        
        if reply == QtWidgets.QMessageBox.StandardButton.Yes:
            if self.scheduler.loop and self.scheduler.loop.is_running():
                asyncio.run_coroutine_threadsafe(
                    self.scheduler._execute_single_event(event_data),
                    self.scheduler.loop
                )
                QtWidgets.QMessageBox.information(self, "Executed", f"Commands for '{display_name}' have been dispatched.")
            else:
                QtWidgets.QMessageBox.warning(self, "Error", "Scheduler loop is not running.")

    def edit_event(self, display_name):
        all_events = self.scheduler.grab_all_events()
        event_data = next((e for e in all_events if e.get('DisplayName') == display_name), None)
        
        if not event_data:
            QtWidgets.QMessageBox.warning(self, "Error", f"Event '{display_name}' not found.")
            return

        clients = self.controller.get_configured_clients()
        dialog = EventDialog(self, event_data=event_data, clients=clients, all_events=all_events)
        if dialog.exec() == QtWidgets.QDialog.DialogCode.Accepted:
            self._handle_event_dialog_accept(dialog)

    def delete_event(self, display_name):
        reply = QtWidgets.QMessageBox.question(self, "Confirm Delete", 
                                             f"Are you sure you want to delete '{display_name}'?",
                                             QtWidgets.QMessageBox.StandardButton.Yes | QtWidgets.QMessageBox.StandardButton.No)
        
        if reply == QtWidgets.QMessageBox.StandardButton.Yes:
            if self.scheduler.delete_event(display_name):
                self.refresh_grid()
            else:
                QtWidgets.QMessageBox.warning(self, "Error", "Failed to delete event.")


class user_interface:
    def __init__(self, controller):
        self.controller = controller
        self.client_cards = []

    def build_window(self):
        app = QtWidgets.QApplication(sys.argv)
        QtGui.QFontDatabase.addApplicationFont(os.path.join(os.path.dirname(__file__), "fonts", "HostGrotesk-VariableFont_wght.ttf"))
        QtGui.QFontDatabase.addApplicationFont(os.path.join(os.path.dirname(__file__), "fonts", "HostGrotesk-Italic-VariableFont_wght.ttf"))
        window = QtWidgets.QMainWindow()
        window.setWindowTitle("StarScheduler")
        window.setGeometry(100, 100, window_width, window_height)
        
        icon_path = os.path.join(os.path.dirname(__file__), "icon.png")
        if os.path.exists(icon_path):
            window.setWindowIcon(QtGui.QIcon(icon_path))
        else:
            logger.warning(f"Icon file not found at {icon_path}")
        
        window.show()
        return app, window

    def build_header_widget(self):
        header_widget = QtWidgets.QWidget()
        header_layout = QtWidgets.QHBoxLayout()
        brand_widget = QtWidgets.QWidget()
        brand_layout = QtWidgets.QHBoxLayout(brand_widget)
        brand_layout.setContentsMargins(0, 0, 0, 0)
        brand_layout.setSpacing(10)
        title_img = QtWidgets.QLabel()
        meteo_logo = QtWidgets.QLabel()
        brand_layout.addWidget(title_img)
        brand_layout.addWidget(meteo_logo)
        brand_layout.addStretch()
        
        img_path = os.path.join(os.path.dirname(__file__), "img", "starscheduler_wordmark.png")
        meteo_logo_path = os.path.join(os.path.dirname(__file__), "img", "meteochannel.png")
        mid_logo_path = os.path.join(os.path.dirname(__file__), "img", "midscheduler.png")
        import random
        if random.randint(1, 8) == 1 and os.path.exists(mid_logo_path):
            img_path = mid_logo_path
        if os.path.exists(img_path) and os.path.exists(meteo_logo_path):
            pixmap = QtGui.QPixmap(img_path)
            meteo_pixmap = QtGui.QPixmap(meteo_logo_path)
            if not meteo_pixmap.isNull():
                scaled_meteo = meteo_pixmap.scaledToWidth(
                    int(meteo_pixmap.width() * 0.25),
                    QtCore.Qt.TransformationMode.SmoothTransformation
                )
                meteo_logo.setPixmap(scaled_meteo)
            else:
                logger.warning(f"Failed to load image at {meteo_logo_path}")
            
            if not pixmap.isNull():
                scaled_pixmap = pixmap.scaledToWidth(
                    int(pixmap.width() * 0.15),
                    QtCore.Qt.TransformationMode.SmoothTransformation
                )
                title_img.setPixmap(scaled_pixmap)
            else:
                logger.warning(f"Failed to load image at {img_path}")
        else:
            logger.warning(f"Image file not found at {img_path}")
        
        header_layout.addWidget(brand_widget)
        labels_layout = QtWidgets.QVBoxLayout()
        time_label = QtWidgets.QLabel()
        if os.name == 'nt':
            hostname = os.environ.get('COMPUTERNAME', 'Unknown')
        else:
            hostname = os.environ.get('HOSTNAME', 'Unknown')
        hostname_label = QtWidgets.QLabel(f"Hostname: {hostname}")
        outputs_label = QtWidgets.QLabel(f"Concurrent Outputs: {connected_outputs}")
        
        main_status_labels: list[QtWidgets.QLabel] = [
            time_label,
            hostname_label,
            outputs_label
        ]
        
        for label in main_status_labels:
            label.setAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)
            labels_layout.addWidget(label)

        def update_labels():
            current_time = QtCore.QDateTime.currentDateTime().toString("yyyy-MM-dd hh:mm:ss")
            time_label.setText(f"Time: {current_time}")
            outputs_label.setText(f"Concurrent Outputs: {connected_outputs}")
        
        update_labels()

        timer = QtCore.QTimer()
        timer.timeout.connect(update_labels)
        timer.start(1000)
        header_widget.timer = timer
        header_layout.addLayout(labels_layout)
        header_widget.setLayout(header_layout)
        return header_widget

    def build_status_widget(self):
        return HomeTab(self.controller, self)

    def build_clients_tab(self):
        clients_widget = QtWidgets.QWidget()
        clients_widget.setStyleSheet("background-color: #121212;")
        main_layout = QtWidgets.QVBoxLayout()
        main_layout.setSpacing(20)
        main_layout.setContentsMargins(30, 30, 30, 30)
        header_layout = QtWidgets.QHBoxLayout()
        title_label = QtWidgets.QLabel("Output Clients")
        title_label.setStyleSheet("""
            font-size: 24px;
            font-weight: 600;
            color: #E0E0E0;
            font-family: 'Host Grotesk', sans-serif;
        """)
        add_btn = QtWidgets.QPushButton("  Add New Client")
        add_btn.setCursor(QtCore.Qt.CursorShape.PointingHandCursor)
        add_btn.setText("+ New Client")
        add_btn.clicked.connect(self.add_client_dialog)
        header_layout.addWidget(title_label)
        header_layout.addStretch()
        header_layout.addWidget(add_btn)
        main_layout.addLayout(header_layout)
        scroll_area = QtWidgets.QScrollArea()
        scroll_area.setWidgetResizable(True)
        scroll_area.setFrameShape(QtWidgets.QFrame.Shape.NoFrame)
        scroll_area.setStyleSheet("background: transparent;")
        scroll_content = QtWidgets.QWidget()
        scroll_content.setStyleSheet("background: transparent;")
        list_layout = QtWidgets.QVBoxLayout()
        list_layout.setSpacing(15)
        list_layout.setContentsMargins(0, 0, 0, 0)
        list_layout.setAlignment(QtCore.Qt.AlignmentFlag.AlignTop)
        clients = self.controller.get_configured_clients()
        self.client_cards = []
        for client in clients:
            card = ClientCard(client)
            list_layout.addWidget(card)
            self.client_cards.append(card)
        scroll_content.setLayout(list_layout)
        scroll_area.setWidget(scroll_content)
        main_layout.addWidget(scroll_area)
        self.status_timer = QtCore.QTimer()
        self.status_timer.timeout.connect(self.update_client_statuses)
        self.status_timer.start(1000)
        clients_widget.setLayout(main_layout)
        return clients_widget

    def update_client_statuses(self):
        for card in self.client_cards:
            card.update_status()

    def add_client_dialog(self):
        dialog = QtWidgets.QDialog()
        dialog.setWindowTitle("Add Output Client")
        dialog.setModal(True)
        dialog.setFixedWidth(500)
        dialog.setStyleSheet("""
            QDialog {
                background-color: #1e1e1e;
            }
            QLabel {
                color: #ffffff;
                font-size: 12px;
            }
            QLineEdit, QComboBox {
                background-color: #2d2d2d;
                color: #ffffff;
                border: 1px solid #404040;
                padding: 8px;
                border-radius: 4px;
                font-size: 12px;
            }
            QPushButton {
                padding: 8px 20px;
                border-radius: 4px;
                font-size: 12px;
            }
        """)
        
        layout = QtWidgets.QVBoxLayout()
        layout.setSpacing(15)
        layout.setContentsMargins(20, 20, 20, 20)
        title = QtWidgets.QLabel("Add New Output Client")
        title.setStyleSheet("font-size: 16px; font-weight: bold; color: #ffffff; margin-bottom: 10px;")
        layout.addWidget(title)
        form_layout = QtWidgets.QFormLayout()
        form_layout.setSpacing(10)
        star_type_combo = QtWidgets.QComboBox()
        valid_types = [k for k in sorted(STAR_NAMES.keys()) if k not in ["wsxl", "ws4000", "i2"]]
        star_type_combo.addItems(valid_types)
        form_layout.addRow("Star Type:", star_type_combo)
        star_type_combo.addItems(valid_types)
        form_layout.addRow("Star Type:", star_type_combo)
        protocol_combo = QtWidgets.QComboBox()
        protocol_combo.addItem("ssh", "ssh")
        protocol_combo.addItem("subprocess", "subprocess")
        protocol_combo.addItem("Multicast UDP (Experimental)", "udp")
        protocol_combo.addItem("Telnet (Legacy)", "telnet")
        form_layout.addRow("Protocol:", protocol_combo)
        conn_fields_widget = QtWidgets.QWidget()
        conn_layout = QtWidgets.QFormLayout()
        conn_layout.setContentsMargins(0, 5, 0, 0)
        conn_layout.setSpacing(10)
        hostname_input = QtWidgets.QLineEdit()
        hostname_input.setPlaceholderText("e.g. 192.168.1.100 or 224.1.1.77")
        conn_layout.addRow("Hostname/IP:", hostname_input)
        port_input = QtWidgets.QLineEdit()
        port_input.setText("22")
        conn_layout.addRow("Port:", port_input)
        username_input = QtWidgets.QLineEdit()
        username_input.setPlaceholderText("SSH username")
        conn_layout.addRow("Username:", username_input)
        password_input = QtWidgets.QLineEdit()
        password_input.setEchoMode(QtWidgets.QLineEdit.EchoMode.Password)
        password_input.setPlaceholderText("SSH password")
        conn_layout.addRow("Password:", password_input)
        su_input = QtWidgets.QLineEdit()
        su_input.setPlaceholderText("e.g., dgadmin")
        conn_layout.addRow("Substitute User (su):", su_input)
        conn_fields_widget.setLayout(conn_layout)
        form_layout.addRow(conn_fields_widget)
        layout.addLayout(form_layout)
        def update_ui_state():
            star = star_type_combo.currentText()
            proto = protocol_combo.currentData()
            if proto == "subprocess":
                conn_fields_widget.setVisible(False)
            else:
                conn_fields_widget.setVisible(True)
                is_ssh = (proto == "ssh")
                is_udp = (proto == "udp")
                is_telnet = (proto == "telnet")
                has_creds = is_ssh or is_telnet
                username_input.setVisible(has_creds)
                conn_layout.labelForField(username_input).setVisible(has_creds)
                password_input.setVisible(has_creds)
                conn_layout.labelForField(password_input).setVisible(has_creds)
                
                is_i1 = (star == "i1")
                show_su = is_i1 and has_creds
                su_input.setVisible(show_su)
                if conn_layout.labelForField(su_input):
                    conn_layout.labelForField(su_input).setVisible(show_su)
                
                if show_su and not su_input.text():
                    su_input.setText("dgadmin")
                
                curr_port = port_input.text()
                if is_udp and (curr_port == "22" or curr_port == "23" or not curr_port):
                    port_input.setText("7787")
                elif is_telnet and (curr_port == "22" or curr_port == "7787" or not curr_port):
                    port_input.setText("23")
                elif is_ssh and (curr_port == "7787" or curr_port == "23" or not curr_port):
                    port_input.setText("22")
                
                curr_host = hostname_input.text()
                if is_udp and not curr_host:
                    hostname_input.setText("224.1.1.77")

        star_type_combo.currentTextChanged.connect(update_ui_state)
        protocol_combo.currentIndexChanged.connect(update_ui_state)

        update_ui_state()

        footnote_text = (
            "- Subprocess can be used with exec.exe, which is available on all IntelliStar 2 systems\n"
            "- SSH is supported on all Stars.\n"
            "- Although UDP communication is currently experimental, it should be able to work on all IntelliStar systems.\n"
            "- Telnet is supported on WeatherStar XL, IntelliStar and IntelliStar 2s running Windows 7 or a third-party Telnet server otherwise."
        )
        footnote_label = QtWidgets.QLabel(footnote_text)
        footnote_label.setStyleSheet("color: #888888; font-size: 11px; font-style: italic; margin: 10px 0;")
        footnote_label.setWordWrap(True)
        layout.addWidget(footnote_label)

        button_layout = QtWidgets.QHBoxLayout()
        button_layout.addStretch()
        
        cancel_btn = QtWidgets.QPushButton("Cancel")
        cancel_btn.clicked.connect(dialog.reject)
        
        add_btn = QtWidgets.QPushButton("+ Add Output Client")
        add_btn.clicked.connect(lambda: self.save_new_client(
            dialog,
            star_type_combo.currentText(),
            hostname_input.text(),
            port_input.text(),
            username_input.text(),
            password_input.text(),
            protocol_combo.currentData(),
            su_input.text()
        ))
        
        button_layout.addWidget(cancel_btn)
        button_layout.addWidget(add_btn)
        layout.addLayout(button_layout)
        dialog.setLayout(layout)
        dialog.exec()
    
    def save_new_client(self, dialog, star_type, hostname, port, username, password, protocol, su):
        if protocol == "ssh" and (not hostname or not username or not password):
            msg = QtWidgets.QMessageBox()
            msg.setIcon(QtWidgets.QMessageBox.Icon.Warning)
            msg.setWindowTitle("Validation Error")
            msg.setText("Please fill in all required fields for SSH.")
            msg.exec()
            return
        elif protocol == "telnet" and not hostname:
            msg = QtWidgets.QMessageBox()
            msg.setIcon(QtWidgets.QMessageBox.Icon.Warning)
            msg.setWindowTitle("Validation Error")
            msg.setText("Please fill in Hostname/IP for Telnet.")
            msg.exec()
            return
        elif protocol == "udp" and (not hostname or not port):
            msg = QtWidgets.QMessageBox()
            msg.setIcon(QtWidgets.QMessageBox.Icon.Warning)
            msg.setWindowTitle("Validation Error")
            msg.setText("Please fill in Hostname/IP and Port for UDP.")
            msg.exec()
            return
        
        new_client = {
            "id": f"{star_type}_{hostname.replace('.', '_')}",
            "star": star_type,
            "displayName": STAR_NAMES.get(star_type, "Unknown"),
            "protocol": protocol,
            "credentials": {
                "hostname": hostname,
                "user": username,
                "password": password,
                "port": int(port) if port.isdigit() else 22,
                "su": su if su and star_type == "i1" else None
            }
        }
        self.controller.config["outputs"].append(new_client)
        try:
            with open('user/config.json', 'w') as f:
                json.dump(self.controller.config, f, indent=4)
            
            msg = QtWidgets.QMessageBox()
            msg.setIcon(QtWidgets.QMessageBox.Icon.Information)
            msg.setWindowTitle("Success")
            msg.setText(f"Client added successfully!\n\nRestart the application to connect to the new client.")
            msg.exec()
            
            dialog.accept()
        except Exception as e:
            msg = QtWidgets.QMessageBox()
            msg.setIcon(QtWidgets.QMessageBox.Icon.Critical)
            msg.setWindowTitle("Error")
            msg.setText(f"Failed to save client: {str(e)}")
            msg.exec()

    def build_main_widget(self):
        tabs = QtWidgets.QTabWidget()
        self.home_tab = self.build_status_widget()
        tabs.addTab(self.home_tab, "Home")
        self.scheduler_tab = SchedulerTab(self.controller)
        tabs.addTab(self.scheduler_tab, "Scheduler")
        if isinstance(self.home_tab, HomeTab):
            self.home_tab.set_scheduler(self.scheduler_tab.scheduler)
        self.quick_tab = QuickTimeEventTab(self.controller)
        tabs.addTab(self.quick_tab, "Quick Time Event")
        return tabs

    def build_output_widget(self):
        output_widget = QtWidgets.QTextEdit()
        output_widget.setReadOnly(True)
        output_widget.setMaximumHeight(150)
        output_widget.setStyleSheet("""
            QTextEdit {
                background-color: #0d0d0d;
                color: #00ff00;
                font-family: 'Courier New', monospace;
                font-size: 10px;
            }
        """)
        return output_widget

class star_controller:
    def __init__(self):
        with open('user/config.json', 'r') as f:
            self.config = json.load(f)
        load_performance_config(self.config)
        self.log_proxy = LogSignalProxy()
        self.client_manager = ClientManager(self)
        self.client_manager.set_log_callback(self._on_log)
        self.scheduler = None
        self.stats = {}
        self.connection_registry = None
    
    def init_persistent_connections(self, async_loop=None):
        clients = self.get_configured_clients()
        if not clients:
            logger.warning("No clients configured for persistent connections")
            return
        
        self.connection_registry = provision.get_connection_registry()
        self.connection_registry.start(clients, async_loop)
        logger.info(f"Persistent connections initialized for {len(clients)} clients")

    def _on_log(self, client_id, text):
        self.log_proxy.log_received.emit(client_id, text)
    
    async def get_all_output_clients(self) -> list:
        global connected_outputs, connected_outputs_data
        
        clients = self.config.get("outputs", [])
        
        async def check_client(client):
            protocol = client.get("protocol", "ssh")
            star_type = client.get("star")
            creds = client.get('credentials', {})
            
            if protocol == "ssh":
                try:
                    stdout, stderr = await provision.execute_ssh_command(
                        hostname=creds.get("hostname"), 
                        user=creds.get("user"), 
                        password=creds.get("password"),
                        port=creds.get("port", 22), 
                        command="echo 'connected'",
                        timeout=5.0
                    )
                    if stderr or "connected" not in stdout:
                        return None
                    return {
                        "id": client.get("id"),
                        "hostname": creds.get("hostname"),
                        "star_type": star_type,
                        "protocol": protocol,
                        "last_ping": datetime.now().strftime("%H:%M:%S")
                    }
                except Exception:
                    return None
                    
            elif protocol == "subprocess":
                 if star_type.startswith("i2"):
                    the_thing_that_shows_when_i2service_isnt_running = "Could not connect to net.tcp://localhost:8082/ExecutionerWCFService/."
                    
                    if os.name == "nt":
                        stdout, stderr = await provision.subproc_cancel_i2_pres(PresentationId="1")
                        if the_thing_that_shows_when_i2service_isnt_running in stdout or the_thing_that_shows_when_i2service_isnt_running in stderr:
                            return None
                    else:
                        logger.info("This isn't Windows... How are you even running I2 on this thing???????")
                        return None
                    return {
                        "id": client.get("id"),
                        "hostname": creds.get("hostname", "localhost"),
                        "star_type": star_type,
                        "protocol": protocol,
                         "last_ping": datetime.now().strftime("%H:%M:%S")
                    }

            elif protocol == "udp":
                 return {
                    "id": client.get("id"),
                    "hostname": creds.get("hostname", "224.1.1.77"),
                    "star_type": star_type,
                    "protocol": protocol,
                    "last_ping": datetime.now().strftime("%H:%M:%S")
                 }

            elif protocol == "telnet":
                try:
                    stdout, stderr = await provision.execute_telnet_command(
                        hostname=creds.get("hostname"),
                        port=creds.get("port", 23),
                        command="echo connected",
                        su=creds.get("su") if star_type == "i1" else None,
                        timeout=5.0
                    )
                    if stderr:
                        return None
                    return {
                        "id": client.get("id"),
                        "hostname": creds.get("hostname"),
                        "star_type": star_type,
                        "protocol": protocol,
                        "last_ping": datetime.now().strftime("%H:%M:%S")
                    }
                except Exception:
                    return None
            
            return None
        results = await asyncio.gather(*[check_client(c) for c in clients])
        new_data = [r for r in results if r is not None]
        connected_outputs = len(new_data)
        connected_outputs_data = new_data
        return clients
    
    def get_configured_clients(self) -> list:
        clients = self.config.get("outputs", [])
        for c in clients:
            if not c.get('id'):
                host = c.get('credentials', {}).get('hostname', 'unknown').replace('.', '_')
                c['id'] = f"{c.get('star', 'unknown')}_{host}"
        return clients
    
    def get_client_status(self, hostname: str) -> dict:
        clients = self.get_configured_clients()
    
class ClientWorker:
    @classmethod
    def get_shared_executor(cls) -> ThreadPoolExecutor:
        return provision._get_executor()
    
    @classmethod
    def shutdown_shared_executor(cls):
        pass
    
    def __init__(self, client_id, controller):
        self.client_id = client_id
        self.controller = controller
        self.running = True
    
    def submit_task(self, task):
        if not self.running:
            return
        self.get_shared_executor().submit(self._execute_safe, task)

    def _execute_safe(self, task):
        try:
            task()
        except Exception as e:
            logger.error(f"ClientWorker-{self.client_id} Error: {e}")
    
    def stop(self):
        self.running = False

class ClientManager:
    def __init__(self, controller=None):
        self.workers = {}
        self.controller = controller
        self.lock = threading.Lock()
        self.logs = {}
        self.log_updated = QtCore.pyqtSignal(str, str) if QtCore and hasattr(QtCore, 'pyqtSignal') else None
    def set_log_callback(self, callback):
        self.log_callback = callback
    def get_worker(self, client_id):
        with self.lock:
            if client_id not in self.workers:
                self.workers[client_id] = ClientWorker(client_id, self.controller)
            return self.workers[client_id]
    def dispatch(self, client_id, func, *args, **kwargs):
        worker = self.get_worker(client_id)
        runtime_error_strings = [
            "Neither a playlist nor a copy split was generated.",
            "Exception:",
            "Error:"
        ]
        i2service_dies_lmao = "Could not connect to net.tcp://localhost:8082/ExecutionerWCFService/"
        STUPID_FUCKING_DUMBASS_SHITTY_AHH_ISTAR1_CORBA_ERROR_THAT_DOESNT_MEAN_SHIT = [
            "'NoneType' object is not callable",
            "twccommon.corba.CosEventChannelAdmin._objref_ProxyPushConsumer instance at 0x852d48c>> ignored"
        ]
        def wrapped_task():
            try:
                res = func(*args, **kwargs)
                if res and isinstance(res, tuple) and len(res) == 2:
                    stdout, stderr = res
                    output = ""
                    if any(err_str in stdout for err_str in runtime_error_strings) or any(err_str in stderr for err_str in runtime_error_strings) and client_id.startswith("i2") :
                        logger.warning(f"I2Service or Viz runtime error detected in output for client {client_id}")
                        output += f"\x1b[1;31m{stdout}\x1b[0m"
                    elif i2service_dies_lmao in stdout or i2service_dies_lmao in stderr and client_id.startswith("i2"):
                        logger.warning(f"I2Service connection error detected for client {client_id}. Please verify that the service is running on the target output client.")
                        output += f"\x1b[1;33m{stdout}\x1b[0m"
                    elif client_id.startswith("i1") and any(err_str in stdout for err_str in STUPID_FUCKING_DUMBASS_SHITTY_AHH_ISTAR1_CORBA_ERROR_THAT_DOESNT_MEAN_SHIT):
                        logger.debug('STUPID FUCKING TWC INTELLISTAR CORBA RUNTIME ERROR DETECTED! CRASHING THE FUCK OUT! CALLING ALL MIST WEATHER MEDIA STAR, CREATIVE, MODERATION, AND DEVELOPMENT TEAMS!')
                        logger.debug('INITIATING OPERATION YELL AT 3D CREW FOR LEAVING THIS STUPID THING IN PRODUCTION ISTARD.')
                    else:
                        output += f"[STDOUT]\n{stdout}\n"
                        if self.controller.config['system'].get("logSTDOUT", True):
                            logger.info(f"[{client_id}] STDOUT: {stdout.strip()}")
                    if stderr: 
                        output += f"[STDERR]\n{stderr}\n"
                        logger.warning(f"[{client_id}] STDERR: {stderr.strip()}")
                    if output:
                        self.log_output(client_id, output)
                return res
            except Exception as e:
                logger.error(f"[{client_id}] Execution Error: {e}")
                self.log_output(client_id, f"Error: {str(e)}")

        worker.submit_task(wrapped_task)

    def log_output(self, client_id, text):
        if client_id not in self.logs:
            self.logs[client_id] = []
        timestamp = datetime.now().strftime("%H:%M:%S")
        formatted = f"[{timestamp}] {text}"
        self.logs[client_id].append(formatted)
        if hasattr(self, 'log_callback') and self.log_callback:
            self.log_callback(client_id, formatted)

class LogSignalProxy(QtCore.QObject):
    log_received = QtCore.pyqtSignal(str, str)


class EventSchedulerEngine:

    def __init__(self, controller):
        self.controller = controller
        self.timetable_file = os.path.join(os.path.dirname(__file__), "user", "timetable.xml")
        self._scheduler: Optional[BackgroundScheduler] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None
        self._running = False
        self._cached_events: List[Dict] = []
        self._cached_mtime: float = 0
        self._cache_lock = threading.Lock()
        self.last_event_name = "None"
        self.last_event_time = "N/A"
        self.last_event_offset = 0.0
        self.next_event_name = "None"
        self.next_event_time = "N/A"
        self.next_event_countdown = "00:00:00"
        self.next_event_dt: Optional[datetime] = None
        self.next_check_time = "Scanning..."
        self.startup_event_fired = False
        self.total_client_warnings = 0
        self._event_jobs: Dict[str, List[str]] = {}
        self._countdown_job_id: Optional[str] = None
        
    def start(self):
        if self._running:
            logger.warning("Scheduler already running")
            return
            
        self._running = True
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(
            target=self._loop.run_forever,
            daemon=True,
            name="SchedulerEventLoop"
        )
        self._thread.start()
        self._scheduler = BackgroundScheduler(
            timezone=local_timezone,
            job_defaults={
                'coalesce': True,
                'max_instances': 1,
                'misfire_grace_time': 30
            }
        )
        self._scheduler.add_listener(
            self._on_job_event,
            EVENT_JOB_EXECUTED | EVENT_JOB_ERROR | EVENT_JOB_MISSED
        )
        self._scheduler.start()
        self._do_initial_setup()
        self._scheduler.add_job(
            self._check_timetable_changes,
            'interval',
            seconds=5,
            id='timetable_watcher',
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=10
        )
        self._countdown_job_id = None
        
        logger.info("APScheduler engine started")
        
    def stop(self):
        self._running = False
        if self._scheduler:
            try:
                self._scheduler.shutdown(wait=False)
            except Exception:
                pass
        if self._loop:
            self._loop.call_soon_threadsafe(self._loop.stop)
        logger.info("APScheduler engine stopped")
        
    def _do_initial_setup(self):
        try:
            self.controller.scheduler = self
            self.controller.init_persistent_connections(self._loop)
        except Exception as e:
            logger.warning(f"Failed to initialize persistent connections: {e}")
        self._reload_events()
        self._schedule_all_events()
        if not self.startup_event_fired:
            try:
                future = asyncio.run_coroutine_threadsafe(
                    self._fire_startup_events(), 
                    self._loop
                )
                future.result(timeout=30)
            except Exception as e:
                logger.error(f"Error firing startup events: {e}")
            self.startup_event_fired = True
            
    async def _fire_startup_events(self):
        events = self.grab_all_events()
        tasks = []
        for event in events:
            if event.get('RunAtStartup', False) and event.get('Enabled', True):
                logger.info(f"Firing startup event: {event.get('DisplayName')}")
                target_time = datetime.now()
                tasks.append(self._execute_event(event, target_time=target_time, is_startup=True))
        
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
            
    def _check_timetable_changes(self):
        try:
            if os.path.exists(self.timetable_file):
                current_mtime = os.path.getmtime(self.timetable_file)
                if current_mtime != self._cached_mtime:
                    logger.info("Timetable changed, reloading events...")
                    self._reload_events()
                    self._schedule_all_events()
        except Exception as e:
            logger.error(f"Error checking timetable changes: {e}")
            
    def _reload_events(self):
        try:
            if not os.path.exists(self.timetable_file):
                with self._cache_lock:
                    self._cached_events = []
                return
                
            current_mtime = os.path.getmtime(self.timetable_file)
            tree = ET.parse(self.timetable_file)
            root = tree.getroot()
            events = []
            
            for event_elem in root.findall('event'):
                event = self._parse_event_element(event_elem)
                events.append(event)
                
            with self._cache_lock:
                self._cached_events = events
                self._cached_mtime = current_mtime
                
            logger.debug(f"Loaded {len(events)} events from timetable")
            
        except Exception as e:
            logger.error(f"Error loading timetable: {e}")
            
    def _parse_event_element(self, event_elem) -> Dict:
        event = {}
        for tag in ['DisplayName', 'CustomCommand', 'MinuteInterval', 'Category', 'TargetID']:
            el = event_elem.find(tag)
            event[tag] = el.text if el is not None and el.text else ""
        for tag, default in [('Enabled', False), ('RunAtStartup', False)]:
            el = event_elem.find(tag)
            event[tag] = (el.text.lower() == 'true') if el is not None and el.text else default
        for tag, child_tag in [
            ('TenMinuteInterval', 'TenMinute'),
            ('Days', 'Day'),
            ('Weeks', 'Week'),
            ('Months', 'Month'),
            ('clients', 'client')
        ]:
            container = event_elem.find(tag)
            event[tag if tag != 'clients' else 'clients'] = [
                e.text for e in container.findall(child_tag) if e.text
            ] if container is not None else []
        hours_elem = event_elem.find('Hours')
        event['Hours'] = []
        if hours_elem is not None:
            for h_el in hours_elem.findall('Hour'):
                event['Hours'].append({
                    'hour': h_el.text,
                    'period': h_el.get('period', 'AM/PM')
                })
        cc_elem = event_elem.find('ClientConfigs')
        event['client_config'] = {}
        if cc_elem is not None:
            for cf in cc_elem.findall('ClientConfig'):
                raw_id = cf.get('id')
                client_ref = cf.get('client')
                config = {
                    'action': cf.find('Action').text if cf.find('Action') is not None else "LoadRun",
                    'flavor': cf.find('Flavor').text if cf.find('Flavor') is not None else "",
                    'presentation_id': cf.find('PresentationID').text if cf.find('PresentationID') is not None else "",
                    'duration': cf.find('Duration').text if cf.find('Duration') is not None else "",
                    'logo': cf.find('Logo').text if cf.find('Logo') is not None else "",
                    'command': cf.find('Command').text if cf.find('Command') is not None else "",
                    'su': cf.find('SU').text if cf.find('SU') is not None else "",
                    'ldl_state': cf.find('LDLState').text if cf.find('LDLState') is not None else ""
                }
                sep_el = cf.find('SeparateLoadRun')
                config['separate_load_run'] = (sep_el.text.lower() == 'true') if sep_el is not None and sep_el.text else False
                load_off_el = cf.find('LoadOffset')
                try:
                    config['load_offset'] = int(load_off_el.text) if load_off_el is not None and load_off_el.text else -20
                except Exception:
                    config['load_offset'] = -20
                run_off_el = cf.find('RunOffset')
                try:
                    config['run_offset'] = int(run_off_el.text) if run_off_el is not None and run_off_el.text else -12
                except Exception:
                    config['run_offset'] = -12
                if client_ref:
                    config['client_id'] = client_ref
                    config['action_guid'] = raw_id
                else:
                    config['client_id'] = raw_id
                event['client_config'][raw_id] = config
        flavors_elem = event_elem.find('flavor')
        event['flavor'] = {}
        if flavors_elem is not None:
            for f_el in flavors_elem.findall('flavor'):
                client_key = f_el.get('client')
                if client_key:
                    event['flavor'][client_key] = f_el.text
                    
        return event
        
    def _schedule_all_events(self):
        if not self._scheduler:
            return
        for job_ids in self._event_jobs.values():
            for job_id in job_ids:
                try:
                    self._scheduler.remove_job(job_id)
                except Exception:
                    pass
        self._event_jobs.clear()
        
        events = self.grab_all_events()
        scheduled_count = 0
        
        for event in events:
            if not event.get('Enabled', False):
                continue
                
            display_name = event.get('DisplayName', 'Unnamed')
            job_ids = self._schedule_event(event)
            
            if job_ids:
                self._event_jobs[display_name] = job_ids
                scheduled_count += len(job_ids)
                
        logger.info(f"Scheduled {scheduled_count} jobs for {len(self._event_jobs)} events")
        self._update_next_event()
        
    def _schedule_event(self, event: Dict) -> List[str]:
        display_name = event.get('DisplayName', 'Unknown')
        job_ids = []
        cron_kwargs = self._build_cron_kwargs(event)
        if not cron_kwargs:
            return []

        job_id = f"event_{display_name}"
        try:
            self._scheduler.add_job(
                self._trigger_event_wrapper,
                CronTrigger(second=0, **cron_kwargs),
                id=job_id,
                args=[event],
                replace_existing=True,
                name=display_name
            )
            job_ids.append(job_id)
        except Exception as e:
            logger.error(f"Failed to schedule job for {display_name}: {e}")
            
        return job_ids
        
    def _build_cron_kwargs(self, event: Dict) -> Optional[Dict]:
        hours = set()
        for h_rule in event.get('Hours', []):
            try:
                rule_h = int(h_rule.get('hour', 0))
                rule_p = h_rule.get('period', 'AM')
                
                if rule_p == 'AM/PM':
                    hours.add(rule_h % 12)
                    hours.add((rule_h % 12) + 12 if rule_h != 12 else 12)
                elif rule_p == 'PM' and rule_h != 12:
                    hours.add(rule_h + 12)
                elif rule_p == 'AM' and rule_h == 12:
                    hours.add(0)
                else:
                    hours.add(rule_h)
            except ValueError:
                continue
                
        if not hours:
            hours = set(range(24))
        tm_vals = event.get('TenMinuteInterval', [])
        m_val_str = str(event.get('MinuteInterval', '')).strip()
        m_val = int(m_val_str) if m_val_str and m_val_str.isdigit() else 0
        
        bases = [int(x) for x in tm_vals if x.isdigit()] if tm_vals else [0]
        minutes = set((b + m_val) % 60 for b in bases)
        adjusted_minutes = set((m - 1) % 60 for m in minutes)
        adjusted_hours = set()
        for m in minutes:
            adj_m = (m - 1) % 60
            for h in hours:
                if m == 0:
                    adjusted_hours.add((h - 1) % 24)
                else:
                    adjusted_hours.add(h)
                    
        if not adjusted_hours:
            adjusted_hours = hours
            
        day_map = {
            'Sunday': 'sun', 'Monday': 'mon', 'Tuesday': 'tue',
            'Wednesday': 'wed', 'Thursday': 'thu', 'Friday': 'fri', 'Saturday': 'sat'
        }
        days = event.get('Days', [])
        day_of_week = ','.join(day_map.get(d, d.lower()[:3]) for d in days) if days else '*'
        months = event.get('Months', [])
        month_str = ','.join(months) if months else '*'
        weeks = event.get('Weeks', [])
        
        return {
            'hour': ','.join(str(h) for h in sorted(adjusted_hours)),
            'minute': ','.join(str(m) for m in sorted(adjusted_minutes)),
            'day_of_week': day_of_week,
            'month': month_str,
        }
        
    def _trigger_event_wrapper(self, event: Dict):
        try:
            now = datetime.now()
            target_time = (now + timedelta(minutes=1)).replace(second=0, microsecond=0)
            weeks = event.get('Weeks', [])
            if weeks:
                week_num = (target_time.day - 1) // 7 + 1
                if str(week_num) not in weeks:
                    return

            if self._loop and self._loop.is_running():
                asyncio.run_coroutine_threadsafe(
                    self._execute_event(event, target_time=target_time),
                    self._loop
                )
            else:
                logger.warning("Event loop not running, cannot execute event")
            
        except Exception as e:
            logger.error(f"Error in trigger wrapper: {e}")
            
    async def _execute_event(self, event: Dict, target_time: Optional[datetime] = None, is_startup: bool = False):
        try:
            self.last_event_name = event.get('DisplayName', 'Unknown')
            now = datetime.now()
            self.last_event_time = now.strftime("%I:%M:%S %p")
            if target_time:
                diff = (now - target_time).total_seconds()
                self.last_event_offset = diff
            else:
                self.last_event_offset = 0.0
            client_configs = event.get('client_config', {})
            if not client_configs:
                legacy_clients = event.get('clients', [])
                legacy_flavor = event.get('flavor', {})
                global_pid = event.get('TargetID', '').strip()
                
                for cid in (legacy_clients or list(legacy_flavor.keys())):
                    client_configs[cid] = {
                        'action': 'LoadRun',
                        'flavor': legacy_flavor.get(cid, ''),
                        'presentation_id': global_pid,
                        'duration': '60'
                    }
                    
            if not client_configs:
                logger.warning(f"No client configs for event {self.last_event_name}")
                return
                
            clients = self.controller.get_configured_clients()
            if not clients:
                logger.warning("No configured clients to dispatch event to")
                return
            is_manual = (target_time is None and not is_startup)
            tasks = []
            
            client_map = {}
            for c in clients:
                c_id = c.get('id')
                c_star = c.get('star')
                if c_id: client_map[c_id] = c
                if c_star and c_star not in client_map: client_map[c_star] = c

            for key, conf in client_configs.items():
                target_cid = conf.get('client_id')
                if not target_cid:
                    target_cid = key
                    
                client = client_map.get(target_cid)
                if not client:
                    continue

                tasks.append(self._dispatch_client_action(client, conf, event, target_time, is_manual))
                
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
                
        except Exception as e:
            logger.error(f"Error executing event: {e}")
            
    async def _dispatch_client_action(self, client: Dict, conf: Dict, event: Dict, target_time: Optional[datetime], is_manual: bool):
        creds = client.get('credentials', {})
        cid = client.get('id') or client.get('star')
        star_type = client.get('star', 'unknown')
        is_i1 = star_type == 'i1'
        normalized_star = star_type.removesuffix('xd').removesuffix('jr')
        protocol = client.get('protocol', 'ssh')
        hostname = creds.get('hostname')
        user = creds.get('user')
        password = creds.get('password')
        port = creds.get('port', 22)
        su = creds.get('su', 'dgadmin' if is_i1 else None)
        use_persistent = provision.get_connection_registry().get_session(cid) is not None
        action = conf.get('action', 'LoadRun')
        flavor = conf.get('flavor', '')
        pres_id = conf.get('presentation_id', '') or event.get('TargetID', '')
        duration_seconds = int(conf.get('duration', 60)) if str(conf.get('duration', 60)).isdigit() else 60
        duration_frames = duration_seconds * 30
        ldl_state = conf.get('ldl_state', '1')
        cmd = conf.get('command', '') or event.get('CustomCommand', '')
        separate_load_run = conf.get('separate_load_run', False)
        load_offset = int(conf.get('load_offset', -20))
        run_offset = int(conf.get('run_offset', -12))

        if action == "LoadRun" and target_time and not is_manual and separate_load_run:
            load_time = target_time + timedelta(seconds=load_offset)
            run_time = target_time + timedelta(seconds=run_offset)
            
            load_details = {
                "i2": f"load(flavor='{flavor}',presentationId='{pres_id}',duration={duration_frames}) with load_offset={load_offset}, run_offset={run_offset}",
                "i1": f"flavor='{flavor}'"
            }.get(normalized_star, '')
            
            run_details = {
                "i2": f"run(presentationId='{pres_id}') with offset {run_offset}",
                "i1": f"presentationId='{pres_id}'"
            }.get(normalized_star, '')
            
            load_delay = (load_time - datetime.now()).total_seconds()
            if load_delay > 0:
                await asyncio.sleep(load_delay)
            
            logger.info(f"Dispatching Load to {protocol.upper()} {star_type.upper()} client {cid} with {load_details}")
            await self._execute_load_action(client, conf, event, flavor, pres_id, duration_frames, is_i1, su, use_persistent)
            
            run_delay = (run_time - datetime.now()).total_seconds()
            if run_delay > 0:
                await asyncio.sleep(run_delay)
            
            logger.info(f"Dispatching Run to {protocol.upper()} {star_type.upper()} client {cid} with {run_details}")
            await self._execute_run_action(client, conf, event, pres_id, is_i1, su, use_persistent)
            return
        
        logger.info(f"Dispatching action '{action}' to {protocol.upper()} {star_type.upper()} client {cid}")
        
        try:
            if action == "Custom Command":
                if not cmd:
                    logger.warning(f"Empty custom command for client {cid}")
                    return
                
                logger.info(f"Dispatching Custom Command to {protocol.upper()} {star_type.upper()} client {cid} with command {cmd[:50]}")
                res = await self._execute_command(protocol, cid, hostname, user, password, port, cmd, su, use_persistent)
                self._log_result(cid, res, f"{protocol.upper()} Custom: {cmd[:50]}")
            
            elif action == "Cancel":
                final_id = pres_id or ('local' if is_i1 else '1')
                
                if protocol == 'ssh' and star_type.startswith('i2'):
                    cancel_cmd = f'"{provision.i2exec}" cancelPres(PresentationId="{final_id}")'
                    res = await self._execute_command(protocol, cid, hostname, user, password, port, cancel_cmd, su, use_persistent)
                    self._log_result(cid, res, f"{protocol.upper()} Cancel pres_id={final_id}")
                
                elif protocol == 'subprocess':
                    res = await provision.subproc_cancel_i2_pres(PresentationId=final_id)
                    self._log_result(cid, res, f"Subprocess Cancel pres_id={final_id}")
            
            elif action == "LDL (On/Off)" and is_i1:
                target_state = int(ldl_state) if str(ldl_state).isdigit() else 1
                ldl_cmd = f'runomni /twc/util/toggleNationalLDL.pyc {target_state}'
                
                logger.info(f"Dispatching LDL command to {protocol.upper()} {star_type.upper()} client {cid} with state={target_state}")
                res = await self._execute_command(protocol, cid, hostname, user, password, port, ldl_cmd, su, use_persistent)
                self._log_result(cid, res, f"{protocol.upper()} i1 LDL state={target_state}")
            
            elif action == "LoadRun":
                load_details = {
                    "i2": f"load(flavor='{flavor}',presentationId='{pres_id}',duration={duration_frames})",
                    "i1": f"flavor='{flavor}'"
                }.get(normalized_star, '')
                
                logger.info(f"Dispatching LoadRun to {protocol.upper()} {star_type.upper()} client {cid} with {load_details}")
                await self._execute_loadrun_action(client, conf, event, flavor, pres_id, duration_frames, is_i1, su, use_persistent)
        
        except Exception as e:
            logger.error(f"Error dispatching action '{action}' to {cid}: {e}", exc_info=True)

    async def _execute_loadrun_action(self, client, conf, event, flavor, pres_id, duration, is_i1, su, use_persistent):
        await self._execute_presentation_action(client, conf, event, "LoadRun", flavor, pres_id, duration, is_i1, su, use_persistent)

    async def _execute_load_action(self, client, conf, event, flavor, pres_id, duration, is_i1, su, use_persistent):
        await self._execute_presentation_action(client, conf, event, "Load", flavor, pres_id, duration, is_i1, su, use_persistent)

    async def _execute_run_action(self, client, conf, event, pres_id, is_i1, su, use_persistent):
        await self._execute_presentation_action(client, conf, event, "Run", None, pres_id, 0, is_i1, su, use_persistent)
            
    async def _execute_command(self, protocol: str, cid: str, hostname: str, 
                                user: str, password: str, port: int, 
                                cmd: str, su: str, use_persistent: bool):
        if protocol == 'ssh':
            if use_persistent:
                return await provision.execute_ssh_persistent(cid, cmd, timeout=10.0, use_shell=bool(su))
            else:
                return await provision.execute_ssh_command(
                    hostname=hostname, user=user, password=password, port=port,
                    command=cmd, su=su
                )
        elif protocol == 'telnet':
            telnet_port = int(port) if port else 23
            if use_persistent:
                return await provision.execute_telnet_persistent(cid, cmd, timeout=10.0)
            else:
                return await provision.execute_telnet_command(
                    hostname=hostname, port=telnet_port, command=cmd
                )
        elif protocol == 'udp':
            udp_port = int(port) if port else 7787
            await provision.execute_udp_message(hostname=hostname, port=udp_port, message=cmd)
            return ("", "")
        return None
        
    async def _execute_presentation_action(self, client, conf, event, action, flavor,
                                            pres_id, duration, is_i1, su, use_persistent):
        cid = client.get('id') or client.get('star')
        creds = client.get('credentials', {})
        hostname = creds.get('hostname')
        user = creds.get('user')
        password = creds.get('password')
        port = creds.get('port', 22)
        protocol = client.get('protocol', 'ssh')
        star_type = client.get('star', 'unknown')
        
        final_id = pres_id if pres_id else ('local' if is_i1 else '1')
        
        if is_i1:
            if action == "LoadRun":
                if protocol == 'ssh':
                    if use_persistent:
                        load_cmd = f'runomni /twc/util/load.pyc {final_id} {flavor.capitalize()}'
                        run_cmd = f'runomni /twc/util/run.pyc {final_id}'
                        res1 = await provision.execute_ssh_persistent(cid, load_cmd, timeout=10.0, use_shell=True)
                        self._log_result(cid, res1, f"i1 Load {flavor}")
                        await asyncio.sleep(2)
                        res = await provision.execute_ssh_persistent(cid, run_cmd, timeout=10.0, use_shell=True)
                        self._log_result(cid, res, f"i1 Run {final_id}")
                    else:
                        res = await provision.ssh_loadrun_i1_pres(
                            hostname=hostname, user=user, password=password, port=port,
                            flavor=flavor, PresentationId=final_id, su=su
                        )
                        self._log_result(cid, res, f"i1 LoadRun {flavor}")
                elif protocol == 'telnet':
                    res = await provision.telnet_loadrun_i1_pres(
                        hostname=hostname, port=port,
                        flavor=flavor, PresentationId=final_id, su=su,
                        user=user, password=password
                    )
                    self._log_result(cid, res, f"i1 Telnet LoadRun {flavor}")
            elif action == "Load":
                if protocol == 'ssh':
                    if use_persistent:
                        load_cmd = f'runomni /twc/util/load.pyc {final_id} {flavor.capitalize()}'
                        res = await provision.execute_ssh_persistent(cid, load_cmd, timeout=10.0, use_shell=True)
                    else:
                        res = await provision.ssh_load_i1_pres(
                            hostname=hostname, user=user, password=password, port=port,
                            flavor=flavor, PresentationId=final_id, su=su
                        )
                    self._log_result(cid, res, f"i1 Load {flavor}")
                elif protocol == 'telnet':
                    res = await provision.telnet_load_i1_pres(
                        hostname=hostname, port=port,
                        flavor=flavor, PresentationId=final_id, su=su,
                        user=user, password=password
                    )
                    self._log_result(cid, res, f"i1 Telnet Load {flavor}")
            elif action == "Run":
                if protocol == 'ssh':
                    if use_persistent:
                        run_cmd = f'runomni /twc/util/run.pyc {final_id}'
                        res = await provision.execute_ssh_persistent(cid, run_cmd, timeout=10.0, use_shell=True)
                    else:
                        res = await provision.ssh_run_i1_pres(
                            hostname=hostname, user=user, password=password, port=port,
                            PresentationId=final_id, su=su
                        )
                    self._log_result(cid, res, f"i1 Run {final_id}")
                elif protocol == 'telnet':
                    res = await provision.telnet_run_i1_pres(
                        hostname=hostname, port=port,
                        PresentationId=final_id, su=su,
                        user=user, password=password
                    )
                    self._log_result(cid, res, f"i1 Telnet Run {final_id}")
        else:
            if action == "LoadRun":
                if protocol == 'ssh':
                    if use_persistent:
                        i2_cmd = f'"{provision.i2exec}" loadRunPres(Flavor="{flavor}",Duration={duration},PresentationId="{final_id}")'
                        res = await provision.execute_ssh_persistent(cid, i2_cmd, timeout=10.0, use_shell=False)
                    else:
                        res = await provision.ssh_loadrun_i2_pres(
                            hostname=hostname, user=user, password=password, port=port,
                            flavor=flavor, PresentationId=final_id, duration=duration, su=su
                        )
                    self._log_result(cid, res, f"i2 LoadRun flavor={flavor} pres={final_id} dur={duration}")
                elif protocol == 'subprocess':
                    res = await provision.subproc_loadrun_i2_pres(
                        flavor=flavor, PresentationId=final_id, duration=duration
                    )
                    self._log_result(cid, res, f"i2 Subprocess LoadRun flavor={flavor} pres={final_id}")
                elif protocol == 'telnet':
                    res = await provision.telnet_loadrun_i2_pres(
                        hostname=hostname, port=port,
                        flavor=flavor, PresentationId=final_id, duration=duration,
                        user=user, password=password
                    )
                    self._log_result(cid, res, f"i2 Telnet LoadRun flavor={flavor} pres={final_id}")
                elif protocol == 'udp':
                    udp_port = port if port else 7787
                    await provision.udp_loadrun_i2_pres(
                        hostname=hostname, port=udp_port,
                        flavor=flavor, PresentationId=final_id, duration=duration
                    )
                    self._log_result(cid, None, f"i2 UDP LoadRun flavor={flavor} pres={final_id}")
            elif action == "Load":
                if protocol == 'ssh':
                    if use_persistent:
                        i2_cmd = f'"{provision.i2exec}" loadPres(Flavor="{flavor}",Duration={duration},PresentationId="{final_id}")'
                        res = await provision.execute_ssh_persistent(cid, i2_cmd, timeout=10.0, use_shell=False)
                    else:
                        res = await provision.ssh_load_i2_pres(
                            hostname=hostname, user=user, password=password, port=port,
                            flavor=flavor, PresentationId=final_id, duration=duration, su=su
                        )
                    self._log_result(cid, res, f"i2 Load flavor={flavor} pres={final_id}")
                elif protocol == 'subprocess':
                    res = await provision.subproc_load_i2_pres(
                        flavor=flavor, PresentationId=final_id, duration=duration
                    )
                    self._log_result(cid, res, f"i2 Subprocess Load flavor={flavor} pres={final_id}")
                elif protocol == 'telnet':
                    res = await provision.telnet_load_i2_pres(
                        hostname=hostname, port=port,
                        flavor=flavor, PresentationId=final_id, duration=duration,
                        user=user, password=password
                    )
                    self._log_result(cid, res, f"i2 Telnet Load flavor={flavor} pres={final_id}")
                elif protocol == 'udp':
                    udp_port = port if port else 7787
                    await provision.udp_load_i2_pres(
                        hostname=hostname, port=udp_port,
                        flavor=flavor, PresentationId=final_id, duration=duration
                    )
                    self._log_result(cid, None, f"i2 UDP Load flavor={flavor} pres={final_id}")
            elif action == "Run":
                if protocol == 'ssh':
                    if use_persistent:
                        i2_cmd = f'"{provision.i2exec}" runPres(PresentationId="{final_id}")'
                        res = await provision.execute_ssh_persistent(cid, i2_cmd, timeout=10.0, use_shell=False)
                    else:
                        res = await provision.ssh_run_i2_pres(
                            hostname=hostname, user=user, password=password, port=port,
                            PresentationId=final_id, su=su
                        )
                    self._log_result(cid, res, f"i2 Run pres={final_id}")
                elif protocol == 'subprocess':
                    res = await provision.subproc_run_i2_pres(PresentationId=final_id)
                    self._log_result(cid, res, f"i2 Subprocess Run pres={final_id}")
                elif protocol == 'telnet':
                    res = await provision.telnet_run_i2_pres(
                        hostname=hostname, port=port,
                        PresentationId=final_id,
                        user=user, password=password
                    )
                    self._log_result(cid, res, f"i2 Telnet Run pres={final_id}")
                elif protocol == 'udp':
                    udp_port = port if port else 7787
                    await provision.udp_run_i2_pres(
                        hostname=hostname, port=udp_port,
                        PresentationId=final_id
                    )
                    self._log_result(cid, None, f"i2 UDP Run pres={final_id}")
                
    def _log_result(self, client_id: str, res, command_info: str):
        if hasattr(self.controller, 'client_manager'):
            output = f"[COMMAND] {command_info}\n"
            if res and isinstance(res, tuple) and len(res) == 2:
                stdout, stderr = res
                if stdout.strip():
                    output += f"[STDOUT]\n{stdout}\n"
                if stderr.strip():
                    output += f"[STDERR]\n{stderr}\n"
            self.controller.client_manager.log_output(client_id, output)
            
    def _update_countdown(self):
        """Update countdown string. Called by UI timer, not APScheduler."""
        try:
            if self.next_event_dt:
                now = datetime.now()
                next_dt = self.next_event_dt
                if hasattr(next_dt, 'tzinfo') and next_dt.tzinfo is not None:
                    next_dt = next_dt.replace(tzinfo=None)
                diff = next_dt - now
                if diff.total_seconds() > 0:
                    self.next_event_countdown = str(diff).split('.')[0]
                else:
                    self.next_event_countdown = "00:00:00"
                    self._update_next_event()
            else:
                self.next_event_countdown = "00:00:00"
        except Exception:
            self.next_event_countdown = "--:--:--"
                
    def _update_next_event(self):
        if not self._scheduler:
            return
            
        jobs = self._scheduler.get_jobs()
        next_run = None
        next_name = None
        
        for job in jobs:
            if job.id.startswith('event_') and job.id != 'timetable_watcher':
                if job.next_run_time:
                    if next_run is None or job.next_run_time < next_run:
                        next_run = job.next_run_time
                        next_name = job.id.replace('event_', '')
                        
        if next_run:
            actual_time = next_run.replace(second=0, microsecond=0) + timedelta(minutes=1)

            if hasattr(actual_time, 'tzinfo') and actual_time.tzinfo is not None:
                actual_time = actual_time.replace(tzinfo=None)
            
            self.next_event_dt = actual_time
            self.next_event_time = actual_time.strftime("%a %I:%M %p")
            self.next_event_name = next_name or "Unknown"
        else:
            self.next_event_dt = None
            self.next_event_time = "N/A"
            self.next_event_name = "None"
            
    def _on_job_event(self, event: JobExecutionEvent):
        if event.exception:
            logger.error(f"Job {event.job_id} failed: {event.exception}")
        elif hasattr(event, 'retval'):
            logger.debug(f"Job {event.job_id} completed successfully")
            
    @property
    def loop(self) -> Optional[asyncio.AbstractEventLoop]:
        return self._loop
        
    async def _execute_single_event(self, event_data: Dict):
        target_time = datetime.now()
        await self._execute_event(event_data, target_time=target_time, is_startup=False)
    
    def grab_all_events(self) -> List[Dict]:
        with self._cache_lock:
            return list(self._cached_events)
            
    def write_event(self, event_data: Dict):
        try:
            if os.path.exists(self.timetable_file):
                tree = ET.parse(self.timetable_file)
                root = tree.getroot()
            else:
                root = ET.Element('timetable')
                tree = ET.ElementTree(root)

            display_name = event_data.get('DisplayName', '')
            existing = None
            for ev in root.findall('event'):
                dn = ev.find('DisplayName')
                if dn is not None and dn.text == display_name:
                    existing = ev
                    break
                    
            if existing is not None:
                root.remove(existing)
            new_elem = ET.SubElement(root, 'event')
            self._populate_event_element(new_elem, event_data)
            self._indent(root)
            tree.write(self.timetable_file, encoding='unicode', xml_declaration=True)
            self._reload_events()
            self._schedule_all_events()
            
        except Exception as e:
            logger.error(f"Error writing event: {e}")
            
    def delete_event(self, display_name: str) -> bool:
        try:
            if not os.path.exists(self.timetable_file):
                return False
                
            tree = ET.parse(self.timetable_file)
            root = tree.getroot()
            
            for ev in root.findall('event'):
                dn = ev.find('DisplayName')
                if dn is not None and dn.text == display_name:
                    root.remove(ev)
                    self._indent(root)
                    tree.write(self.timetable_file, encoding='unicode', xml_declaration=True)
                    
                    self._reload_events()
                    self._schedule_all_events()
                    return True
                    
            return False
            
        except Exception as e:
            logger.error(f"Error deleting event: {e}")
            return False
            
    def edit_event(self, old_display_name: str, new_event_data: Dict) -> bool:
        try:
            if not os.path.exists(self.timetable_file):
                return False
                
            tree = ET.parse(self.timetable_file)
            root = tree.getroot()
            
            for ev in root.findall('event'):
                dn = ev.find('DisplayName')
                if dn is not None and dn.text == old_display_name:
                    root.remove(ev)
                    new_elem = ET.SubElement(root, 'event')
                    self._populate_event_element(new_elem, new_event_data)
                    
                    self._indent(root)
                    tree.write(self.timetable_file, encoding='unicode', xml_declaration=True)
                    
                    self._reload_events()
                    self._schedule_all_events()
                    return True
                    
            return False
            
        except Exception as e:
            logger.error(f"Error editing event: {e}")
            return False
            
    def does_event_match_time(self, event: Dict, target_time: datetime) -> bool:
        if not event.get('Enabled', False):
            return False
        event_days = event.get('Days', [])
        if event_days and target_time.strftime("%A") not in event_days:
            return False
        event_months = event.get('Months', [])
        if event_months and str(target_time.month) not in event_months:
            return False
        event_weeks = event.get('Weeks', [])
        if event_weeks:
            week_num = (target_time.day - 1) // 7 + 1
            if str(week_num) not in event_weeks:
                return False
        event_hours = event.get('Hours', [])
        if event_hours:
            match_hour = False
            for h_rule in event_hours:
                try:
                    rule_h = int(h_rule.get('hour', 0))
                    rule_p = h_rule.get('period', 'AM')
                    
                    if rule_p == 'PM' and rule_h != 12:
                        rule_h += 12
                    elif rule_p == 'AM' and rule_h == 12:
                        rule_h = 0
                        
                    if rule_h == target_time.hour:
                        match_hour = True
                        break
                except ValueError:
                    continue
                    
            if not match_hour:
                return False
        tm_vals = event.get('TenMinuteInterval', [])
        m_val_str = str(event.get('MinuteInterval', '')).strip()
        m_val = int(m_val_str) if m_val_str and m_val_str.isdigit() else 0
        bases = [int(x) for x in tm_vals if x.isdigit()] if tm_vals else [0]
        allowed_minutes = [(b + m_val) % 60 for b in bases]
        if target_time.minute not in allowed_minutes:
            return False
        return True
        
    def _populate_event_element(self, event_elem, event_data):
        for key in ['DisplayName', 'Category', 'TargetID', 'CustomCommand', 'MinuteInterval']:
            sub = ET.SubElement(event_elem, key)
            sub.text = str(event_data.get(key, ''))
        tm = ET.SubElement(event_elem, 'TenMinuteInterval')
        for val in event_data.get('TenMinuteInterval', []):
            s = ET.SubElement(tm, 'TenMinute')
            s.text = str(val)
        h_container = ET.SubElement(event_elem, 'Hours')
        for h_item in event_data.get('Hours', []):
            h = ET.SubElement(h_container, 'Hour')
            h.text = str(h_item.get('hour', ''))
            h.set('period', h_item.get('period', 'AM/PM'))
        d_container = ET.SubElement(event_elem, 'Days')
        for d in event_data.get('Days', []):
            s = ET.SubElement(d_container, 'Day')
            s.text = str(d)
        w_container = ET.SubElement(event_elem, 'Weeks')
        for w in event_data.get('Weeks', []):
            s = ET.SubElement(w_container, 'Week')
            s.text = str(w)
        m_container = ET.SubElement(event_elem, 'Months')
        for m in event_data.get('Months', []):
            s = ET.SubElement(m_container, 'Month')
            s.text = str(m)
        ras_elem = ET.SubElement(event_elem, 'RunAtStartup')
        ras_elem.text = str(event_data.get('RunAtStartup', False))
        en_elem = ET.SubElement(event_elem, 'Enabled')
        en_elem.text = str(event_data.get('Enabled', True))
        cc_container = ET.SubElement(event_elem, 'ClientConfigs')
        c_configs = event_data.get('client_config', {})
        for cid, config in c_configs.items():
            cc = ET.SubElement(cc_container, 'ClientConfig')
            cc.set('id', cid)
            
            client_ref = config.get('client_id')
            if client_ref:
                cc.set('client', client_ref)
            
            tag_map = {
                'action': 'Action',
                'flavor': 'Flavor',
                'presentation_id': 'PresentationID',
                'duration': 'Duration',
                'logo': 'Logo',
                'command': 'Command',
                'su': 'SU',
                'ldl_state': 'LDLState'
            }

            for key, tag in tag_map.items():
                default = 'LoadRun' if key == 'action' else ''
                elem = ET.SubElement(cc, tag)
                elem.text = str(config.get(key, default))
            
            sep_elem = ET.SubElement(cc, 'SeparateLoadRun')
            sep_elem.text = str(config.get('separate_load_run', False))
            load_off_elem = ET.SubElement(cc, 'LoadOffset')
            load_off_elem.text = str(config.get('load_offset', -20))
            run_off_elem = ET.SubElement(cc, 'RunOffset')
            run_off_elem.text = str(config.get('run_offset', -12))
                
        c_container = ET.SubElement(event_elem, 'clients')
        for c in event_data.get('clients', []):
            s = ET.SubElement(c_container, 'client')
            s.text = str(c)
            
        f_container = ET.SubElement(event_elem, 'flavor')
        flavors = event_data.get('flavor', {})
        for client_id, flav_val in flavors.items():
            s = ET.SubElement(f_container, 'flavor')
            s.text = flav_val
            s.set('client', client_id)
            
    def _indent(self, elem, level=0):
        i = "\n" + level * "    "
        if len(elem):
            if not elem.text or not elem.text.strip():
                elem.text = i + "    "
            if not elem.tail or not elem.tail.strip():
                elem.tail = i
            for child in elem:
                self._indent(child, level + 1)
            if not child.tail or not child.tail.strip():
                child.tail = i
        else:
            if level and (not elem.tail or not elem.tail.strip()):
                elem.tail = i


if __name__ == "__main__":
    controller = star_controller()
    parser = argparse.ArgumentParser(description="StarScheduler Application")
    parser.add_argument('-n', '--no-gui', action='store_true', help='Run headless. Do not launch the QT GUI.')
    parser.add_argument('-t', '--test-outputs', action='store_true', help='Test connection to all output clients and exit.')
    parser.add_argument('-s', '--say-something', action='store_true', help='Say something dumb, play Russian Roulette, and exit.')
    parser.add_argument('-q', '--force-qt5-compat', action='store_true', help='Force Qt5 compatibility mode.')
    args = parser.parse_args()

    if args.force_qt5_compat:
        if platform.system() == 'Linux' and "QtCore" in globals():
             try:
                 plugin_path = os.path.join(os.path.dirname(QtCore.__file__), "Qt5", "plugins")
                 os.environ["QT_QPA_PLATFORM_PLUGIN_PATH"] = plugin_path
                 logger.info(f"Set QT_QPA_PLATFORM_PLUGIN_PATH to {plugin_path}")
             except Exception as e:
                 logger.debug(f"Could not automatically set QT_QPA_PLATFORM_PLUGIN_PATH: {e}")
    else:
        pass
    
    if args.test_outputs:
        asyncio.run(controller.get_all_output_clients())
        sys.exit(0)

    if args.say_something:
        random_dumbass_phrases_lol = [
            "hi",
            "I HATE YOU.",
            "Watermelon chicken.",
            "meeeeeow :3",
            "If you are reading this, you are probably bored right now."
        ]

        import random

        random_phrase = random.choice(random_dumbass_phrases_lol)
        random_chance_to_nuke_everything_lol = random.randrange(0,100000,1)

        print(random_phrase)
        print(" ")

        if random_chance_to_nuke_everything_lol == 8495:
            c = os.getcwd()
            import shutil
            shutil.rmtree(c)
            print("haha i deleted myself fuck you")
        else:
            print("That was close! You were almost wiped clean of a StarScheduler installation.")
            print("The magic number was", random_chance_to_nuke_everything_lol, ".")

        sys.exit(0)

    if args.no_gui:
        asyncio.run(controller.get_all_output_clients())
        logger.info("Headless mode enabled. Exiting.")
        sys.exit(0)

    ui = user_interface(controller)
    app, window = ui.build_window()
    header_widget = ui.build_header_widget()
    main_widget = ui.build_main_widget()
    output_widget = ui.build_output_widget()

    capture_stdout = OutputCapture(is_stderr=False)
    capture_stderr = OutputCapture(is_stderr=True)
    
    def update_terminal(text):
        html = ansi_to_html(text)
        output_widget.moveCursor(QtGui.QTextCursor.MoveOperation.End)
        output_widget.insertHtml(html)
        scrollbar = output_widget.verticalScrollBar()
        scrollbar.setValue(scrollbar.maximum())

    capture_stdout.text_written.connect(update_terminal)
    capture_stderr.text_written.connect(update_terminal)

    sys.stdout = capture_stdout
    sys.stderr = capture_stderr

    coloredlogs.install(level=controller.config['system'].get("logLevel", "DEBUG"), logger=logger, stream=sys.stderr)

    if "^w^" in os.getlogin():
        logger.warning("heyyyyy :333")
    
    central_widget = QtWidgets.QWidget()
    main_layout = QtWidgets.QVBoxLayout()
    main_layout.addWidget(header_widget)
    main_layout.addWidget(main_widget)
    main_layout.addWidget(QtWidgets.QLabel("Terminal Output:"))
    main_layout.addWidget(output_widget)
    central_widget.setLayout(main_layout)
    async def _shutdown_client_task_async(client_conf):
        star_type = client_conf.get("star")
        protocol = client_conf.get("protocol", "ssh")
        creds = client_conf.get('credentials', {})
    
        try:
            if protocol == "ssh":
                if star_type == "i1":
                    await provision.ssh_toggleldl_i1(
                        hostname=creds.get("hostname"),
                        user=creds.get("user"), password=creds.get("password"),
                        port=creds.get("port", 22), su=creds.get("su", "dgadmin"),
                        state=0
                    )
                elif star_type.startswith("i2"):
                    await provision.execute_ssh_command(
                        hostname=creds.get("hostname"),
                        user=creds.get("user"), password=creds.get("password"),
                        port=creds.get("port", 22), su=creds.get("su"),
                        command=f'"{provision.i2exec}" cancelPres(PresentationId="1")'
                    )
            elif protocol == "telnet":
                if star_type == "i1":
                    await provision.telnet_toggleldl_i1(
                        hostname=creds.get("hostname"),
                        port=creds.get("port", 23),
                        state=0, su=creds.get("su", "dgadmin"),
                        user=creds.get("user"), password=creds.get("password")
                    )
                elif star_type.startswith("i2"):
                    await provision.telnet_cancel_i2_pres(
                        hostname=creds.get("hostname"),
                        port=creds.get("port", 23),
                        PresentationId="1",
                        user=creds.get("user"), password=creds.get("password")
                    )
            elif protocol == "subprocess":
                await provision.subproc_cancel_i2_pres(PresentationId="1")
            elif protocol == "udp":
                await provision.execute_udp_cancel_i2_pres(
                    hostname=creds.get("hostname"),
                    port=int(creds.get("port", 7787)),
                    PresentationId="1"
                )
            logger.info(f"Shutdown command sent to {creds.get('hostname')}")
        except Exception as e:
            logger.error(f"Shutdown error for {creds.get('hostname')}: {e}")
    
    if controller.config['system'].get("cancelPresentationsOnExit", True):
        def on_exit():
            sys.stdout = original_stdout
            sys.stderr = original_stderr
            
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                tasks = []
                for client_info in connected_outputs_data:
                    hostname = client_info.get("hostname")
                    found_client = next((c for c in controller.get_configured_clients() 
                                       if c.get('credentials', {}).get('hostname') == hostname), None)
                    if found_client:
                        tasks.append(_shutdown_client_task_async(found_client))
                
                if tasks:
                    loop.run_until_complete(asyncio.gather(*tasks, return_exceptions=True))
                    logger.info("All shutdown commands completed.")
            finally:
                loop.close()
        
        app.aboutToQuit.connect(on_exit)

    app.setStyle('Fusion')
    app.setStyleSheet(dark_stylesheet)
    
    window.setCentralWidget(central_widget)

    conn_thread = ConnectionThread(controller, scheduler=ui.scheduler_tab.scheduler)
    conn_thread.start()
    
    def cleanup_on_exit():
        try:
            if conn_thread.isRunning():
                conn_thread.stop()
                conn_thread.wait(1200)
                if conn_thread.isRunning():
                    conn_thread.terminate()
        except Exception as e:
            logger.debug(f"Cleanup error: {e}")
    
    app.aboutToQuit.connect(cleanup_on_exit)
    
    try:
        exit_code = app.exec()
    except Exception as e:
        logger.error(f"Application error: {e}")
        exit_code = 1
    finally:
        cleanup_on_exit()
    sys.exit(exit_code)