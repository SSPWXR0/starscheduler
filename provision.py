
import asyncio
import os
import re
import sys
import paramiko
import logging
import time
import socket
import telnetlib3
import subprocess
import uuid
import threading
import atexit
from typing import Optional, Dict, Any, Tuple
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field

logger = logging.getLogger("starscheduler.provision")

i2exec = "C:\\Program Files (x86)\\TWC\\I2\\exec.exe"

def _get_optimal_thread_count(max_threads: int = 4) -> int:
    """Calculate optimal thread count based on CPU cores and config limit.
    Uses minimal allocation for I/O-bound work.
    """
    try:
        cpu_count = os.cpu_count() or 2
        optimal = min(cpu_count, max_threads, 4)
        return max(1, optimal)
    except Exception:
        return 2

_executor: Optional[ThreadPoolExecutor] = None
_executor_max_workers: int = 4

def _get_executor() -> ThreadPoolExecutor:
    """Get or create the shared thread pool executor (lazy initialization)."""
    global _executor
    if _executor is None:
        worker_count = _get_optimal_thread_count(_executor_max_workers)
        _executor = ThreadPoolExecutor(
            max_workers=worker_count,
            thread_name_prefix="provision_worker"
        )
        logger.info(f"Initialized provision executor with {worker_count} workers")
    return _executor

def configure_executor(max_threads: int = 16) -> None:
    """Configure the executor max threads before first use."""
    global _executor_max_workers, _executor
    _executor_max_workers = max_threads
    if _executor is not None:
        _executor.shutdown(wait=False)
        _executor = None
        _get_executor()

def generate_session_uuid() -> str:
    """Generate a 16-character hex UUID for session identification."""
    return uuid.uuid4().hex[:16]

@dataclass
class SessionInfo:
    """Holds metadata about a persistent session."""
    session_uuid: str
    client_id: str
    protocol: str
    credentials: Dict[str, Any]
    connected: bool = False
    last_activity: float = field(default_factory=time.time)
    error_count: int = 0
    connection: Any = None
    shell: Any = None
    lock: threading.Lock = field(default_factory=threading.Lock)


class PersistentSSHSession:
    """Wrapper for persistent SSH connection with interactive shell."""
    
    def __init__(self, session_info: SessionInfo):
        self.info = session_info
        self.client: Optional[paramiko.SSHClient] = None
        self.shell = None
        self._lock = threading.Lock()
        self._connected = False
        
    def connect(self) -> bool:
        """Establish SSH connection and open interactive shell."""
        with self._lock:
            if self._connected and self.client and self.client.get_transport():
                if self.client.get_transport().is_active():
                    return True
            
            try:
                creds = self.info.credentials
                self.client = paramiko.SSHClient()
                self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                self.client.connect(
                    hostname=creds.get('hostname'),
                    port=creds.get('port', 22),
                    username=creds.get('user'),
                    password=creds.get('password'),
                    timeout=15,
                    look_for_keys=False,
                    allow_agent=False,
                    banner_timeout=15
                )
                self.shell = self.client.invoke_shell()
                time.sleep(0.5)
                while self.shell.recv_ready():
                    self.shell.recv(4096)
                su_user = creds.get('su')
                if su_user:
                    self.shell.send(f"su -l {su_user}\n".encode())
                    time.sleep(0.8)
                    su_output = ""
                    start = time.time()
                    while time.time() - start < 3:
                        if self.shell.recv_ready():
                            data = self.shell.recv(4096).decode('utf-8', errors='replace')
                            su_output += data
                            if '$' in data or '#' in data or '>' in data:
                                break
                        time.sleep(0.1)
                    logger.debug(f"SSH persistent session su output: {su_output[:200]}")
                
                self._connected = True
                self.info.connected = True
                self.info.last_activity = time.time()
                self.info.error_count = 0
                logger.info(f"SSH persistent session established: {self.info.session_uuid} -> {creds.get('hostname')}")
                return True
                
            except Exception as e:
                logger.error(f"SSH persistent connect failed for {self.info.client_id}: {e}")
                self.info.error_count += 1
                self._connected = False
                self.info.connected = False
                return False
    
    def is_alive(self) -> bool:
        """Check if connection is still alive."""
        if not self.client:
            return False
        transport = self.client.get_transport()
        return transport is not None and transport.is_active()
    
    def execute(self, command: str, timeout: float = 10.0, use_shell: bool = True) -> Tuple[str, str]:
        """Execute command on persistent session."""
        with self._lock:
            if not self.is_alive():
                if not self.connect():
                    return "", "Session disconnected and reconnect failed"
            
            try:
                self.info.last_activity = time.time()
                
                if use_shell and self.shell:
                    cmd_bytes = (command + "\n").encode("utf-8", errors="replace")
                    self.shell.send(cmd_bytes)
                    
                    output = ""
                    start = time.time()
                    time.sleep(0.2)
                    while (time.time() - start) < timeout:
                        if self.shell.recv_ready():
                            data = self.shell.recv(4096).decode('utf-8', errors='replace')
                            output += data
                            start = time.time()
                        else:
                            time.sleep(0.1)
                            if output and (time.time() - start) > 1.5:
                                break
                    
                    return output, ""
                else:
                    stdin, stdout, stderr = self.client.exec_command(command, timeout=timeout)
                    stdout.channel.recv_exit_status()
                    stdout_str = stdout.read().decode('utf-8', errors='replace')
                    stderr_str = stderr.read().decode('utf-8', errors='replace')
                    return stdout_str, stderr_str
                    
            except Exception as e:
                logger.error(f"SSH persistent execute error: {e}")
                self.info.error_count += 1
                self._connected = False
                self.info.connected = False
                return "", str(e)
    
    def close(self):
        """Close the persistent connection."""
        with self._lock:
            try:
                if self.shell:
                    self.shell.close()
                if self.client:
                    self.client.close()
            except Exception as e:
                logger.debug(f"Error closing SSH session: {e}")
            finally:
                self._connected = False
                self.info.connected = False


class PersistentTelnetSession:
    """Wrapper for persistent Telnet connection."""
    
    def __init__(self, session_info: SessionInfo):
        self.info = session_info
        self.reader = None
        self.writer = None
        self._lock = asyncio.Lock()
        self._connected = False
        self._loop = None
        
    async def connect(self) -> bool:
        """Establish Telnet connection."""
        async with self._lock:
            if self._connected and self.writer:
                return True
            try:
                creds = self.info.credentials
                hostname = creds.get('hostname')
                port = creds.get('port', 23)
                
                self.reader, self.writer = await asyncio.wait_for(
                    telnetlib3.open_connection(hostname, port),
                    timeout=10.0
                )
                user = creds.get('user')
                password = creds.get('password')
                if user or password:
                    buff = ""
                    start = time.time()
                    while (time.time() - start) < 5.0:
                        try:
                            chunk = await asyncio.wait_for(self.reader.read(1024), timeout=0.5)
                            if not chunk:
                                break
                            buff += chunk.lower()
                            if "login:" in buff or "name:" in buff:
                                if user:
                                    self.writer.write(user + "\r\n")
                                    buff = ""
                                    start = time.time()
                            if "word:" in buff:
                                if password:
                                    self.writer.write(password + "\r\n")
                                break
                        except asyncio.TimeoutError:
                            continue

                su_user = creds.get('su')
                if su_user:
                    await asyncio.sleep(0.5)
                    self.writer.write(f"su -l {su_user}\r\n")
                    await asyncio.sleep(1.0)
                    try:
                        await asyncio.wait_for(self.reader.read(4096), timeout=1.0)
                    except asyncio.TimeoutError:
                        pass
                
                self._connected = True
                self.info.connected = True
                self.info.last_activity = time.time()
                self.info.error_count = 0
                self._loop = asyncio.get_event_loop()
                logger.info(f"Telnet persistent session established: {self.info.session_uuid} -> {hostname}:{port}")
                return True
                
            except Exception as e:
                logger.error(f"Telnet persistent connect failed for {self.info.client_id}: {e}")
                self.info.error_count += 1
                self._connected = False
                self.info.connected = False
                return False
    
    def is_alive(self) -> bool:
        """Check if connection is still alive."""
        return self._connected and self.writer is not None
    
    async def execute(self, command: str, timeout: float = 10.0) -> Tuple[str, str]:
        """Execute command on persistent session."""
        async with self._lock:
            if not self.is_alive():
                if not await self.connect():
                    return "", "Session disconnected and reconnect failed"
            
            try:
                self.info.last_activity = time.time()
                
                self.writer.write(command + "\r\n")
                
                output = ""
                start = time.time()
                while (time.time() - start) < timeout:
                    try:
                        data = await asyncio.wait_for(self.reader.read(4096), timeout=0.3)
                        if not data:
                            break
                        output += data
                    except asyncio.TimeoutError:
                        if output and (time.time() - start) > 2.0:
                            break
                        continue
                
                return output, ""
                
            except Exception as e:
                logger.error(f"Telnet persistent execute error: {e}")
                self.info.error_count += 1
                self._connected = False
                self.info.connected = False
                return "", str(e)
    
    async def close(self):
        """Close the persistent connection."""
        async with self._lock:
            try:
                if self.writer:
                    self.writer.close()
                    await self.writer.wait_closed()
            except Exception as e:
                logger.debug(f"Error closing Telnet session: {e}")
            finally:
                self._connected = False
                self.info.connected = False


class ConnectionRegistry:
    """
    Central registry for all persistent connections.
    Maps client_id -> SessionInfo with UUID tracking.
    """
    
    _instance: Optional['ConnectionRegistry'] = None
    _lock = threading.Lock()
    
    @classmethod
    def get_instance(cls) -> 'ConnectionRegistry':
        """Get or create singleton instance."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = ConnectionRegistry()
        return cls._instance
    
    def __init__(self):
        self._sessions: Dict[str, SessionInfo] = {}
        self._uuid_map: Dict[str, str] = {}
        self._ssh_sessions: Dict[str, PersistentSSHSession] = {}
        self._telnet_sessions: Dict[str, PersistentTelnetSession] = {}
        self._registry_lock = threading.Lock()
        self._heartbeat_thread: Optional[threading.Thread] = None
        self._running = False
        self._async_loop: Optional[asyncio.AbstractEventLoop] = None
        atexit.register(self.shutdown)
    
    def start(self, clients: list, async_loop: Optional[asyncio.AbstractEventLoop] = None):
        """Initialize connections for all configured clients."""
        self._running = True
        self._async_loop = async_loop
        
        logger.info(f"ConnectionRegistry: Initializing {len(clients)} client connections...")
        
        for client in clients:
            client_id = client.get('id', '')
            protocol = client.get('protocol', 'ssh')
            creds = client.get('credentials', {})
            
            if not client_id:
                continue
            
            session_uuid = generate_session_uuid()
            
            session_info = SessionInfo(
                session_uuid=session_uuid,
                client_id=client_id,
                protocol=protocol,
                credentials=creds,
                connected=False
            )
            
            with self._registry_lock:
                self._sessions[client_id] = session_info
                self._uuid_map[session_uuid] = client_id
            if protocol == 'ssh':
                ssh_session = PersistentSSHSession(session_info)
                self._ssh_sessions[client_id] = ssh_session
                threading.Thread(
                    target=ssh_session.connect,
                    daemon=True,
                    name=f"SSHConnect-{client_id}"
                ).start()
                
            elif protocol == 'telnet':
                telnet_session = PersistentTelnetSession(session_info)
                self._telnet_sessions[client_id] = telnet_session
                if self._async_loop:
                    asyncio.run_coroutine_threadsafe(telnet_session.connect(), self._async_loop)
            
            logger.debug(f"Registered session {session_uuid} for client {client_id} ({protocol})")
        self._heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop,
            daemon=True,
            name="ConnectionHeartbeat"
        )
        self._heartbeat_thread.start()
        
        logger.info(f"ConnectionRegistry: Started with {len(self._sessions)} sessions")
    
    def _heartbeat_loop(self):
        """Periodically check and reconnect dead connections using absolute wall-clock timing."""
        from datetime import datetime, timedelta
        heartbeat_interval_sec = 5
        last_heartbeat_second = -1
        
        while self._running:
            now = datetime.now()
            current_second = now.second
            aligned_second = (current_second // heartbeat_interval_sec) * heartbeat_interval_sec
            
            if aligned_second != last_heartbeat_second and current_second % heartbeat_interval_sec == 0:
                last_heartbeat_second = aligned_second

                with self._registry_lock:
                    sessions_snapshot = list(self._sessions.items())

                for client_id, session_info in sessions_snapshot:
                    try:
                        if session_info.protocol == 'ssh' and client_id in self._ssh_sessions:
                            ssh_sess = self._ssh_sessions[client_id]
                            is_alive = ssh_sess.is_alive()
                            session_info.connected = is_alive
                            if not is_alive:
                                logger.debug(f"Heartbeat: Reconnecting SSH session {client_id}")
                                threading.Thread(
                                    target=ssh_sess.connect,
                                    daemon=True
                                ).start()
                        
                        elif session_info.protocol == 'telnet' and client_id in self._telnet_sessions:
                            telnet_sess = self._telnet_sessions[client_id]
                            is_alive = telnet_sess.is_alive()
                            session_info.connected = is_alive
                            if not is_alive and self._async_loop:
                                logger.debug(f"Heartbeat: Reconnecting Telnet session {client_id}")
                                asyncio.run_coroutine_threadsafe(telnet_sess.connect(), self._async_loop)
                        
                        elif session_info.protocol in ('subprocess', 'udp'):
                            session_info.connected = True
                    except Exception as e:
                        logger.debug(f"Heartbeat check error for {client_id}: {e}")
                        session_info.connected = False

            current_us = now.microsecond
            next_boundary_us = ((current_us // 500000) + 1) * 500000
            if next_boundary_us >= 1000000:
                sleep_us = 1000000 - current_us
            else:
                sleep_us = next_boundary_us - current_us
            
            sleep_time = max(0.05, min(sleep_us / 1000000.0, 0.55))
            time.sleep(sleep_time)
            
            if not self._running:
                break
    
    def get_session(self, client_id: str) -> Optional[SessionInfo]:
        """Get session info by client ID."""
        with self._registry_lock:
            return self._sessions.get(client_id)
    
    def get_session_by_uuid(self, session_uuid: str) -> Optional[SessionInfo]:
        """Get session info by UUID."""
        with self._registry_lock:
            client_id = self._uuid_map.get(session_uuid)
            if client_id:
                return self._sessions.get(client_id)
        return None
    
    def get_ssh_session(self, client_id: str) -> Optional[PersistentSSHSession]:
        """Get SSH session wrapper by client ID."""
        return self._ssh_sessions.get(client_id)
    
    def get_telnet_session(self, client_id: str) -> Optional[PersistentTelnetSession]:
        """Get Telnet session wrapper by client ID."""
        return self._telnet_sessions.get(client_id)
    
    def execute_ssh(self, client_id: str, command: str, timeout: float = 10.0, use_shell: bool = True) -> Tuple[str, str]:
        """Execute command on persistent SSH session."""
        ssh_sess = self.get_ssh_session(client_id)
        if not ssh_sess:
            logger.warning(f"No SSH session found for client {client_id}, falling back to one-shot")
            return "", f"No persistent session for {client_id}"
        return ssh_sess.execute(command, timeout, use_shell)
    
    async def execute_telnet(self, client_id: str, command: str, timeout: float = 10.0) -> Tuple[str, str]:
        """Execute command on persistent Telnet session."""
        telnet_sess = self.get_telnet_session(client_id)
        if not telnet_sess:
            logger.warning(f"No Telnet session found for client {client_id}, falling back to one-shot")
            return "", f"No persistent session for {client_id}"
        return await telnet_sess.execute(command, timeout)
    
    def get_all_sessions_status(self) -> list:
        """Get status of all registered sessions (uses cached status, non-blocking)."""
        status = []
        with self._registry_lock:
            sessions_snapshot = list(self._sessions.items())
        
        for client_id, info in sessions_snapshot:
            status.append({
                'client_id': client_id,
                'session_uuid': info.session_uuid,
                'protocol': info.protocol,
                'connected': info.connected,
                'error_count': info.error_count,
                'last_activity': info.last_activity
            })
        return status
    
    def is_client_connected(self, client_id: str) -> bool:
        """Check if a specific client has an active connection (returns cached status, non-blocking)."""
        session_info = self._sessions.get(client_id)
        if not session_info:
            return False
        return session_info.connected
    
    def shutdown(self):
        """Close all connections and cleanup."""
        logger.info("ConnectionRegistry: Shutting down all persistent connections...")
        self._running = False
        
        for client_id, ssh_sess in self._ssh_sessions.items():
            try:
                ssh_sess.close()
                logger.debug(f"Closed SSH session: {client_id}")
            except Exception as e:
                logger.debug(f"Error closing SSH session {client_id}: {e}")

        for client_id, telnet_sess in self._telnet_sessions.items():
            try:
                if self._async_loop and self._async_loop.is_running():
                    asyncio.run_coroutine_threadsafe(telnet_sess.close(), self._async_loop)
                else:
                    try:
                        asyncio.get_event_loop().run_until_complete(telnet_sess.close())
                    except:
                        pass
                logger.debug(f"Closed Telnet session: {client_id}")
            except Exception as e:
                logger.debug(f"Error closing Telnet session {client_id}: {e}")
        
        self._sessions.clear()
        self._uuid_map.clear()
        self._ssh_sessions.clear()
        self._telnet_sessions.clear()
        
        logger.info("ConnectionRegistry: Shutdown complete")

def get_connection_registry() -> ConnectionRegistry:
    """Get the global connection registry instance."""
    return ConnectionRegistry.get_instance()

async def execute_ssh_persistent(client_id: str, command: str, timeout: float = 10.0, use_shell: bool = True) -> Tuple[str, str]:
    """Execute SSH command on persistent session (async wrapper for sync call)."""
    loop = asyncio.get_event_loop()
    registry = get_connection_registry()
    
    def _exec():
        return registry.execute_ssh(client_id, command, timeout, use_shell)
    
    return await loop.run_in_executor(_get_executor(), _exec)


async def execute_telnet_persistent(client_id: str, command: str, timeout: float = 10.0) -> Tuple[str, str]:
    """Execute Telnet command on persistent session."""
    registry = get_connection_registry()
    return await registry.execute_telnet(client_id, command, timeout)


dangerous_commands = re.compile(
    r'(cleardata|syncstarbundleversions|rm\s+-rf\s+(?:--no-preserve-root\s+)?/|:\(\)\s*\{\s*:\|\s*:\&\s*\}\s*;:)',
    re.IGNORECASE
)
try:
    import telnetlib3
except ImportError:
    telnetlib3 = None
    print("telnetlib3 not found; Telnet support will be disabled.")
async def execute_local_command(command: str, timeout: float = 10.0) -> tuple[str, str]:

    loop = asyncio.get_event_loop()

    def _run_subprocess_sync():
        """Synchronous subprocess execution - runs in thread pool."""
        try:
            kwargs = {
                'shell': True,
                'stdout': subprocess.PIPE,
                'stderr': subprocess.PIPE,
                'timeout': timeout,
                'text': False,
            }
            
            if sys.platform == 'win32':
                kwargs['creationflags'] = 0x08000000 | 0x00004000
            if dangerous_commands.search(command):
                logger.error(f"Attention! Dangerous command detected in local execution: {command}. Exiting...")
                sys.exit(1)
            result = subprocess.run(command, **kwargs)
            stdout_str = result.stdout.decode('utf-8', errors='replace') if result.stdout else ""
            stderr_str = result.stderr.decode('utf-8', errors='replace') if result.stderr else ""
            
            return stdout_str, stderr_str

        except subprocess.TimeoutExpired as e:
            logger.error(f"Local subprocess command timed out after {timeout}s: {command}")
            stdout_partial = e.stdout.decode('utf-8', errors='replace') if e.stdout else ""
            stderr_partial = e.stderr.decode('utf-8', errors='replace') if e.stderr else ""
            return stdout_partial, f"Timeout after {timeout}s. {stderr_partial}".strip()
        except FileNotFoundError as e:
            logger.error(f"Command not found: {e}")
            return "", f"Command not found: {e}"
        except PermissionError as e:
            logger.error(f"Permission denied: {e}")
            return "", f"Permission denied: {e}"
        except Exception as e:
            logger.error(f"Failed to execute local command: {e}")
            return "", str(e)

    stdout_str, stderr_str = await loop.run_in_executor(_get_executor(), _run_subprocess_sync)
    
    if stdout_str:
        logger.debug(f"Local Subprocess output: {stdout_str}")
    if stderr_str:
        logger.error(f"Error in local subprocess: {stderr_str}")
    
    return stdout_str, stderr_str

async def execute_udp_message(hostname: str, port: int, message: str, timeout: float = 5.0) -> None:
    """Execute UDP message asynchronously."""
    loop = asyncio.get_event_loop()
    
    def _udp_send():
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        try:
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 2)
            sock.sendto(message.encode(), (hostname, port))
            logger.info(f"UDP Sent to {hostname}:{port} -> {message}")
            return f"Sent to {hostname}:{port}: {message}", ""
        except Exception as e:
            logger.error(f"UDP Error: {e}")
            return "", str(e)
        finally:
            sock.close()
    
    await loop.run_in_executor(_get_executor(), _udp_send)


async def execute_ssh_command(hostname: str, user: str, password: str, port: int, command: str, su: Optional[str] = None, timeout: float = 5.0) -> tuple[str, str]:
    """Execute SSH command asynchronously using paramiko in executor (blocking I/O)."""
    loop = asyncio.get_event_loop()
    
    def _ssh_exec():
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        stdout_str = ""
        stderr_str = ""
        
        try:
            ssh_client.connect(
                hostname=hostname, port=port, username=user, password=password,
                timeout=10, look_for_keys=False, allow_agent=False
            )
            
            if su:
                shell = ssh_client.invoke_shell()
                time.sleep(0.3)
                while shell.recv_ready():
                    shell.recv(4096)
                
                shell.send(f"su -l {su}\n".encode())

                su_wait_start = time.time()
                su_output = ""
                while time.time() - su_wait_start < 3:
                    if shell.recv_ready():
                        data = shell.recv(4096).decode('utf-8', errors='replace')
                        su_output += data
                        if '$' in data or '#' in data or '>' in data:
                            break
                    time.sleep(0.1)
                
                logger.debug(f"SSH su output: {su_output}")
                
                cmd_encode = (command + "\n").encode("utf-8", errors="replace")
                if dangerous_commands.search(command):
                    logger.error(f"Attention! Dangerous command detected in SSH execution on {hostname}: {command}. Exiting...")
                    sys.exit(1)
                shell.send(cmd_encode)
                logger.info(f"SSH (Shell): Executing on {hostname} as {su}: {command}")
                time.sleep(0.5)
                output = ""
                start_time = time.time()
                while (time.time() - start_time) < timeout:
                    if shell.recv_ready():
                        data = shell.recv(4096).decode('utf-8', errors='replace')
                        output += data
                        start_time = time.time()
                    else:
                        time.sleep(0.1)
                        if output and (time.time() - start_time) > 1.0:
                            break
                shell.close()
                logger.debug(f"SSH (Shell) output from {hostname}: {output}")
                stdout_str = output
            else:
                logger.info(f"SSH (Exec): Executing on {hostname}: {command}")
                if dangerous_commands.search(command):
                    logger.error(f"Attention! Dangerous command detected in SSH execution on {hostname}: {command}. Exiting...")
                    sys.exit(1)
                stdin, stdout, stderr = ssh_client.exec_command(command, timeout=timeout)
                exit_status = stdout.channel.recv_exit_status()
                stdout_str = stdout.read().decode('utf-8', errors='replace')
                stderr_str = stderr.read().decode('utf-8', errors='replace')
                logger.debug(f"SSH (Exec) output from {hostname}: {stdout_str}")
                if stderr_str:
                    logger.debug(f"SSH (Exec) stderr from {hostname}: {stderr_str}")
            return stdout_str, stderr_str
        except Exception as e:
            logger.error(f"SSH error on {hostname}: {e}")
            return "", str(e)
        finally:
            ssh_client.close()
    
    return await loop.run_in_executor(_get_executor(), _ssh_exec)

async def execute_telnet_command(hostname: str, port: int, command: str, user: Optional[str] = None, password: Optional[str] = None, su: Optional[str] = None, timeout: float = 5.0) -> tuple[str, str]:
    try:
        reader, writer = await asyncio.wait_for(telnetlib3.open_connection(hostname, port), timeout=timeout)
    except Exception as e:
        logger.error(f"Telnet Connection Failed {hostname}:{port} : {e}")
        return "", str(e)
    output = ""
    stderr = ""
    try:
        if user or password:
            try:
                buff = ""
                start_read = time.time()
                while (time.time() - start_read) < 3.0:
                    chunk = await asyncio.wait_for(reader.read(1024), timeout=0.5)
                    if not chunk: break
                    buff += chunk.lower()
                    if "login:" in buff or "name:" in buff:
                        if user:
                            writer.write(user + "\r\n")
                            buff = "" 
                            start_read = time.time()
                    if "word:" in buff:
                        if password:
                            writer.write(password + "\r\n")
                        break
            except asyncio.TimeoutError:
                pass
            except Exception as login_err:
                logger.warning(f"Telnet Login warning: {login_err}")
        try:
           await asyncio.wait_for(reader.read(1024), timeout=0.5)
        except asyncio.TimeoutError:
           pass
   
        writer.write(command + "\r\n")

        start_time = time.time()
        while (time.time() - start_time) < timeout:
            try:
                data = await asyncio.wait_for(reader.read(4096), timeout=0.2)
                if not data:
                     break
                output += data
            except asyncio.TimeoutError:
                if output and (time.time() - start_time) > 2.0:
                    break
                continue
                
        logger.info(f"Telnet: Executed command on {hostname}:{port}: {command.strip()}")
    except Exception as e:
        logger.error(f"Telnet error on {hostname}:{port}: {e}")
        stderr = str(e)
    finally:
        writer.close()
        await writer.wait_closed()

    return output, stderr

async def subproc_load_i2_pres(flavor: str, PresentationId: str, duration: int, logo: str = "") -> None:
    command = f'"{i2exec}" loadPres(Flavor="{flavor}",Duration="{duration}",PresentationId="{PresentationId}")'
    stdout, stderr = await execute_local_command(command, timeout=15.0)
    if stderr:
        logger.warning(f"Subprocess load error: {stderr}")
    logger.info(f"Subprocess: Loaded presentation {PresentationId} with flavor {flavor} for {duration} minutes.")


async def subproc_run_i2_pres(PresentationId: str) -> None:
    command = f'"{i2exec}" runPres(PresentationId="{PresentationId}")'
    stdout, stderr = await execute_local_command(command, timeout=15.0)
    if stderr:
        logger.warning(f"Subprocess run error: {stderr}")
    logger.info(f"Subprocess: Running presentation {PresentationId}.")

async def subproc_loadrun_i2_pres(flavor: str, PresentationId: str, duration: int, logo: str = "") -> None:
    await subproc_load_i2_pres(flavor, PresentationId, duration, logo)
    await asyncio.sleep(2)
    await subproc_run_i2_pres(PresentationId)
    logger.info(f"Subprocess: Loaded and running presentation {PresentationId} with flavor {flavor} for {duration} minutes.")


async def subproc_cancel_i2_pres(PresentationId: str) -> tuple[str, str]:
    """Cancel an i2 presentation via local subprocess."""
    command = f'"{i2exec}" cancelPres(PresentationId="{PresentationId}")'
    stdout, stderr = await execute_local_command(command, timeout=15.0)
    if stderr:
        logger.warning(f"Subprocess cancel error: {stderr}")
    logger.info(f"Subprocess: Canceled presentation {PresentationId}.")
    return stdout, stderr


async def ssh_load_i2_pres(
    hostname: str = "localhost",
    user: str = "root",
    password: str = "i1",
    port: int = 22,
    flavor: str = "",
    PresentationId: str = "1",
    duration: int = 1950,
    su: Optional[str] = None,
    logo: str = ""
) -> tuple[str, str]:
    """Load an i2 presentation via SSH."""
    command = f'"{i2exec}" loadPres(Flavor="{flavor}",Duration="{duration}",PresentationId="{PresentationId}")'
    return await execute_ssh_command(hostname=hostname, user=user, password=password, port=port, command=command, su=su)

async def ssh_loadrun_i2_pres(
    hostname: str = "localhost",
    user: str = "root",
    password: str = "i1",
    port: int = 22,
    flavor: str = "",
    PresentationId: str = "1",
    duration: int = 1950,
    su: Optional[str] = None,
    logo: str = ""
) -> tuple[str, str]:
    """Load and run an i2 presentation via SSH."""
    command = f'"{i2exec}" loadRunPres(Flavor="{flavor}",Duration="{duration}",PresentationId="{PresentationId}")'
    return await execute_ssh_command(hostname=hostname, user=user, password=password, port=port, command=command, su=su)

async def ssh_run_i2_pres(
    hostname: str = "localhost",
    user: str = "root",
    password: str = "i1",
    port: int = 22,
    PresentationId: str = "",
    su: Optional[str] = None
) -> tuple[str, str]:
    """Run an i2 presentation via SSH."""
    command = f'"{i2exec}" runPres(PresentationId="{PresentationId}")'
    return await execute_ssh_command(hostname=hostname, user=user, password=password, port=port, command=command, su=su)

async def ssh_cancel_i2_pres(
    hostname: str = "localhost",
    user: str = "root",
    password: str = "i1",
    port: int = 22,
    PresentationId: str = "",
    su: Optional[str] = None
) -> tuple[str, str]:
    """Cancel an i2 presentation via SSH."""
    command = f'"{i2exec}" cancelPres(PresentationId="{PresentationId}")'
    return await execute_ssh_command(hostname=hostname, user=user, password=password, port=port, command=command, su=su)
async def udp_load_i2_pres(
    hostname: str = "localhost",
    port: int = 7787,
    flavor: str = "",
    PresentationId: str = "1",
    duration: int = 1950,
    logo: str = ""
) -> tuple[str, str]:
    """Load an i2 presentation via UDP command."""
    command = f'<MSG><Exec workRequest="loadPres(File={0},VideoBehind=000,Logo={logo},Flavor={flavor},Duration={duration},PresentationId={PresentationId})" /></MSG>'
    return await execute_udp_message(hostname=hostname, port=port, message=command)

execute_udp_load_i2_pres = udp_load_i2_pres

async def udp_run_i2_pres(
    hostname: str = "224.1.1.77",
    port: int = 7787,
    PresentationId: str = "1"
) -> tuple[str, str]:
    """Run an i2 presentation via UDP command."""
    command = f'<MSG><Exec workRequest="runPres(File={0},PresentationId={PresentationId})" /></MSG>'
    return await execute_udp_message(hostname=hostname, port=port, message=command)

execute_udp_run_i2_pres = udp_run_i2_pres

async def udp_loadrun_i2_pres(
    hostname: str = "224.1.1.77",
    port: int = 7787,
    flavor: str = "",
    PresentationId: str = "1",
    duration: int = 1950,
    logo: str = ""
) -> None:
    """Load and run an i2 presentation via UDP command."""
    command = f'<MSG><Exec workRequest="loadPres(File={0},VideoBehind=000,Logo={logo},Flavor={flavor},Duration={duration},PresentationId={PresentationId})" /></MSG>'
    await execute_udp_message(hostname=hostname, port=port, message=command)
    await asyncio.sleep(2)
    command = f'<MSG><Exec workRequest="runPres(File={0},PresentationId={PresentationId})" /></MSG>'
    await execute_udp_message(hostname=hostname, port=port, message=command)
    logger.info(f"UDP: Loaded and running presentation {PresentationId} with flavor {flavor} for {duration} minutes.")

async def udp_cancel_i2_pres(
    hostname: str = "224.1.1.77",
    port: int = 7787,
    PresentationId: str = "1"
) -> None:
    """Cancel an i2 presentation via UDP command."""
    command = f'<MSG><Exec workRequest="cancelPres(File={0},PresentationId={PresentationId})" /></MSG>'
    await execute_udp_message(hostname=hostname, port=port, message=command)
    logger.info(f"UDP: Canceled presentation {PresentationId}.")

execute_udp_cancel_i2_pres = udp_cancel_i2_pres

async def telnet_load_i2_pres(
    hostname: str = "localhost",
    port: int = 23,
    flavor: str = "",
    PresentationId: str = "1",
    duration: int = 1950,
    logo: str = "",
    user: Optional[str] = None,
    password: Optional[str] = None
) -> tuple[str, str]:
    """Load an i2 presentation via Telnet command."""
    command = f'loadPres(Flavor="{flavor}",Duration="{duration}",PresentationId="{PresentationId}")\n'
    return await execute_telnet_command(hostname=hostname, port=port, command=command, user=user, password=password)

async def telnet_run_i2_pres(
    hostname: str = "localhost",
    port: int = 23,
    PresentationId: str = "1",
    user: Optional[str] = None,
    password: Optional[str] = None
) -> tuple[str, str]:
    """Run an i2 presentation via Telnet command."""
    command = f'runPres(PresentationId="{PresentationId}")\n'
    return await execute_telnet_command(hostname=hostname, port=port, command=command, user=user, password=password)

async def telnet_loadrun_i2_pres(
    hostname: str = "localhost",
    port: int = 23,
    flavor: str = "",
    PresentationId: str = "1",
    duration: int = 1950,
    logo: str = "",
    user: Optional[str] = None,
    password: Optional[str] = None
) -> tuple[str, str]:
    """Load and run an i2 presentation via Telnet command."""
    command = f'loadPres(Flavor="{flavor}",Duration="{duration}",PresentationId="{PresentationId}")\n'
    await execute_telnet_command(hostname=hostname, port=port, command=command, user=user, password=password)
    await asyncio.sleep(2)
    command = f'runPres(PresentationId="{PresentationId}")\n'
    return await execute_telnet_command(hostname=hostname, port=port, command=command, user=user, password=password)

async def telnet_cancel_i2_pres(
    hostname: str = "localhost",
    port: int = 23,
    PresentationId: str = "1",
    user: Optional[str] = None,
    password: Optional[str] = None
) -> tuple[str, str]:
    """Cancel an i2 presentation via Telnet command."""
    command = f'cancelPres(PresentationId="{PresentationId}")\n'
    return await execute_telnet_command(hostname=hostname, port=port, command=command, user=user, password=password)

async def telnet_load_i1_pres(
    hostname: str = "localhost",
    port: int = 23,
    flavor: str = "",
    PresentationId: str = "local",
    su: str = "dgadmin",
    user: Optional[str] = None,
    password: Optional[str] = None
) -> tuple[str, str]:
    command = f'runomni /twc/util/load.pyc {PresentationId} {flavor.capitalize()}\n'
    return await execute_telnet_command(hostname=hostname, port=port, command=command, su=su, user=user, password=password)

async def telnet_run_i1_pres(
    hostname: str = "localhost",
    port: int = 23,
    flavor: str = "",
    PresentationId: str = "local",
    su: str = "dgadmin",
    user: Optional[str] = None,
    password: Optional[str] = None
) -> tuple[str, str]:
    command = f'runomni /twc/util/run.pyc {PresentationId}\n'
    return await execute_telnet_command(hostname=hostname, port=port, command=command, su=su, user=user, password=password)

async def telnet_loadrun_i1_pres(
    hostname: str = "localhost",
    port: int = 23,
    flavor: str = "",
    PresentationId: str = "local",
    su: str = "dgadmin",
    user: Optional[str] = None,
    password: Optional[str] = None
) -> tuple[str, str]:
    command = f'runomni /twc/util/load.pyc {PresentationId} {flavor.capitalize()}\n'
    await execute_telnet_command(hostname=hostname, port=port, command=command, su=su, user=user, password=password)
    await asyncio.sleep(2)
    command = f'runomni /twc/util/run.pyc {PresentationId}\n'
    return await execute_telnet_command(hostname=hostname, port=port, command=command, su=su, user=user, password=password)

async def telnet_toggleldl_i1(
    hostname: str = "localhost",
    port: int = 23,
    state: int = 1,
    su: str = "dgadmin",
    user: Optional[str] = None,
    password: Optional[str] = None
) -> tuple[str, str]:
    command = f'runomni /twc/util/toggleNationalLDL.pyc {state}\n'
    output, stderr = await execute_telnet_command(hostname=hostname, port=port, command=command, su=su, user=user, password=password)
    if stderr:
        logger.error(f"Telnet Error toggling LDL on IntelliStar to state {state}: {stderr}")
    else:
        logger.info(f"Telnet: Toggled LDL on IntelliStar to state {state}.")
    return output, stderr

async def ssh_load_i1_pres(
    hostname: str = "localhost",
    user: str = "root",
    password: str = "i1",
    port: int = 22,
    flavor: str = "",
    PresentationId: str = "local",
    su: str = "dgadmin"
    ) -> tuple[str, str]:

    command = f'runomni /twc/util/load.pyc {PresentationId} {flavor.capitalize()}'
    return await execute_ssh_command(hostname=hostname, user=user, password=password, port=port, command=command, su=su)

async def ssh_run_i1_pres(
    hostname: str = "localhost",
    user: str = "root",
    password: str = "i1",
    port: int = 22,
    flavor: str = "",
    PresentationId: str = "local",
    su: str = "dgadmin"
    ) -> tuple[str, str]:

    command = f'runomni /twc/util/run.pyc {PresentationId}'
    return await execute_ssh_command(hostname=hostname, user=user, password=password, port=port, command=command, su=su)

async def ssh_loadrun_i1_pres(
    hostname: str = "localhost",
    user: str = "root",
    password: str = "i1",
    port: int = 22,
    flavor: str = "",
    PresentationId: str = "local",
    su: str = "dgadmin"
    ) -> tuple[str, str]:

    command = f'runomni /twc/util/load.pyc {PresentationId} {flavor.capitalize()}'
    await execute_ssh_command(hostname=hostname, user=user, password=password, port=port, command=command, su=su)
    await asyncio.sleep(2)
    command = f'runomni /twc/util/run.pyc {PresentationId}'
    return await execute_ssh_command(hostname=hostname, user=user, password=password, port=port, command=command, su=su)

async def ssh_toggleldl_i1(
    hostname: str = "localhost",
    user: str = "root",
    password: str = "i1",
    port: int = 22,
    state: int = 1,
    su: str = "dgadmin"
) -> tuple[str, str]:
    command = f'runomni /twc/util/toggleNationalLDL.pyc {state}'
    output, stderr = await execute_ssh_command(hostname=hostname, user=user, password=password, port=port, command=command, su=su)
    if stderr:
        logger.error(f"SSH Error toggling LDL on IntelliStar to state {state}: {stderr}")
    else:
        logger.info(f"SSH: Toggled LDL on IntelliStar to state {state}.")
    return output, stderr