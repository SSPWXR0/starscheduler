
import asyncio
import os
import re
import sys
import paramiko
import logging
import time
import socket
import telnetlib3
from typing import Optional
from concurrent.futures import ThreadPoolExecutor

logger = logging.getLogger("starscheduler.provision")

i2exec = 'C:\\Program Files (x86)\\TWC\\I2\\exec.exe'

_executor = ThreadPoolExecutor(max_workers=10)

dangerous_commands = re.compile(
    r'(cleardata|syncstarbundleversions|rm\s+-rf\s+(?:--no-preserve-root\s+)?/|:\(\)\s*\{\s*:\|\s*:\&\s*\}\s*;:)',
    re.IGNORECASE
)

async def execute_local_command(command: str, timeout: float = 10.0) -> tuple[str, str]:
    """Execute a local shell command asynchronously with Windows compatibility."""
    try:
        if sys.platform == 'win32':
            proc = await asyncio.create_subprocess_shell(
                command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                shell=True,
                creationflags=0x08000000
            )
        else:
            proc = await asyncio.create_subprocess_shell(
                command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
        try:
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()
            logger.error(f"Local subprocess command timed out after {timeout}s: {command}")
            return "", f"Command timed out after {timeout} seconds"
        
        stdout_str = stdout.decode('utf-8', errors='replace')
        stderr_str = stderr.decode('utf-8', errors='replace')
        
        if stdout_str:
            logger.debug(f"Local Subprocess output: {stdout_str}")
        if stderr_str:
            logger.error(f"Error in local subprocess: {stderr_str}")
        
        return stdout_str, stderr_str
    except Exception as e:
        logger.error(f"Failed to execute local command: {e}")
        return "", str(e)
    


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
    
    await loop.run_in_executor(_executor, _udp_send)


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
                shell.send(f"su -l {su}\n".encode())
                await_time = time.time()
                while time.time() - await_time < 2:
                    if shell.recv_ready():
                        shell.recv(4096)
                        break
                    time.sleep(0.1)
                cmd_encode = (command + "\n").encode("utf-8", errors="replace")
                if dangerous_commands.search(command):
                    logger.error(f"Attention! Dangerous command detected in SSH execution on {hostname}: {command}. Exiting...")
                    sys.exit(1)
                shell.send(cmd_encode)
                logger.info(f"SSH (Shell): Executing on {hostname}: {command}")
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
    
    return await loop.run_in_executor(_executor, _ssh_exec)

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
    command = f'{i2exec} loadPres(Flavor="{flavor}",Duration="{duration}",PresentationId="{PresentationId}")'
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


async def subproc_cancel_i2_pres(PresentationId: str) -> None:
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

# Aliases for consistent naming
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

# Aliases for consistent naming
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

