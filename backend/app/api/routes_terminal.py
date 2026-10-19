import os
import sys
import json
import asyncio
import logging
from typing import Optional
from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from app.config import settings

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/terminal", tags=["terminal"])

# Attempt to load platform-specific PTY drivers
HAS_WINPTY = False
HAS_UNIXPTY = False

if sys.platform == "win32":
    try:
        from winpty import PtyProcess
        HAS_WINPTY = True
    except ImportError:
        HAS_WINPTY = False
else:
    try:
        import pty
        import fcntl
        import termios
        import struct
        HAS_UNIXPTY = True
    except ImportError:
        HAS_UNIXPTY = False


class BaseTerminalSession:
    async def start(self, cols: int = 80, rows: int = 24):
        raise NotImplementedError

    async def write(self, data: str):
        raise NotImplementedError

    async def resize(self, cols: int, rows: int):
        pass

    async def read(self) -> Optional[str]:
        raise NotImplementedError

    async def close(self):
        raise NotImplementedError


class WinPtySession(BaseTerminalSession):
    def __init__(self, cwd: str):
        self.cwd = cwd
        self.proc: Optional[PtyProcess] = None
        self._alive = False

    async def start(self, cols: int = 80, rows: int = 24):
        # Prefer powershell.exe, fallback to cmd.exe
        shell = "powershell.exe"
        self.proc = PtyProcess.spawn(
            shell,
            cwd=self.cwd,
            dimensions=(rows, cols),
        )
        self._alive = True

    async def write(self, data: str):
        if self.proc and self._alive:
            await asyncio.to_thread(self.proc.write, data)

    async def resize(self, cols: int, rows: int):
        if self.proc and self._alive:
            try:
                await asyncio.to_thread(self.proc.set_winsize, rows, cols)
            except Exception as e:
                logger.warning(f"Failed to resize winpty window: {e}")

    async def read(self) -> Optional[str]:
        if not self.proc or not self._alive:
            return None
        try:
            # Read non-blocking / chunk
            chunk = await asyncio.to_thread(self.proc.read, 2048)
            return chunk
        except (EOFError, OSError):
            self._alive = False
            return None

    async def close(self):
        self._alive = False
        if self.proc:
            try:
                self.proc.close()
            except Exception:
                pass
            self.proc = None


class UnixPtySession(BaseTerminalSession):
    def __init__(self, cwd: str):
        self.cwd = cwd
        self.master_fd: Optional[int] = None
        self.proc: Optional[asyncio.subprocess.Process] = None
        self._alive = False

    async def start(self, cols: int = 80, rows: int = 24):
        import pty
        import fcntl
        import termios
        import struct

        master, slave = pty.openpty()
        self.master_fd = master

        # Set initial size
        winsize = struct.pack("HHHH", rows, cols, 0, 0)
        fcntl.ioctl(master, termios.TIOCSWINSZ, winsize)

        shell = os.environ.get("SHELL", "/bin/bash")
        if not os.path.exists(shell):
            shell = "/bin/sh"

        self.proc = await asyncio.create_subprocess_exec(
            shell,
            stdin=slave,
            stdout=slave,
            stderr=slave,
            cwd=self.cwd,
            preexec_fn=os.setsid,
        )
        os.close(slave)
        self._alive = True

    async def write(self, data: str):
        if self.master_fd is not None and self._alive:
            os.write(self.master_fd, data.encode("utf-8", errors="replace"))

    async def resize(self, cols: int, rows: int):
        if self.master_fd is not None and self._alive:
            try:
                import fcntl
                import termios
                import struct
                winsize = struct.pack("HHHH", rows, cols, 0, 0)
                fcntl.ioctl(self.master_fd, termios.TIOCSWINSZ, winsize)
            except Exception as e:
                logger.warning(f"Failed to resize unix pty: {e}")

    async def read(self) -> Optional[str]:
        if self.master_fd is None or not self._alive:
            return None
        try:
            loop = asyncio.get_running_loop()
            raw = await loop.run_in_executor(None, os.read, self.master_fd, 2048)
            if not raw:
                self._alive = False
                return None
            return raw.decode("utf-8", errors="replace")
        except (OSError, EOFError):
            self._alive = False
            return None

    async def close(self):
        self._alive = False
        if self.master_fd is not None:
            try:
                os.close(self.master_fd)
            except OSError:
                pass
            self.master_fd = None
        if self.proc:
            try:
                self.proc.terminate()
            except ProcessLookupError:
                pass
            self.proc = None


class PipeFallbackSession(BaseTerminalSession):
    """
    Subprocess fallback if no pseudo-terminal driver is available.
    """
    def __init__(self, cwd: str):
        self.cwd = cwd
        self.proc: Optional[asyncio.subprocess.Process] = None
        self._alive = False

    async def start(self, cols: int = 80, rows: int = 24):
        shell = "powershell.exe" if sys.platform == "win32" else "/bin/sh"
        self.proc = await asyncio.create_subprocess_exec(
            shell,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            cwd=self.cwd,
        )
        self._alive = True

    async def write(self, data: str):
        if self.proc and self.proc.stdin and self._alive:
            self.proc.stdin.write(data.encode("utf-8", errors="replace"))
            await self.proc.stdin.drain()

    async def read(self) -> Optional[str]:
        if not self.proc or not self.proc.stdout or not self._alive:
            return None
        try:
            chunk = await self.proc.stdout.read(1024)
            if not chunk:
                self._alive = False
                return None
            return chunk.decode("utf-8", errors="replace")
        except Exception:
            self._alive = False
            return None

    async def close(self):
        self._alive = False
        if self.proc:
            try:
                self.proc.terminate()
            except ProcessLookupError:
                pass
            self.proc = None


def create_terminal_session(cwd: str) -> BaseTerminalSession:
    if sys.platform == "win32" and HAS_WINPTY:
        return WinPtySession(cwd)
    elif sys.platform != "win32" and HAS_UNIXPTY:
        return UnixPtySession(cwd)
    else:
        return PipeFallbackSession(cwd)


@router.websocket("/ws")
async def terminal_websocket(websocket: WebSocket):
    await websocket.accept()

    cwd = str(settings.git_repo_path.resolve())
    session = create_terminal_session(cwd)

    try:
        await session.start(cols=100, rows=30)
        shell_label = "PowerShell (Windows)" if sys.platform == "win32" else "bash (Linux)"
        await websocket.send_text(json.dumps({"type": "session_info", "shell": shell_label, "os": sys.platform}))
    except Exception as e:

        logger.error(f"Failed to start terminal session: {e}", exc_info=True)
        await websocket.send_text(f"\r\n\x1b[31mFailed to start terminal: {str(e)}\x1b[0m\r\n")
        await websocket.close()
        return

    # Task for reading from process and sending to client
    async def process_to_websocket():
        try:
            while True:
                output = await session.read()
                if output is None:
                    break
                await websocket.send_text(output)
        except (WebSocketDisconnect, asyncio.CancelledError):
            pass
        except Exception as e:
            logger.debug(f"Terminal read loop ended: {e}")

    read_task = asyncio.create_task(process_to_websocket())

    try:
        while True:
            message = await websocket.receive_text()
            # Check if message is a JSON control frame (e.g. resize)
            if message.startswith("{") and message.endswith("}"):
                try:
                    payload = json.loads(message)
                    msg_type = payload.get("type")
                    if msg_type == "resize":
                        cols = int(payload.get("cols", 80))
                        rows = int(payload.get("rows", 24))
                        await session.resize(cols, rows)
                        continue
                    elif msg_type == "input":
                        data = payload.get("data", "")
                        await session.write(data)
                        continue
                except json.JSONDecodeError:
                    pass

            # Otherwise write raw keystrokes/data to terminal
            await session.write(message)

    except (WebSocketDisconnect, asyncio.CancelledError):
        pass
    except Exception as e:
        logger.warning(f"Terminal websocket error: {e}")
    finally:
        read_task.cancel()
        await session.close()
