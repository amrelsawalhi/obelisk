import os
import sys
import json
import asyncio
import logging
from typing import Optional
from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from app.config import settings
from app.core.security import authorize_websocket

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


# One shell per open tab is the intent; the cap is what keeps a forgotten tab from
# turning into an unbounded number of processes on the host.
_active_sessions = 0
MAX_MESSAGE_CHARS = 65_536


def _clamp(value: object, low: int, high: int, default: int) -> int:
    try:
        number = int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return default
    return max(low, min(high, number))


# One shell per open tab is the intent; the cap is what keeps a forgotten tab from
# turning into an unbounded number of processes on the host.
_active_sessions = 0
MAX_MESSAGE_CHARS = 65_536

CRLF = chr(13) + chr(10)
RED = chr(27) + "[31m"
YELLOW = chr(27) + "[33m"
RESET = chr(27) + "[0m"


def _clamp(value: object, low: int, high: int, default: int) -> int:
    try:
        number = int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return default
    return max(low, min(high, number))


@router.websocket("/ws")
async def terminal_websocket(websocket: WebSocket):
    """A real shell on the host, so this is the one socket that asks who is calling.

    Refusals close *before* ``accept`` so a browser gets a handshake denial instead of
    a socket that opens and then dies. Origin is checked even while auth is off: that
    is what stops a page on another site from driving this shell through the
    operator's browser.
    """
    global _active_sessions

    if not settings.TERMINAL_ENABLED:
        await websocket.close(code=1008, reason="The terminal is disabled on this deployment.")
        return

    principal = await authorize_websocket(websocket, "terminal")
    if principal is None:
        return

    if _active_sessions >= max(1, int(settings.TERMINAL_MAX_SESSIONS)):
        logger.warning("Refused terminal: %s session(s) already open.", _active_sessions)
        await websocket.close(code=1013, reason="Too many terminal sessions open.")
        return

    await websocket.accept()
    _active_sessions += 1

    client = (
        f"{websocket.client.host}:{websocket.client.port}" if websocket.client else "unknown"
    )
    cwd = str(settings.git_repo_path.resolve())
    logger.info("Terminal opened for %r from %s (cwd=%s)", principal.get("sub"), client, cwd)

    idle_seconds = max(30, int(settings.TERMINAL_IDLE_TIMEOUT_SECONDS))
    session = create_terminal_session(cwd)
    read_task: Optional[asyncio.Task] = None

    try:
        try:
            await session.start(cols=100, rows=30)
        except Exception as exc:
            logger.error("Failed to start terminal session: %s", exc, exc_info=True)
            await websocket.send_text(
                f"{CRLF}{RED}Failed to start terminal: {exc}{RESET}{CRLF}"
            )
            return

        shell_label = "PowerShell (Windows)" if sys.platform == "win32" else "bash (Linux)"
        await websocket.send_text(json.dumps({
            "type": "session_info",
            "shell": shell_label,
            "os": sys.platform,
            "principal": principal.get("sub"),
        }))

        async def process_to_websocket():
            try:
                while True:
                    output = await session.read()
                    if output is None:
                        break
                    await websocket.send_text(output)
            except (WebSocketDisconnect, asyncio.CancelledError):
                pass
            except Exception as exc:
                logger.debug("Terminal read loop ended: %s", exc)

        read_task = asyncio.create_task(process_to_websocket())

        try:
            while True:
                try:
                    message = await asyncio.wait_for(
                        websocket.receive_text(), timeout=idle_seconds
                    )
                except asyncio.TimeoutError:
                    logger.info("Terminal idle for %ss; closing.", idle_seconds)
                    await websocket.send_text(
                        f"{CRLF}{YELLOW}Session closed after {idle_seconds}s of inactivity.{RESET}{CRLF}"
                    )
                    break

                if len(message) > MAX_MESSAGE_CHARS:
                    logger.warning("Terminal input of %s chars refused.", len(message))
                    break

                payload = None
                if message.startswith("{") and message.endswith("}"):
                    try:
                        decoded = json.loads(message)
                        payload = decoded if isinstance(decoded, dict) else None
                    except json.JSONDecodeError:
                        payload = None

                if payload is not None:
                    kind = payload.get("type")
                    if kind == "resize":
                        await session.resize(
                            _clamp(payload.get("cols"), 1, 500, 80),
                            _clamp(payload.get("rows"), 1, 200, 24),
                        )
                        continue
                    if kind == "input":
                        await session.write(str(payload.get("data", "")))
                        continue

                await session.write(message)
        except (WebSocketDisconnect, asyncio.CancelledError):
            pass
        except Exception as exc:
            logger.warning("Terminal websocket error: %s", exc)
    finally:
        if read_task is not None:
            read_task.cancel()
        await session.close()
        _active_sessions = max(0, _active_sessions - 1)
        logger.info(
            "Terminal closed for %r (%s session(s) open)",
            principal.get("sub"),
            _active_sessions,
        )
