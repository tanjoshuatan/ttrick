from __future__ import annotations

import asyncio
import base64
import json
import os
import platform
import socket
import uuid
from pathlib import Path

import websockets

HERE = Path(__file__).resolve().parent
HOST_FILE = HERE / "host.txt"

RETRY_MIN = 1.0
RETRY_MAX = 15.0
MAX_TRANSFER = 32 * 1024 * 1024  # 32 MB cap per file transfer

IS_WINDOWS = platform.system() == "Windows"

_delay = RETRY_MIN


def log(msg: str) -> None:
    print(f"[client] {msg}", flush=True)


# ---------------------------------------------------------------- config

def read_host_url() -> str | None:
    try:
        text = HOST_FILE.read_text(encoding="utf-8")
    except FileNotFoundError:
        log(f"{HOST_FILE.name} not found")
        return None
    except OSError as e:
        log(f"cannot read {HOST_FILE.name}: {e}")
        return None

    for line in text.splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            return line
    log(f"{HOST_FILE.name} is empty")
    return None


def to_ws_url(base: str) -> str:
    base = base.rstrip("/")
    if base.startswith("https://"):
        base = "wss://" + base[len("https://"):]
    elif base.startswith("http://"):
        base = "ws://" + base[len("http://"):]
    elif not base.startswith(("ws://", "wss://")):
        base = "ws://" + base
    return f"{base}/ws?role=client"


# ---------------------------------------------------------------- persistent shell

class PersistentShell:
    """One shell process per WS session. Commands run serially, so state persists."""

    def __init__(self, ws, send_lock: asyncio.Lock):
        self.ws = ws
        self.send_lock = send_lock
        self.proc: asyncio.subprocess.Process | None = None
        self.pump_task: asyncio.Task | None = None
        self.lock = asyncio.Lock()
        self.current: dict | None = None

    async def _send(self, payload: dict) -> None:
        async with self.send_lock:
            try:
                await self.ws.send(json.dumps(payload))
            except Exception:
                pass

    async def start(self) -> None:
        if IS_WINDOWS:
            self.proc = await asyncio.create_subprocess_exec(
                "cmd.exe", "/Q", "/V:ON",
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
        else:
            self.proc = await asyncio.create_subprocess_exec(
                "/bin/bash",
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                start_new_session=True,
            )
        self.pump_task = asyncio.create_task(self._pump())
        await self._prime()

    async def _prime(self) -> None:
        marker = f"__RELAY_PRIME_{uuid.uuid4().hex}__"
        payload = f"echo {marker}\r\n" if IS_WINDOWS else f"printf '{marker}\\n'\n"
        event = asyncio.Event()
        self.current = {"id": "__prime__", "marker": marker, "event": event, "capture": []}
        assert self.proc and self.proc.stdin
        self.proc.stdin.write(payload.encode())
        await self.proc.stdin.drain()
        await event.wait()

    async def _emit(self, data: str, stream: str = "stdout") -> None:
        cur = self.current
        if cur is None:
            return
        if cur.get("capture") is not None:
            cur["capture"].append(data)
        else:
            await self._send({
                "type": "output",
                "id": cur["id"],
                "stream": stream,
                "data": data,
            })

    async def _pump(self) -> None:
        assert self.proc is not None

        async def stderr_pump() -> None:
            while True:
                try:
                    chunk = await self.proc.stderr.read(4096)
                except Exception:
                    return
                if not chunk:
                    return
                if self.current:
                    await self._emit(chunk.decode("utf-8", errors="replace"), "stderr")

        stderr_task = asyncio.create_task(stderr_pump())

        buf = ""
        try:
            while True:
                chunk = await self.proc.stdout.read(4096)
                if not chunk:
                    break
                buf += chunk.decode("utf-8", errors="replace")

                if self.current is None:
                    buf = ""
                    continue

                while self.current is not None:
                    marker = self.current["marker"]
                    idx = buf.find(marker)

                    if idx == -1:
                        keep = len(marker) - 1
                        if len(buf) > keep:
                            out, buf = buf[:len(buf) - keep], buf[len(buf) - keep:]
                            if out:
                                await self._emit(out)
                        break

                    out = buf[:idx]
                    if out:
                        await self._emit(out)

                    rest = buf[idx + len(marker):]
                    nl = rest.find("\n")
                    if nl == -1:
                        break

                    code_str = rest[:nl].strip()
                    try:
                        code = int(code_str)
                    except ValueError:
                        code = -1

                    cur = self.current
                    self.current = None
                    cur["code"] = code
                    if cur.get("capture") is None:
                        await self._send({"type": "exit", "id": cur["id"], "code": code})
                    cur["event"].set()
                    buf = rest[nl + 1:]
        finally:
            stderr_task.cancel()
            try:
                await stderr_task
            except (asyncio.CancelledError, Exception):
                pass

    async def run(self, cmd_id: str, cmd: str) -> None:
        async with self.lock:
            if self.proc is None or self.proc.returncode is not None:
                await self.start()

            marker = f"__RELAY_DONE_{uuid.uuid4().hex}__"
            event = asyncio.Event()
            self.current = {"id": cmd_id, "marker": marker, "event": event}

            if IS_WINDOWS:
                oneline = cmd.replace("\r\n", "\n").replace("\n", " & ")
                payload = f"{oneline} & echo {marker}!errorlevel!\r\n"
            else:
                payload = f"{cmd}\nprintf '\\n{marker}%d\\n' $?\n"

            try:
                assert self.proc and self.proc.stdin
                self.proc.stdin.write(payload.encode("utf-8"))
                await self.proc.stdin.drain()
            except (BrokenPipeError, ConnectionResetError):
                await self._send({
                    "type": "output", "id": cmd_id,
                    "stream": "stderr", "data": "shell died, restarting\n",
                })
                await self._send({"type": "exit", "id": cmd_id, "code": 1})
                self.proc = None
                self.current = None
                return

            await event.wait()

    async def capture(self, cmd: str) -> tuple[str, int]:
        """Run a command silently and return (stdout+stderr, exit code)."""
        async with self.lock:
            if self.proc is None or self.proc.returncode is not None:
                await self.start()

            marker = f"__RELAY_CAP_{uuid.uuid4().hex}__"
            event = asyncio.Event()
            buf: list[str] = []
            cur_ref: dict = {"id": "__capture__", "marker": marker, "event": event, "capture": buf}
            self.current = cur_ref

            if IS_WINDOWS:
                oneline = cmd.replace("\r\n", "\n").replace("\n", " & ")
                payload = f"{oneline} & echo {marker}!errorlevel!\r\n"
            else:
                payload = f"{cmd}\nprintf '\\n{marker}%d\\n' $?\n"

            try:
                assert self.proc and self.proc.stdin
                self.proc.stdin.write(payload.encode("utf-8"))
                await self.proc.stdin.drain()
            except Exception:
                self.current = None
                raise

            await event.wait()
            return "".join(buf), cur_ref.get("code", -1)

    async def get_cwd(self) -> str:
        if IS_WINDOWS:
            out, _ = await self.capture("cd")
        else:
            out, _ = await self.capture("pwd")
        lines = [ln for ln in out.splitlines() if ln.strip()]
        return lines[-1].strip() if lines else os.getcwd()

    async def stop(self) -> None:
        if self.proc and self.proc.returncode is None:
            try:
                self.proc.terminate()
            except ProcessLookupError:
                pass
            try:
                await asyncio.wait_for(self.proc.wait(), timeout=3)
            except asyncio.TimeoutError:
                try:
                    self.proc.kill()
                except ProcessLookupError:
                    pass
        if self.pump_task:
            self.pump_task.cancel()
            try:
                await self.pump_task
            except (asyncio.CancelledError, Exception):
                pass


# ---------------------------------------------------------------- fs handlers

async def _send_msg(ws, send_lock: asyncio.Lock, payload: dict) -> None:
    async with send_lock:
        try:
            await ws.send(json.dumps(payload))
        except Exception:
            pass


async def handle_fs_cwd(shell: PersistentShell, ws, send_lock, m: dict) -> None:
    try:
        cwd = await shell.get_cwd()
        await _send_msg(ws, send_lock, {"type": "fs_cwd_result", "id": m["id"], "cwd": cwd})
    except Exception as e:
        await _send_msg(ws, send_lock, {"type": "fs_cwd_result", "id": m["id"], "error": str(e)})


async def handle_fs_list(ws, send_lock, m: dict) -> None:
    path = m.get("path", "/")
    try:
        entries = []
        with os.scandir(path) as it:
            for e in it:
                try:
                    is_dir = e.is_dir()
                    size = 0 if is_dir else e.stat().st_size
                except OSError:
                    is_dir, size = False, 0
                entries.append({
                    "name": e.name,
                    "path": os.path.join(path, e.name),
                    "type": "dir" if is_dir else "file",
                    "size": size,
                })
        entries.sort(key=lambda x: (x["type"] != "dir", x["name"].lower()))
        await _send_msg(ws, send_lock, {
            "type": "fs_list_result", "id": m["id"], "path": path, "entries": entries,
        })
    except OSError as e:
        await _send_msg(ws, send_lock, {
            "type": "fs_list_result", "id": m["id"], "path": path, "error": str(e), "entries": [],
        })


async def handle_fs_get(ws, send_lock, m: dict) -> None:
    path = m.get("path", "")
    try:
        st = os.stat(path)
        if st.st_size > MAX_TRANSFER:
            raise OSError(f"file too large ({st.st_size} bytes > {MAX_TRANSFER})")
        with open(path, "rb") as f:
            data = f.read()
        b64 = base64.b64encode(data).decode("ascii")
        await _send_msg(ws, send_lock, {
            "type": "fs_get_result", "id": m["id"], "path": path, "data": b64, "size": len(data),
        })
    except OSError as e:
        await _send_msg(ws, send_lock, {
            "type": "fs_get_result", "id": m["id"], "path": path, "error": str(e),
        })


async def handle_fs_put(ws, send_lock, m: dict) -> None:
    path = m.get("path", "")
    try:
        data = base64.b64decode(m.get("data", ""))
        if len(data) > MAX_TRANSFER:
            raise OSError(f"payload too large ({len(data)} bytes > {MAX_TRANSFER})")
        parent = os.path.dirname(path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        with open(path, "wb") as f:
            f.write(data)
        await _send_msg(ws, send_lock, {
            "type": "fs_put_result", "id": m["id"], "path": path, "ok": True, "size": len(data),
        })
    except Exception as e:
        await _send_msg(ws, send_lock, {
            "type": "fs_put_result", "id": m["id"], "path": path, "ok": False, "error": str(e),
        })


# ---------------------------------------------------------------- session

async def session(url: str) -> None:
    global _delay

    async with websockets.connect(url, ping_interval=20, ping_timeout=20, max_size=None) as ws:
        _delay = RETRY_MIN
        log(f"connected to {url}")

        send_lock = asyncio.Lock()
        async with send_lock:
            await ws.send(json.dumps({
                "type": "hello",
                "host": socket.gethostname(),
                "cwd": os.getcwd(),
                "shell": "cmd" if IS_WINDOWS else "bash",
            }))

        shell = PersistentShell(ws, send_lock)
        await shell.start()

        try:
            async for raw in ws:
                try:
                    m = json.loads(raw)
                except json.JSONDecodeError:
                    continue
                t = m.get("type")
                if t == "command":
                    log(f"exec: {m.get('cmd','')}")
                    asyncio.create_task(shell.run(m["id"], m.get("cmd") or ""))
                elif t == "fs_cwd":
                    asyncio.create_task(handle_fs_cwd(shell, ws, send_lock, m))
                elif t == "fs_list":
                    asyncio.create_task(handle_fs_list(ws, send_lock, m))
                elif t == "fs_get":
                    asyncio.create_task(handle_fs_get(ws, send_lock, m))
                elif t == "fs_put":
                    asyncio.create_task(handle_fs_put(ws, send_lock, m))
        finally:
            await shell.stop()


# ---------------------------------------------------------------- main loop

async def main() -> None:
    global _delay
    while True:
        url = read_host_url()
        if url is None:
            log(f"retrying config read in {_delay:.1f}s")
            await asyncio.sleep(_delay)
            _delay = min(_delay * 2, RETRY_MAX)
            continue

        ws_url = to_ws_url(url)
        try:
            await session(ws_url)
            log("connection closed")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log(f"{type(e).__name__}: {e}")

        log(f"reconnecting in {_delay:.1f}s")
        await asyncio.sleep(_delay)
        _delay = min(_delay * 2, RETRY_MAX)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log("bye")