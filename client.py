from __future__ import annotations

import argparse
import asyncio
import base64
import json
import os
import platform
import socket
import subprocess
import urllib.parse
import uuid
from pathlib import Path

import aiohttp
import websockets

IS_WINDOWS = platform.system() == "Windows"

if not IS_WINDOWS:
    import fcntl
    import pty
    import struct
    import termios

HERE = Path(__file__).resolve().parent
DEFAULT_HOST_FILE = HERE / "host.txt"
HOST_FILE: Path = DEFAULT_HOST_FILE

RETRY_DELAY = 10.0
HEARTBEAT_INTERVAL = 20.0
MAX_TRANSFER = 32 * 1024 * 1024


def log(msg: str) -> None:
    print(f"[client] {msg}", flush=True)


# ---------------------------------------------------------------- config

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(prog="client.py")
    p.add_argument(
        "host_file",
        nargs="?",
        default=str(DEFAULT_HOST_FILE),
        help=f"file containing the KV store base URL (default: {DEFAULT_HOST_FILE})",
    )
    p.add_argument(
        "-u", "--username",
        required=True,
        help="username to look up in the KV store",
    )
    return p.parse_args()


def read_host_url() -> str | None:
    try:
        text = HOST_FILE.read_text(encoding="utf-8")
    except (FileNotFoundError, OSError):
        return None
    for line in text.splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            return line
    return None


async def resolve_target(kv_base: str, username: str) -> str | None:
    base = kv_base.rstrip("/")
    url = f"{base}/?username={urllib.parse.quote(username)}"
    try:
        timeout = aiohttp.ClientTimeout(total=15)
        async with aiohttp.ClientSession(timeout=timeout) as sess:
            async with sess.get(url) as resp:
                if resp.status != 200:
                    return None
                text = (await resp.text()).strip()
                return text or None
    except Exception:
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


# ---------------------------------------------------------------- pty helper

if not IS_WINDOWS:
    class _PipeEndpoint:
        __slots__ = ("_fd",)
        def __init__(self, fd): self._fd = fd
        def fileno(self): return self._fd
        def close(self):
            try: os.close(self._fd)
            except OSError: pass


# ---------------------------------------------------------------- shell

class PersistentShell:
    """One bash per session (sid), persistent, with pty so Ctrl+C works."""

    def __init__(self, ws, send_lock, sid, username):
        self.ws = ws
        self.send_lock = send_lock
        self.sid = sid
        self.username = username
        self.proc = None
        self.master_fd = None
        self.reader = None
        self.transport = None  # <-- Keep reference to prevent GC
        self.pump_task = None
        self.stderr_task = None
        self.lock = asyncio.Lock()
        self.current = None

    async def _send(self, payload):
        p = dict(payload); p["sid"] = self.sid
        async with self.send_lock:
            try:
                await self.ws.send(json.dumps(p))
            except Exception:
                pass

    def _proc_dead(self):
        if self.proc is None:
            return True
        if IS_WINDOWS:
            return self.proc.returncode is not None
        return self.proc.poll() is not None

    async def start(self):
        # Clean up any previous transport/tasks
        if self.pump_task:
            self.pump_task.cancel()
            try:
                await self.pump_task
            except (asyncio.CancelledError, Exception):
                pass
            self.pump_task = None
            
        if self.transport:
            self.transport.close()
            self.transport = None

        if IS_WINDOWS:
            await self._start_windows()
        else:
            await self._start_posix()
            
        self.pump_task = asyncio.create_task(self._pump())
        await self._prime()

    async def _start_windows(self):
        self.proc = await asyncio.create_subprocess_exec(
            "cmd.exe", "/Q", "/V:ON",
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        self.reader = self.proc.stdout

    async def _start_posix(self):
        master, slave = pty.openpty()
        
        # Set master to non-blocking
        flags = fcntl.fcntl(master, fcntl.F_GETFL)
        fcntl.fcntl(master, fcntl.F_SETFL, flags | os.O_NONBLOCK)
        
        try:
            attrs = termios.tcgetattr(slave)
            attrs[3] &= ~(termios.ECHO | termios.ECHONL)
            attrs[3] |= termios.NOFLSH
            attrs[1] &= ~termios.ONLCR
            termios.tcsetattr(slave, termios.TCSANOW, attrs)
            fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack("HHHH", 30, 120, 0, 0))
        except Exception as e:
            log(f"pty termios warning: {e}")

        env = os.environ.copy()
        env["TERM"] = "xterm"  # Changed from "dumb" just in case
        env["PS1"] = ""
        env["PS2"] = ""
        env["PROMPT_COMMAND"] = ""
        env["PYTHONUNBUFFERED"] = "1"
        
        # Explicitly set user variables
        env["USER"] = self.username
        env["LOGNAME"] = self.username
        env["USERNAME"] = self.username
        env["HOME"] = f"/project/fyp26_lz1/{self.username}"

        def _child_setup():
            os.setsid()
            try:
                fcntl.ioctl(0, termios.TIOCSCTTY, 0)
            except OSError:
                pass

        rc_file = HERE / ".bashrc"
        cmd = ["/bin/bash", "--noediting", "--rcfile", str(rc_file), "-i"]

        # CRITICAL: Capture stderr so we can see bash's crash message!
        self.proc = subprocess.Popen(
            cmd,
            stdin=slave, stdout=slave, stderr=subprocess.PIPE,
            env=env, preexec_fn=_child_setup, close_fds=True,
        )
        os.close(slave)
        self.master_fd = os.dup(master)

        # Background task to read bash's errors
        async def read_stderr():
            while True:
                try:
                    chunk = await self.proc.stderr.read(1024)
                except Exception:
                    break
                if not chunk:
                    break
                log(f"BASH CRASH: {chunk.decode('utf-8', errors='replace').strip()}")

        asyncio.create_task(read_stderr())

        loop = asyncio.get_event_loop()
        reader = asyncio.StreamReader()
        protocol = asyncio.StreamReaderProtocol(reader)
        self.transport, _ = await loop.connect_read_pipe(lambda: protocol, _PipeEndpoint(master))
        self.reader = reader

    async def _prime(self):
        marker = f"__RELAY_PRIME_{uuid.uuid4().hex}__"
        event = asyncio.Event()
        self.current = {"id": "__prime__", "marker": marker, "event": event, "capture": []}
        
        # Just echo the marker. No `cd` command.
        if IS_WINDOWS:
            payload = f"echo {marker}\r\n"
        else:
            payload = f"printf '{marker}\\n'\n"
            
        await self._write(payload.encode())
        await event.wait()

    async def _write(self, data):
        if IS_WINDOWS:
            if not (self.proc and self.proc.stdin):
                raise BrokenPipeError("no stdin")
            self.proc.stdin.write(data)
            await self.proc.stdin.drain()
            return
        if self.master_fd is None:
            raise BrokenPipeError("no master fd")
        view = memoryview(data)
        while view:
            try:
                n = os.write(self.master_fd, view)
                if n == 0:
                    await asyncio.sleep(0.005)
                    continue
                view = view[n:]
            except BlockingIOError:
                await asyncio.sleep(0.005)
            except InterruptedError:
                continue

    async def _emit(self, data, stream="stdout"):
        cur = self.current
        if cur is None:
            return
        if cur.get("capture") is not None:
            cur["capture"].append(data)
        else:
            await self._send({
                "type": "output", "id": cur["id"], "stream": stream, "data": data,
            })

    async def _stderr_pump(self):
        if not (self.proc and getattr(self.proc, "stderr", None)):
            return
        while True:
            try:
                chunk = await self.proc.stderr.read(4096)
            except Exception:
                return
            if not chunk:
                return
            if self.current:
                await self._emit(chunk.decode("utf-8", errors="replace"), "stderr")

    async def _pump(self):
        if IS_WINDOWS:
            self.stderr_task = asyncio.create_task(self._stderr_pump())
        assert self.reader is not None
        buf = ""
        try:
            while True:
                try:
                    chunk = await self.reader.read(4096)
                except Exception as e:
                    log(f"pump read error: {e}")
                    break
                if not chunk:
                    log("pump read EOF (shell closed)")
                    break
                buf += chunk.decode("utf-8", errors="replace")
                if self.current is None:
                    buf = ""
                    continue

                while self.current is not None:
                    marker = self.current["marker"]
                    idx = buf.find(marker)
                    if idx == -1:
                        keep = 0
                        for k in range(min(len(buf), len(marker)), 0, -1):
                            if buf[-k:] == marker[:k]:
                                keep = k
                                break
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
            if self.stderr_task:
                self.stderr_task.cancel()
                try:
                    await self.stderr_task
                except (asyncio.CancelledError, Exception):
                    pass
                self.stderr_task = None
            cur = self.current
            if cur is not None and not cur["event"].is_set():
                cur["event"].set()
                if cur.get("capture") is None:
                    try:
                        await self._send({"type": "exit", "id": cur["id"], "code": -1})
                    except Exception:
                        pass

    def _build_payload(self, cmd, marker):
        if IS_WINDOWS:
            oneline = cmd.replace("\r\n", "\n").replace("\n", " & ")
            return f"{oneline} & echo {marker}!errorlevel!\r\n"
        return f"{cmd}\nprintf '\\n{marker}%d\\n' $?\n"

    async def run(self, cmd_id, cmd):
        async with self.lock:
            if self._proc_dead():
                await self.start()
            marker = f"__RELAY_DONE_{uuid.uuid4().hex}__"
            event = asyncio.Event()
            self.current = {"id": cmd_id, "marker": marker, "event": event}
            try:
                await self._write(self._build_payload(cmd, marker).encode("utf-8"))
            except (BrokenPipeError, ConnectionResetError, OSError):
                await self._send({
                    "type": "output", "id": cmd_id,
                    "stream": "stderr", "data": "shell died, restarting\n",
                })
                await self._send({"type": "exit", "id": cmd_id, "code": 1})
                self.proc = None
                self.current = None
                return
            
            # 2-hour timeout for very long commands (like model downloads)
            try:
                await asyncio.wait_for(event.wait(), timeout=7200)
            except asyncio.TimeoutError:
                await self._send({
                    "type": "output", "id": cmd_id,
                    "stream": "stderr", "data": "\n[terminal] command timed out after 2 hours\n",
                })
                await self._send({"type": "exit", "id": cmd_id, "code": -1})
                self.current = None
                await self.stop()
                self.proc = None

    async def capture(self, cmd):
        async with self.lock:
            if self._proc_dead():
                await self.start()
            marker = f"__RELAY_CAP_{uuid.uuid4().hex}__"
            event = asyncio.Event()
            buf: list[str] = []
            cur_ref = {"id": "__capture__", "marker": marker, "event": event, "capture": buf}
            self.current = cur_ref
            try:
                await self._write(self._build_payload(cmd, marker).encode("utf-8"))
            except Exception:
                self.current = None
                raise
            await event.wait()
            return "".join(buf), cur_ref.get("code", -1)

    async def get_cwd(self):
        out, _ = await self.capture("cd" if IS_WINDOWS else "pwd")
        lines = [ln for ln in out.splitlines() if ln.strip()]
        return lines[-1].strip() if lines else os.getcwd()

    async def interrupt(self, cmd_id):
        cur = self.current
        if cur is None:
            return
        if cmd_id and cur["id"] != cmd_id:
            return
        if IS_WINDOWS:
            if self.proc and self.proc.returncode is None:
                try:
                    self.proc.terminate()
                except Exception:
                    pass
            return
        if self.master_fd is None:
            return
        try:
            os.write(self.master_fd, b"\x03")
        except OSError:
            pass

    async def stop(self):
        if self.proc is not None:
            if IS_WINDOWS:
                if self.proc.returncode is None:
                    try: self.proc.terminate()
                    except Exception: pass
                    try: await asyncio.wait_for(self.proc.wait(), timeout=3)
                    except asyncio.TimeoutError:
                        try: self.proc.kill()
                        except Exception: pass
            else:
                if self.proc.poll() is None:
                    try: self.proc.terminate()
                    except Exception: pass
                    try: self.proc.wait(timeout=3)
                    except subprocess.TimeoutExpired:
                        try: self.proc.kill()
                        except Exception: pass
                        
        if self.transport:
            self.transport.close()
            self.transport = None
            
        if self.master_fd is not None:
            try: os.close(self.master_fd)
            except OSError: pass
            self.master_fd = None
            
        if self.pump_task:
            self.pump_task.cancel()
            try:
                await self.pump_task
            except (asyncio.CancelledError, Exception):
                pass
            self.pump_task = None


# ---------------------------------------------------------------- helpers

async def _send_msg(ws, send_lock, payload, sid=None):
    p = dict(payload)
    if sid:
        p["sid"] = sid
    async with send_lock:
        try:
            await ws.send(json.dumps(p))
        except Exception:
            pass


async def _heartbeat_loop(ws, send_lock):
    try:
        while True:
            await asyncio.sleep(HEARTBEAT_INTERVAL)
            async with send_lock:
                await ws.send(json.dumps({"type": "ping"}))
    except asyncio.CancelledError:
        return
    except Exception:
        return


# ---------------------------------------------------------------- fs handlers

async def handle_fs_cwd(shell, ws, send_lock, m):
    sid = m.get("sid")
    try:
        cwd = await shell.get_cwd()
        await _send_msg(ws, send_lock, {"type": "fs_cwd_result", "id": m["id"], "cwd": cwd}, sid)
    except Exception as e:
        await _send_msg(ws, send_lock, {"type": "fs_cwd_result", "id": m["id"], "error": str(e)}, sid)


async def handle_fs_list(ws, send_lock, m):
    path = m.get("path", "/")
    sid = m.get("sid")
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
        }, sid)
    except OSError as e:
        await _send_msg(ws, send_lock, {
            "type": "fs_list_result", "id": m["id"], "path": path, "error": str(e), "entries": [],
        }, sid)


async def handle_fs_get(ws, send_lock, m):
    path = m.get("path", "")
    sid = m.get("sid")
    try:
        st = os.stat(path)
        if st.st_size > MAX_TRANSFER:
            raise OSError(f"file too large ({st.st_size} bytes > {MAX_TRANSFER})")
        with open(path, "rb") as f:
            data = f.read()
        b64 = base64.b64encode(data).decode("ascii")
        await _send_msg(ws, send_lock, {
            "type": "fs_get_result", "id": m["id"], "path": path, "data": b64, "size": len(data),
        }, sid)
    except OSError as e:
        await _send_msg(ws, send_lock, {
            "type": "fs_get_result", "id": m["id"], "path": path, "error": str(e),
        }, sid)


async def handle_fs_put(ws, send_lock, m):
    path = m.get("path", "")
    sid = m.get("sid")
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
        }, sid)
    except Exception as e:
        await _send_msg(ws, send_lock, {
            "type": "fs_put_result", "id": m["id"], "path": path, "ok": False, "error": str(e),
        }, sid)


# ---------------------------------------------------------------- tunnel handlers

DROP_FOR_UPSTREAM = {"host", "content-length"}
DROP_FOR_WS_UPSTREAM = {
    "host", "connection", "upgrade",
    "sec-websocket-key", "sec-websocket-version", "sec-websocket-extensions",
}
HOP_BY_HOP_LOCAL = {
    "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
    "te", "trailer", "transfer-encoding", "upgrade",
}


async def handle_tunnel_open(ws, send_lock, m, active_tunnels):
    kind = m.get("kind")
    if kind == "http":
        await _do_http_tunnel(ws, send_lock, m)
    elif kind == "ws":
        await _do_ws_tunnel(ws, send_lock, m, active_tunnels)


async def _do_http_tunnel(ws, send_lock, m):
    tid = m["id"]
    port = m["port"]
    path = m.get("path") or "/"
    query = m.get("query", "")

    url = f"http://127.0.0.1:{port}{path}"
    if query:
        url += "?" + query

    headers = {k: v for k, v in (m.get("headers") or {}).items() if k.lower() not in DROP_FOR_UPSTREAM}
    body = base64.b64decode(m["body_b64"]) if m.get("body_b64") else None

    timeout = aiohttp.ClientTimeout(total=None, sock_connect=10)
    try:
        async with aiohttp.ClientSession(timeout=timeout) as sess:
            async with sess.request(
                m["method"], url, headers=headers, data=body,
                allow_redirects=False, auto_decompress=False,
            ) as resp:
                resp_headers = {k: v for k, v in resp.headers.items() if k.lower() not in HOP_BY_HOP_LOCAL}
                await _send_msg(ws, send_lock, {
                    "type": "tunnel_response", "id": tid,
                    "status": resp.status, "headers": resp_headers,
                })
                async for chunk in resp.content.iter_chunked(16384):
                    if not chunk:
                        continue
                    await _send_msg(ws, send_lock, {
                        "type": "tunnel_data", "id": tid,
                        "data_b64": base64.b64encode(chunk).decode(),
                    })
        await _send_msg(ws, send_lock, {"type": "tunnel_end", "id": tid})
    except Exception as e:
        await _send_msg(ws, send_lock, {
            "type": "tunnel_error", "id": tid, "error": f"{type(e).__name__}: {e}",
        })


async def _do_ws_tunnel(ws, send_lock, m, active_tunnels):
    tid = m["id"]
    port = m["port"]
    path = m.get("path") or "/"
    query = m.get("query", "")

    url = f"ws://127.0.0.1:{port}{path}"
    if query:
        url += "?" + query

    headers = {k: v for k, v in (m.get("headers") or {}).items() if k.lower() not in DROP_FOR_WS_UPSTREAM}

    q: asyncio.Queue = asyncio.Queue()
    active_tunnels[tid] = q

    try:
        async with websockets.connect(
            url, additional_headers=headers, max_size=None,
            ping_interval=None, ping_timeout=None,
        ) as upstream:
            await _send_msg(ws, send_lock, {"type": "tunnel_ready", "id": tid})

            async def b2u():
                while True:
                    msg = await q.get()
                    mt = msg.get("type")
                    if mt == "tunnel_close":
                        return
                    if mt == "tunnel_ws_data":
                        try:
                            if msg.get("is_text"):
                                await upstream.send(msg.get("data", ""))
                            else:
                                await upstream.send(base64.b64decode(msg.get("data", "")))
                        except websockets.ConnectionClosed:
                            return

            async def u2b():
                try:
                    async for msg in upstream:
                        if isinstance(msg, str):
                            await _send_msg(ws, send_lock, {
                                "type": "tunnel_ws_data", "id": tid,
                                "data": msg, "is_text": True,
                            })
                        else:
                            await _send_msg(ws, send_lock, {
                                "type": "tunnel_ws_data", "id": tid,
                                "data": base64.b64encode(msg).decode(), "is_text": False,
                            })
                except websockets.ConnectionClosed:
                    pass

            try:
                await asyncio.gather(b2u(), u2b())
            except Exception:
                pass
    except Exception as e:
        await _send_msg(ws, send_lock, {
            "type": "tunnel_error", "id": tid, "error": f"{type(e).__name__}: {e}",
        })
    finally:
        active_tunnels.pop(tid, None)


# ---------------------------------------------------------------- session

async def session(url: str, username: str) -> None:
    connected = False
    try:
        async with websockets.connect(
            url,
            ping_interval=None,
            ping_timeout=None,
            max_size=None,
            additional_headers={"ngrok-skip-browser-warning": "true"},
        ) as ws:
            connected = True
            log(f"connected to {url}")

            send_lock = asyncio.Lock()
            async with send_lock:
                await ws.send(json.dumps({
                    "type": "hello",
                    "host": socket.gethostname(),
                    "cwd": os.getcwd(),
                }))

            hb = asyncio.create_task(_heartbeat_loop(ws, send_lock))

            shells: dict[str, PersistentShell] = {}
            active_tunnels: dict[str, asyncio.Queue] = {}

            async def get_shell(sid):
                shell = shells.get(sid)
                if shell is None:
                    shell = PersistentShell(ws, send_lock, sid, username)
                    shells[sid] = shell
                    log(f"shell open: {sid[:8]}")
                    await shell.start()
                return shell

            try:
                async for raw in ws:
                    try:
                        m = json.loads(raw)
                    except json.JSONDecodeError:
                        continue
                    t = m.get("type")
                    sid = m.get("sid")

                    if t == "pong":
                        continue
                    if t == "tunnel_open":
                        log(f"tunnel: {m.get('kind')} :{m.get('port')}{m.get('path','')}")
                        asyncio.create_task(handle_tunnel_open(ws, send_lock, m, active_tunnels))
                    elif t in ("tunnel_ws_data", "tunnel_close"):
                        q = active_tunnels.get(m.get("id"))
                        if q:
                            await q.put(m)
                    elif t == "command":
                        if not sid:
                            continue
                        shell = await get_shell(sid)
                        log(f"exec[{sid[:8]}]: {m.get('cmd','')}")
                        asyncio.create_task(shell.run(m["id"], m.get("cmd") or ""))
                    elif t == "command_interrupt":
                        if not sid:
                            continue
                        shell = shells.get(sid)
                        if shell:
                            asyncio.create_task(shell.interrupt(m.get("id") or ""))
                    elif t == "fs_cwd":
                        if not sid:
                            continue
                        shell = await get_shell(sid)
                        asyncio.create_task(handle_fs_cwd(shell, ws, send_lock, m))
                    elif t == "fs_list":
                        asyncio.create_task(handle_fs_list(ws, send_lock, m))
                    elif t == "fs_get":
                        asyncio.create_task(handle_fs_get(ws, send_lock, m))
                    elif t == "fs_put":
                        asyncio.create_task(handle_fs_put(ws, send_lock, m))
            finally:
                hb.cancel()
                try:
                    await hb
                except (asyncio.CancelledError, Exception):
                    pass
                for q in active_tunnels.values():
                    await q.put({"type": "tunnel_close"})
                for shell in list(shells.values()):
                    await shell.stop()
                shells.clear()
    finally:
        if connected:
            log("Disconnecting... Attempt to reconnect?")


# ---------------------------------------------------------------- main

async def main() -> None:
    global HOST_FILE
    args = parse_args()
    HOST_FILE = Path(args.host_file).expanduser().resolve()
    username = args.username

    if not HOST_FILE.exists():
        log(f"warning: {HOST_FILE} does not exist yet — will keep retrying")

    while True:
        kv_base = read_host_url()
        if kv_base is None:
            await asyncio.sleep(RETRY_DELAY)
            continue

        target = await resolve_target(kv_base, username)
        if target is None:
            await asyncio.sleep(RETRY_DELAY)
            continue

        ws_url = to_ws_url(target)

        try:
            await session(ws_url, username)
        except asyncio.CancelledError:
            raise
        except Exception:
            pass

        await asyncio.sleep(RETRY_DELAY)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log("bye")