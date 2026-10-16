from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

HERE = Path(__file__).resolve().parent
CREDENTIALS = HERE / "credentials.json"
ENV_FILE = HERE / ".env"

KV_BASE = "https://ignite-aura-info.chrono-aero.workers.dev"
NGROK_API = "http://127.0.0.1:4040/api/tunnels"

IS_WINDOWS = sys.platform == "win32"


def banner(msg: str) -> None:
    print(f"\n[setup] {msg}")


def _spawn(cmd: list[str]) -> subprocess.Popen:
    if IS_WINDOWS:
        return subprocess.Popen(cmd, creationflags=subprocess.CREATE_NEW_PROCESS_GROUP)
    return subprocess.Popen(cmd, start_new_session=True)


def _kill_tree(proc: subprocess.Popen | None) -> None:
    if proc is None or proc.poll() is not None:
        return
    if IS_WINDOWS:
        subprocess.run(
            ["taskkill", "/F", "/T", "/PID", str(proc.pid)],
            capture_output=True,
        )
    else:
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
        except (ProcessLookupError, PermissionError):
            try:
                proc.terminate()
            except Exception:
                pass


# ---------------------------------------------------------------- .env I/O

def env_read() -> dict[str, str]:
    """Read .env as a dict. Missing file → empty dict."""
    out: dict[str, str] = {}
    if not ENV_FILE.exists():
        return out
    for line in ENV_FILE.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        out[k.strip()] = v.strip()
    return out


def env_set(key: str, value: str) -> None:
    """Set a key in .env, preserving other lines and comments."""
    lines: list[str] = []
    replaced = False
    if ENV_FILE.exists():
        for line in ENV_FILE.read_text(encoding="utf-8").splitlines():
            stripped = line.strip()
            if stripped.startswith(f"{key}="):
                lines.append(f"{key}={value}")
                replaced = True
            else:
                lines.append(line)
    if not replaced:
        if lines and lines[-1].strip():
            lines.append("")
        lines.append(f"{key}={value}")
    ENV_FILE.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")


# ---------------------------------------------------------------- credentials

def ensure_credentials() -> str:
    if CREDENTIALS.exists():
        try:
            data = json.loads(CREDENTIALS.read_text(encoding="utf-8"))
            username = (data.get("username") or "").strip()
            if username:
                banner(f"credentials.json — username: {username}")
                return username
        except (json.JSONDecodeError, OSError):
            pass

    banner("credentials.json missing or invalid")
    try:
        username = input("Enter your ITSC username: ").strip()
    except (EOFError, KeyboardInterrupt):
        sys.exit("\naborted")
    if not username:
        sys.exit("no username provided")

    CREDENTIALS.write_text(
        json.dumps({"username": username}, indent=2) + "\n",
        encoding="utf-8",
    )
    print(f"[setup] wrote {CREDENTIALS.name}")
    return username


# ---------------------------------------------------------------- port

def ensure_port() -> int:
    """.env is the ground truth. Prompt and persist if unknown."""
    raw = env_read().get("PORT", "").strip()

    if raw:
        try:
            port = int(raw)
            if 1 <= port <= 65535:
                banner(f".env — PORT={port}")
                return port
        except ValueError:
            pass
        banner(f".env has invalid PORT ({raw!r}) — asking again")

    while True:
        try:
            entered = input(f"Enter the port for the terminal (e.g. 8081 for the terminal to be on localhost:8081): ").strip()
        except (EOFError, KeyboardInterrupt):
            sys.exit("\naborted")

        if True:
            try:
                port = int(entered)
            except ValueError:
                print("[setup] please enter a number between 1 and 65535")
                continue
            if not (1 <= port <= 65535):
                print("[setup] port must be 1-65535")
                continue
        break

    env_set("PORT", str(port))
    print(f"[setup] wrote PORT={port} to .env")
    return port


# ---------------------------------------------------------------- ngrok

def start_ngrok(port: int) -> tuple[subprocess.Popen, str]:
    banner(f"starting ngrok http {port}")
    try:
        proc = _spawn(["ngrok", "http", str(port)])
    except FileNotFoundError:
        sys.exit("`ngrok` not found in PATH — install it first: https://ngrok.com/download")

    for _ in range(60):  # up to 30s
        time.sleep(0.5)
        if proc.poll() is not None:
            sys.exit(f"ngrok exited early (code {proc.returncode}) — check its output above")
        try:
            with urllib.request.urlopen(NGROK_API, timeout=1) as r:
                data = json.loads(r.read())
            for t in data.get("tunnels", []):
                if t.get("proto") == "https" and t.get("public_url"):
                    return proc, t["public_url"]
        except (urllib.error.URLError, json.JSONDecodeError, TimeoutError):
            continue

    _kill_tree(proc)
    sys.exit("ngrok did not report a tunnel within 30s")


# ---------------------------------------------------------------- KV

def register_url(username: str, ngrok_url: str) -> None:
    banner("registering tunnel with ignite-aura-info")
    print(f"[setup]   username: {username}")
    print(f"[setup]   url:      {ngrok_url}")

    # Worker double-decodes: searchParams.get + decodeURIComponent.
    encoded_url = urllib.parse.quote(urllib.parse.quote(ngrok_url, safe=""), safe="")
    encoded_user = urllib.parse.quote(username, safe="")
    url = f"{KV_BASE}/?username={encoded_user}&url={encoded_url}"

    req = urllib.request.Request(
        url,
        method="POST",
        headers={"User-Agent": "ws-relay-setup/0.1"},
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            body = r.read().decode("utf-8", errors="replace").strip()
    except urllib.error.HTTPError as e:
        detail = e.read().decode(errors="replace")
        sys.exit(f"KV store rejected the request: HTTP {e.code} — {detail}")
    except Exception as e:
        sys.exit(f"failed to reach KV store: {type(e).__name__}: {e}")

    print(f"[setup]   response: {body}")
    if "Successful" not in body:
        print("[setup] warning: response did not contain 'Successful' — double-check the username")


# ---------------------------------------------------------------- server

def start_server() -> subprocess.Popen:
    banner("starting server (reads .env via python-dotenv)")
    try:
        return _spawn(["uv", "run", "server.py"])
    except FileNotFoundError:
        sys.exit("`uv` not found in PATH — install it first: https://docs.astral.sh/uv/")


# ---------------------------------------------------------------- shutdown

def shutdown(procs: list[subprocess.Popen | None]) -> None:
    for p in procs:
        _kill_tree(p)
    for p in procs:
        if not p:
            continue
        try:
            p.wait(timeout=5)
        except subprocess.TimeoutExpired:
            try:
                p.kill()
            except Exception:
                pass


# ---------------------------------------------------------------- main

def main() -> None:
    print("ws-relay host setup")
    print("=" * 40)

    username = ensure_credentials()
    port = ensure_port()

    ngrok_proc: subprocess.Popen | None = None
    server_proc: subprocess.Popen | None = None

    try:
        ngrok_proc, ngrok_url = start_ngrok(port)
        register_url(username, ngrok_url)
        server_proc = start_server()

        print()
        print("=" * 40)
        print(f"  UI:       {ngrok_url}/")
        print(f"  tunnel:   {ngrok_url}")
        print(f"  username: {username}")
        print(f"  port:     {port}")
        print("=" * 40)
        print("  Ctrl+C to stop")
        print()

        server_proc.wait()
    except KeyboardInterrupt:
        print("\n[setup] interrupted")
    finally:
        shutdown([server_proc, ngrok_proc])


if __name__ == "__main__":
    main()