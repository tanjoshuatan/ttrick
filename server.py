from __future__ import annotations

import asyncio
import base64
import json
import logging
import uuid
from pathlib import Path

from aiohttp import WSMsgType, web

HERE = Path(__file__).resolve().parent
INDEX = HERE / "assets/index.html"

HOP_BY_HOP = {
    "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
    "te", "trailer", "transfer-encoding", "upgrade",
}

MAX_BODY = 32 * 1024 * 1024
SID_GRACE_SECONDS = 60.0

log = logging.getLogger("host")

sessions: dict[str, "Session"] = {}
agent_ws: web.WebSocketResponse | None = None
agent_hello: dict = {}
tunnels: dict[str, "Tunnel"] = {}


class Session:
    __slots__ = ("sid", "uis", "close_task")

    def __init__(self, sid: str):
        self.sid = sid
        self.uis: set[web.WebSocketResponse] = set()
        self.close_task: asyncio.Task | None = None


class Tunnel:
    __slots__ = ("id", "kind", "ready", "status", "headers", "error", "body_queue")

    def __init__(self, tid: str, kind: str):
        self.id = tid
        self.kind = kind
        self.ready = asyncio.Event()
        self.status: int | None = None
        self.headers: dict | None = None
        self.error: str | None = None
        self.body_queue: asyncio.Queue = asyncio.Queue()


async def _send(ws: web.WebSocketResponse, payload: dict) -> None:
    try:
        await ws.send_str(json.dumps(payload))
    except Exception:
        pass


async def _broadcast_session(sess: Session, payload: dict) -> None:
    if not sess.uis:
        return
    data = json.dumps(payload)
    await asyncio.gather(*(ws.send_str(data) for ws in list(sess.uis)), return_exceptions=True)


async def _broadcast_all_uis(payload: dict) -> None:
    all_uis: list[web.WebSocketResponse] = []
    for sess in sessions.values():
        all_uis.extend(sess.uis)
    if not all_uis:
        return
    data = json.dumps(payload)
    await asyncio.gather(*(ws.send_str(data) for ws in all_uis), return_exceptions=True)


async def push_status() -> None:
    await _broadcast_all_uis({"type": "status", "agent": "connected" if agent_ws else "disconnected"})


async def _close_session_after(sid: str) -> None:
    await asyncio.sleep(SID_GRACE_SECONDS)
    sess = sessions.get(sid)
    if sess is None or sess.uis:
        return
    sessions.pop(sid, None)
    if agent_ws is not None:
        await _send(agent_ws, {"type": "session_close", "sid": sid})


# ---------------------------------------------------------------- static

async def handle_index(request: web.Request) -> web.StreamResponse:
    resp = web.FileResponse(INDEX)
    resp.headers["Cache-Control"] = "no-store"
    return resp


# ---------------------------------------------------------------- tunnels

async def handle_tunnel_root(request: web.Request) -> web.StreamResponse:
    port = request.match_info["port"]
    if not port.isdigit():
        return web.Response(status=400, text="bad port")
    raise web.HTTPPermanentRedirect(f"/t/{port}/")


async def handle_tunnel(request: web.Request) -> web.StreamResponse:
    port = request.match_info["port"]
    if not port.isdigit() or not (1 <= int(port) <= 65535):
        return web.Response(status=400, text="bad port")

    rel = request.match_info.get("path", "")
    path = "/" + rel

    if agent_ws is None:
        return web.Response(status=502, text="no client connected")

    is_ws = request.headers.get("Upgrade", "").lower() == "websocket"
    if is_ws:
        return await _tunnel_ws(request, agent_ws, port, path)
    return await _tunnel_http(request, agent_ws, port, path)


async def _tunnel_http(request: web.Request, agent, port: str, path: str) -> web.StreamResponse:
    tid = uuid.uuid4().hex

    body = b""
    if request.can_read_body:
        body = await request.read()
        if len(body) > MAX_BODY:
            return web.Response(status=413, text="body too large")

    forward = {
        k: v for k, v in request.headers.items()
        if k.lower() not in HOP_BY_HOP and k.lower() not in ("host", "content-length")
    }

    tunnel = Tunnel(tid, "http")
    tunnels[tid] = tunnel

    try:
        await _send(agent, {
            "type": "tunnel_open", "id": tid, "kind": "http",
            "port": port, "method": request.method, "path": path,
            "query": request.query_string, "headers": forward,
            "body_b64": base64.b64encode(body).decode() if body else "",
        })

        try:
            await asyncio.wait_for(tunnel.ready.wait(), timeout=30)
        except asyncio.TimeoutError:
            return web.Response(status=504, text="upstream timeout")

        if tunnel.error:
            return web.Response(status=502, text=tunnel.error)

        resp_headers = {
            k: v for k, v in (tunnel.headers or {}).items()
            if k.lower() not in HOP_BY_HOP
        }
        stream = web.StreamResponse(status=tunnel.status or 502, headers=resp_headers)
        await stream.prepare(request)

        while True:
            chunk = await tunnel.body_queue.get()
            if chunk is None:
                break
            try:
                await stream.write(chunk)
            except (ConnectionResetError, RuntimeError):
                break

        try:
            await stream.write_eof()
        except Exception:
            pass
        return stream
    finally:
        tunnels.pop(tid, None)
        try:
            await _send(agent, {"type": "tunnel_close", "id": tid})
        except Exception:
            pass


async def _tunnel_ws(request: web.Request, agent, port: str, path: str) -> web.StreamResponse:
    tid = uuid.uuid4().hex
    tunnel = Tunnel(tid, "ws")
    tunnels[tid] = tunnel

    drop = {
        "host", "connection", "upgrade",
        "sec-websocket-key", "sec-websocket-version", "sec-websocket-extensions",
    }
    forward = {
        k: v for k, v in request.headers.items()
        if k.lower() not in HOP_BY_HOP and k.lower() not in drop
    }

    try:
        await _send(agent, {
            "type": "tunnel_open", "id": tid, "kind": "ws",
            "port": port, "path": path, "query": request.query_string,
            "headers": forward,
        })

        try:
            await asyncio.wait_for(tunnel.ready.wait(), timeout=30)
        except asyncio.TimeoutError:
            return web.Response(status=504, text="upstream timeout")

        if tunnel.error:
            return web.Response(status=502, text=tunnel.error)

        ws_browser = web.WebSocketResponse(heartbeat=30, max_msg_size=0)
        await ws_browser.prepare(request)

        async def b2u() -> None:
            async for msg in ws_browser:
                if msg.type == WSMsgType.TEXT:
                    await _send(agent, {
                        "type": "tunnel_ws_data", "id": tid,
                        "data": msg.data, "is_text": True,
                    })
                elif msg.type == WSMsgType.BINARY:
                    await _send(agent, {
                        "type": "tunnel_ws_data", "id": tid,
                        "data": base64.b64encode(msg.data).decode(), "is_text": False,
                    })

        async def u2b() -> None:
            while True:
                item = await tunnel.body_queue.get()
                if item is None:
                    break
                is_text, data = item
                try:
                    if is_text:
                        await ws_browser.send_str(data)
                    else:
                        await ws_browser.send_bytes(base64.b64decode(data))
                except (ConnectionResetError, RuntimeError):
                    break

        try:
            await asyncio.gather(b2u(), u2b())
        except Exception:
            pass
        return ws_browser
    finally:
        tunnels.pop(tid, None)
        try:
            await _send(agent, {"type": "tunnel_close", "id": tid})
        except Exception:
            pass


# ---------------------------------------------------------------- ws

async def handle_ws(request: web.Request) -> web.WebSocketResponse:
    global agent_ws
    ws = web.WebSocketResponse(heartbeat=30, max_msg_size=0)
    await ws.prepare(request)

    role = request.query.get("role", "ui")
    peer = request.remote
    log.info("%s connected from %s", role, peer)

    # -------- client --------
    if role == "client":
        old = agent_ws
        agent_ws = ws
        if old is not None and old is not ws:
            try:
                await old.close()
            except Exception:
                pass
        await push_status()

        try:
            async for msg in ws:
                if msg.type is not WSMsgType.TEXT:
                    continue
                try:
                    m = json.loads(msg.data)
                except json.JSONDecodeError:
                    continue

                t = m.get("type", "")

                if t == "hello":
                    agent_hello.clear()
                    agent_hello.update(m)
                    await _broadcast_all_uis({
                        "type": "hello",
                        "host": m.get("host"),
                    })
                    # (Re)open all live sessions on this client
                    for sid in list(sessions.keys()):
                        await _send(ws, {"type": "session_open", "sid": sid})
                    continue

                if t.startswith("tunnel_"):
                    tunnel = tunnels.get(m.get("id"))
                    if not tunnel:
                        continue
                    if t == "tunnel_response":
                        tunnel.status = m.get("status")
                        tunnel.headers = m.get("headers") or {}
                        tunnel.ready.set()
                    elif t == "tunnel_ready":
                        tunnel.ready.set()
                    elif t == "tunnel_data":
                        try:
                            await tunnel.body_queue.put(base64.b64decode(m["data_b64"]))
                        except Exception:
                            pass
                    elif t == "tunnel_ws_data":
                        await tunnel.body_queue.put((m.get("is_text", True), m.get("data", "")))
                    elif t == "tunnel_end":
                        await tunnel.body_queue.put(None)
                    elif t == "tunnel_error":
                        tunnel.error = m.get("error", "unknown error")
                        tunnel.ready.set()
                        await tunnel.body_queue.put(None)
                    continue

                sid = m.get("sid")
                if not sid:
                    continue
                sess = sessions.get(sid)
                if sess is None:
                    continue
                await _broadcast_session(sess, m)
        finally:
            if agent_ws is ws:
                agent_ws = None
                agent_hello.clear()
                log.info("client disconnected (%s)", peer)
                await push_status()
        return ws

    # -------- ui --------
    sid = request.query.get("sid") or uuid.uuid4().hex
    sess = sessions.get(sid)
    is_new = sess is None
    if sess is None:
        sess = Session(sid)
        sessions[sid] = sess
    if sess.close_task is not None:
        sess.close_task.cancel()
        sess.close_task = None
    sess.uis.add(ws)

    try:
        await _send(ws, {"type": "sid", "sid": sid})
        await _send(ws, {"type": "status", "agent": "connected" if agent_ws else "disconnected"})
        if agent_hello:
            await _send(ws, {"type": "hello", "host": agent_hello.get("host")})

        if is_new and agent_ws is not None:
            await _send(agent_ws, {"type": "session_open", "sid": sid})

        async for msg in ws:
            if msg.type is not WSMsgType.TEXT:
                continue
            try:
                m = json.loads(msg.data)
            except json.JSONDecodeError:
                continue

            t = m.get("type", "")
            if agent_ws is None:
                if t == "command":
                    await _send(ws, {"type": "exit", "id": m.get("id"), "code": 127})
                elif t.startswith("fs_"):
                    rt = {
                        "fs_cwd": "fs_cwd_result",
                        "fs_list": "fs_list_result",
                        "fs_get": "fs_get_result",
                        "fs_put": "fs_put_result",
                    }.get(t)
                    if rt:
                        await _send(ws, {"type": rt, "id": m.get("id"), "error": "client offline"})
                continue

            m2 = dict(m)
            m2["sid"] = sid
            await _send(agent_ws, m2)
    finally:
        sess.uis.discard(ws)
        if not sess.uis:
            sess.close_task = asyncio.create_task(_close_session_after(sid))
        log.info("ui disconnected (%s, sid=%s)", peer, sid)
    return ws


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-7s %(message)s",
        datefmt="%H:%M:%S",
    )
    app = web.Application(client_max_size=MAX_BODY)
    app.router.add_get("/", handle_index)
    app.router.add_get("/ws", handle_ws)
    app.router.add_get("/healthz", lambda _: web.json_response({"ok": True, "agent": agent_ws is not None, "sessions": len(sessions)}))
    app.router.add_get("/t/{port}", handle_tunnel_root)
    app.router.add_route("*", "/t/{port}/{path:.*}", handle_tunnel)
    log.info("host listening on http://0.0.0.0:8080")
    web.run_app(app, host="0.0.0.0", port=8080, print=None)


if __name__ == "__main__":
    main()