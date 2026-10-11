from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path

from aiohttp import WSMsgType, web

HERE = Path(__file__).resolve().parent
INDEX = HERE / "assets/index.html"

log = logging.getLogger("host")

uis: set[web.WebSocketResponse] = set()
agents: set[web.WebSocketResponse] = set()
agent_hello: dict = {}


async def _send(ws: web.WebSocketResponse, payload: dict) -> None:
    try:
        await ws.send_str(json.dumps(payload))
    except (ConnectionResetError, RuntimeError):
        pass


async def broadcast_uis(payload: dict) -> None:
    if not uis:
        return
    data = json.dumps(payload)
    await asyncio.gather(*(ws.send_str(data) for ws in list(uis)), return_exceptions=True)


async def push_status() -> None:
    await broadcast_uis({"type": "status", "agent": "connected" if agents else "disconnected"})


async def handle_index(request: web.Request) -> web.StreamResponse:
    resp = web.FileResponse(INDEX)
    resp.headers["Cache-Control"] = "no-store"
    return resp


async def handle_ws(request: web.Request) -> web.WebSocketResponse:
    ws = web.WebSocketResponse(heartbeat=30, max_msg_size=0)
    await ws.prepare(request)

    role = request.query.get("role", "ui")
    peer = request.remote
    log.info("%s connected from %s", role, peer)

    # ---------------- client (the remote machine) ----------------
    if role == "client":
        agents.add(ws)
        await push_status()
        try:
            async for msg in ws:
                if msg.type is not WSMsgType.TEXT:
                    continue
                try:
                    m = json.loads(msg.data)
                except json.JSONDecodeError:
                    continue

                if m.get("type") == "hello":
                    agent_hello.clear()
                    agent_hello.update(m)
                    await broadcast_uis({
                        "type": "hello",
                        "host": m.get("host"),
                        "cwd": m.get("cwd"),
                    })
                else:
                    # output / exit -> fan out to every UI
                    await broadcast_uis(m)
        finally:
            agents.discard(ws)
            agent_hello.clear()
            log.info("client disconnected (%s)", peer)
            await push_status()
        return ws

    # ---------------- ui (the browser) ----------------
    uis.add(ws)
    try:
        await _send(ws, {"type": "status", "agent": "connected" if agents else "disconnected"})
        if agent_hello:
            await _send(ws, {"type": "hello", "host": agent_hello.get("host"), "cwd": agent_hello.get("cwd")})

        async for msg in ws:
            if msg.type is not WSMsgType.TEXT:
                continue
            try:
                m = json.loads(msg.data)
            except json.JSONDecodeError:
                continue
            if m.get("type") != "command":
                continue

            if not agents:
                await _send(ws, {"type": "exit", "id": m.get("id"), "code": 127})
                continue

            data = json.dumps(m)
            await asyncio.gather(*(a.send_str(data) for a in list(agents)), return_exceptions=True)
    finally:
        uis.discard(ws)
        log.info("ui disconnected (%s)", peer)
    return ws


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-7s %(message)s",
        datefmt="%H:%M:%S",
    )
    app = web.Application()
    app.router.add_get("/", handle_index)
    app.router.add_get("/ws", handle_ws)
    app.router.add_get("/healthz", lambda _: web.json_response({"ok": True, "agent": bool(agents)}))
    log.info("host listening on http://0.0.0.0:8080")
    web.run_app(app, host="0.0.0.0", port=8080, print=None)


if __name__ == "__main__":
    main()