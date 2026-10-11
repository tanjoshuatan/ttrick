from aiohttp import web

HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>hello · :1234</title>
<style>
  :root { color-scheme: dark; }
  * { box-sizing: border-box; }
  html, body { height: 100%; margin: 0; }
  body {
    display: grid;
    place-items: center;
    background: radial-gradient(1200px 800px at 50% 30%, #1b1035 0%, #0a0a0f 60%, #05050a 100%);
    font-family: ui-monospace, "SF Mono", Menlo, Consolas, monospace;
    color: #e5e7eb;
    overflow: hidden;
  }
  .wrap { text-align: center; padding: 2rem; }
  h1 {
    font-size: clamp(2.5rem, 10vw, 7rem);
    font-weight: 800;
    letter-spacing: -0.04em;
    margin: 0;
    background: linear-gradient(120deg, #a3e635 0%, #22d3ee 40%, #a78bfa 70%, #f472b6 100%);
    background-size: 200% 200%;
    -webkit-background-clip: text;
    background-clip: text;
    color: transparent;
    animation: shift 8s ease-in-out infinite;
  }
  .tag {
    margin-top: 1.25rem;
    font-size: 0.9rem;
    color: #9ca3af;
    letter-spacing: 0.15em;
    text-transform: uppercase;
  }
  .pill {
    display: inline-flex; align-items: center; gap: 0.5rem;
    margin-top: 2rem;
    padding: 0.4rem 0.9rem;
    border: 1px solid #2a2a35;
    border-radius: 999px;
    background: rgba(255,255,255,0.03);
    backdrop-filter: blur(6px);
    font-size: 0.8rem;
    color: #d1d5db;
  }
  .dot {
    width: 8px; height: 8px; border-radius: 999px;
    background: #a3e635;
    box-shadow: 0 0 12px #a3e635, 0 0 24px #a3e63588;
    animation: pulse 2s ease-in-out infinite;
  }
  .glow {
    position: fixed; inset: auto auto -20vh 50%;
    transform: translateX(-50%);
    width: 80vw; height: 60vh;
    background: radial-gradient(closest-side, #a3e63522, transparent 70%);
    pointer-events: none;
    filter: blur(40px);
  }
  @keyframes shift {
    0%, 100% { background-position: 0% 50%; }
    50%      { background-position: 100% 50%; }
  }
  @keyframes pulse {
    0%, 100% { opacity: 1; transform: scale(1); }
    50%      { opacity: 0.5; transform: scale(0.85); }
  }
</style>
</head>
<body>
  <div class="glow"></div>
  <div class="wrap">
    <h1>hello.</h1>
    <div class="tag">from port 1234</div>
    <div class="pill"><span class="dot"></span> tunnel ok</div>
  </div>
</body>
</html>
"""


async def handle_index(request):
    return web.Response(text=HTML, content_type="text/html")


def main() -> None:
    app = web.Application()
    app.router.add_get("/", handle_index)
    app.router.add_get("/{tail:.*}", handle_index)
    print("sample server on http://0.0.0.0:1234", flush=True)
    web.run_app(app, host="0.0.0.0", port=1234, print=None, access_log=None)


if __name__ == "__main__":
    main()