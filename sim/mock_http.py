"""HTTP front-end for the Simulator so the real ApiClient can be tested end-to-end."""
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

PREFIX = "/api/v1"


def make_server(sim, token="TEST_TOKEN", port=0):
    class H(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *a):
            pass

        def _send(self, code, obj):
            raw = json.dumps(obj).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def _authed(self):
            return self.headers.get("Authorization", "") == f"Bearer {token}"

        def do_GET(self):
            u = urlparse(self.path)
            p, q = u.path, {k: v[0] for k, v in parse_qs(u.query).items()}
            if not p.startswith(PREFIX):
                return self._send(404, {"error": "not found"})
            p = p[len(PREFIX):]
            if p == "/stage":
                return self._send(*sim.api_stage())
            if p == "/simulation/clock":
                return self._send(*sim.api_clock())
            if not self._authed():
                return self._send(401, {"status": "error", "error": "UNAUTHORIZED"})
            if p == "/stocks":
                return self._send(*sim.api_stocks())
            if p.startswith("/stocks/") and p.endswith("/history"):
                return self._send(*sim.api_history(p.split("/")[2], int(q.get("days", 365))))
            if p == "/portfolio":
                return self._send(*sim.api_portfolio())
            if p == "/portfolio/cash":
                return self._send(*sim.api_cash())
            if p == "/orders":
                return self._send(200, {"status": "ok", "orders": sim.orders})
            self._send(404, {"error": "not found"})

        def do_POST(self):
            n = int(self.headers.get("Content-Length", 0) or 0)
            body = json.loads(self.rfile.read(n) or b"{}")
            if not self._authed():
                return self._send(401, {"status": "error", "error": "UNAUTHORIZED"})
            p = urlparse(self.path).path[len(PREFIX):]
            if p == "/orders":
                return self._send(*sim.api_order(body))
            if p == "/bot/heartbeat":
                return self._send(*sim.api_heartbeat(body.get("seq")))
            self._send(404, {"error": "not found"})

    srv = ThreadingHTTPServer(("127.0.0.1", port), H)
    srv.daemon_threads = True
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv
