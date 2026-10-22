"""A stand-in for Slack, PagerDuty and generic webhooks in tests: records every POST.

    GET    /received   every request so far, as JSON [{path, auth, body}]
    DELETE /received   forget them
    POST   /flaky/...  fails (503) the first two requests about each subject, then accepts
    POST   anything    accepted (202)
"""

import json
from collections import defaultdict
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

received = []
attempts = defaultdict(int)


class Handler(BaseHTTPRequestHandler):
    def _send(self, code, body=None):
        data = json.dumps(body).encode() if body is not None else b""
        self.send_response(code)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        self._send(200, received) if self.path == "/received" else self._send(404, {})

    def do_DELETE(self):
        received.clear()
        attempts.clear()
        self._send(204)

    def do_POST(self):
        body = self.rfile.read(int(self.headers.get("content-length", 0)) or 0)
        try:
            parsed = json.loads(body or b"null")
        except ValueError:
            parsed = body.decode(errors="replace")
        # Per subject, so an alert left over from another test cannot use up the failures.
        subject = parsed.get("subject") if isinstance(parsed, dict) else None
        attempts[(self.path, subject)] += 1
        if self.path.startswith("/flaky") and attempts[(self.path, subject)] <= 2:
            return self._send(503, {"error": "try again"})
        received.append({"path": self.path, "auth": self.headers.get("authorization"), "body": parsed})
        self._send(202, {"status": "ok"})

    def log_message(self, *args):
        pass


ThreadingHTTPServer(("0.0.0.0", 8090), Handler).serve_forever()
