"""A stand-in OpenAI-compatible embeddings endpoint for tests.

- POST /v1/embeddings  {"model": "mock-<dims>", "input": [...]}  -> deterministic embeddings
  Requires "Authorization: Bearer test-key". Every RATE_LIMIT_EVERY-th request gets a 429 with
  Retry-After: 1, so the embedder's retry path is exercised. Input "FAIL" always gets a 400.
- GET  /stats  -> how many times each input text was embedded, for asserting that unchanged rows
  are never re-embedded.
- POST /reset  -> clear the counters.
"""
import hashlib
import json
import math
import os
from collections import Counter
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

RATE_LIMIT_EVERY = int(os.environ.get("RATE_LIMIT_EVERY", "5"))
state = {"requests": 0, "rate_limited": 0, "inputs": Counter()}


def embedding(text: str, dims: int) -> list:
    out = []
    for i in range(dims):
        h = hashlib.sha256(f"{i}:{text.lower()}".encode()).digest()
        out.append(int.from_bytes(h[:4], "big") / 2**32 - 0.5)
    norm = math.sqrt(sum(x * x for x in out)) or 1.0
    return [x / norm for x in out]


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def send(self, code, body, headers=None):
        data = json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        for k, v in (headers or {}).items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        if self.path == "/stats":
            self.send(200, {"requests": state["requests"], "rate_limited": state["rate_limited"],
                            "inputs_total": sum(state["inputs"].values()),
                            "by_input": dict(state["inputs"])})
        else:
            self.send(404, {"error": "not found"})

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = json.loads(self.rfile.read(length) or b"{}")
        if self.path == "/reset":
            state.update(requests=0, rate_limited=0, inputs=Counter())
            return self.send(200, {"ok": True})
        if self.path != "/v1/embeddings":
            return self.send(404, {"error": "not found"})
        if self.headers.get("Authorization") != "Bearer test-key":
            return self.send(401, {"error": {"message": "invalid api key"}})
        state["requests"] += 1
        if RATE_LIMIT_EVERY and state["requests"] % RATE_LIMIT_EVERY == 0:
            state["rate_limited"] += 1
            return self.send(429, {"error": {"message": "rate limited"}}, {"Retry-After": "1"})
        inputs = body["input"] if isinstance(body["input"], list) else [body["input"]]
        if "FAIL" in inputs:
            return self.send(400, {"error": {"message": "input rejected"}})
        dims = int(body["model"].rsplit("-", 1)[1])
        for text in inputs:
            state["inputs"][text] += 1
        tokens = sum(max(1, len(t) // 4) for t in inputs)
        self.send(200, {"object": "list", "model": body["model"],
                        "data": [{"object": "embedding", "index": i, "embedding": embedding(t, dims)}
                                 for i, t in enumerate(inputs)],
                        "usage": {"prompt_tokens": tokens, "total_tokens": tokens}})


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", 8080), Handler).serve_forever()
