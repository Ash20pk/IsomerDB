"""A stand-in OpenAI-compatible embeddings endpoint for tests.

- POST /v1/embeddings  {"model": "mock-<dims>", "input": [...]}  -> deterministic embeddings
  Requires "Authorization: Bearer test-key". Every RATE_LIMIT_EVERY-th request gets a 429 with
  Retry-After: 1, so the embedder's retry path is exercised. Input "FAIL" always gets a 400.
- GET  /stats  -> how many times each input text was embedded, for asserting that unchanged rows
  are never re-embedded.
- POST /reset  -> clear the counters.
- The Batch API: POST /v1/files (multipart, purpose=batch), POST /v1/batches, GET
  /v1/batches/<id>, GET /v1/files/<id>/content. A batch reports "in_progress" on its first check
  and "completed" on the next, when its output file is written; an input "FAIL" gets a 400 line.
  Inputs embedded through batches are counted separately (batch_inputs in /stats).
"""
import hashlib
import json
import math
import os
from collections import Counter
from email.parser import BytesParser
from email.policy import HTTP
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

RATE_LIMIT_EVERY = int(os.environ.get("RATE_LIMIT_EVERY", "5"))
state = {"requests": 0, "rate_limited": 0, "inputs": Counter(), "batch_inputs": Counter(),
         "files": {}, "batches": {}}


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
                            "by_input": dict(state["inputs"]),
                            "batches": len(state["batches"]),
                            "batch_inputs_total": sum(state["batch_inputs"].values()),
                            "batch_by_input": dict(state["batch_inputs"])})
        elif self.path.startswith("/v1/batches/"):
            batch = state["batches"].get(self.path.rsplit("/", 1)[1])
            if batch is None:
                return self.send(404, {"error": {"message": "no such batch"}})
            batch["checks"] += 1
            if batch["checks"] >= 2 and batch["status"] == "in_progress":
                self.run_batch(batch)
            self.send(200, {k: v for k, v in batch.items() if k != "checks"})
        elif self.path.startswith("/v1/files/") and self.path.endswith("/content"):
            content = state["files"].get(self.path.split("/")[3])
            if content is None:
                return self.send(404, {"error": {"message": "no such file"}})
            self.send_response(200)
            self.send_header("Content-Type", "application/jsonl")
            self.send_header("Content-Length", str(len(content)))
            self.end_headers()
            self.wfile.write(content)
        else:
            self.send(404, {"error": "not found"})

    def run_batch(self, batch):
        out = []
        for line in state["files"][batch["input_file_id"]].decode().splitlines():
            req = json.loads(line)
            text = req["body"]["input"]
            dims = int(req["body"]["model"].rsplit("-", 1)[1])
            if text == "FAIL":
                out.append({"custom_id": req["custom_id"], "error": None,
                            "response": {"status_code": 400, "body": {"error": {"message": "input rejected"}}}})
                continue
            state["batch_inputs"][text] += 1
            tokens = max(1, len(text) // 4)
            out.append({"custom_id": req["custom_id"], "error": None,
                        "response": {"status_code": 200, "body": {
                            "data": [{"object": "embedding", "index": 0, "embedding": embedding(text, dims)}],
                            "usage": {"prompt_tokens": tokens, "total_tokens": tokens}}}})
        file_id = f"file-{len(state['files']) + 1}"
        state["files"][file_id] = "".join(json.dumps(o) + "\n" for o in out).encode()
        batch.update(status="completed", output_file_id=file_id)

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        raw = self.rfile.read(length)
        body = {} if self.path == "/v1/files" else json.loads(raw or b"{}")
        if self.path == "/reset":
            state.update(requests=0, rate_limited=0, inputs=Counter(), batch_inputs=Counter(), files={}, batches={})
            return self.send(200, {"ok": True})
        if self.path in ("/v1/files", "/v1/batches") and self.headers.get("Authorization") != "Bearer test-key":
            return self.send(401, {"error": {"message": "invalid api key"}})
        if self.path == "/v1/files":
            msg = BytesParser(policy=HTTP).parsebytes(
                b"Content-Type: " + self.headers["Content-Type"].encode() + b"\r\n\r\n" + raw)
            parts = {part.get_param("name", header="content-disposition"): part.get_payload(decode=True)
                     for part in msg.iter_parts()}
            file_id = f"file-{len(state['files']) + 1}"
            state["files"][file_id] = parts["file"]
            return self.send(200, {"id": file_id, "object": "file", "purpose": parts.get("purpose", b"").decode()})
        if self.path == "/v1/batches":
            batch_id = f"batch-{len(state['batches']) + 1}"
            state["batches"][batch_id] = {"id": batch_id, "object": "batch", "status": "in_progress",
                                          "input_file_id": body["input_file_id"], "output_file_id": None,
                                          "error_file_id": None, "errors": None, "checks": 0}
            return self.send(200, {k: v for k, v in state["batches"][batch_id].items() if k != "checks"})
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
