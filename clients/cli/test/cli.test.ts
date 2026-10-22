// The built CLI against a stand-in API server.
import { test } from "node:test";
import assert from "node:assert/strict";
import { createServer } from "node:http";
import type { AddressInfo } from "node:net";

// @ts-expect-error: the bundle has no type declarations
const { main, renderStatus, renderPlan } = await import("../dist/isomer.mjs");

const overview = {
  version: "0.10.0",
  token: 7516192768000,
  at: "2026-10-12T10:00:00Z",
  projections: [
    { name: "orders_doc", kind: "document", freshness: "bounded", status: "active", lag_budget: "00:00:02", lag_budget_ms: 2000,
      pending_keys: 3, oldest_pending_ms: 3000, embed_backlog: 0, oldest_embed_job_ms: null, search_staleness_ms: null,
      drift: 0, checks: 120, drift_summary: "drift 0 rows · 120 checks · since 2026-10-12 09:00", budget_breaches: 1 },
    { name: "counts", kind: "cache", freshness: "sync", status: "active", lag_budget: "00:00:02", lag_budget_ms: 2000,
      pending_keys: 0, oldest_pending_ms: null, embed_backlog: 0, oldest_embed_job_ms: null, search_staleness_ms: null,
      drift: 2, checks: 50, drift_summary: "drift 2 rows · 50 checks · since 2026-10-12 09:00", budget_breaches: 0 },
  ],
  alerts: [{ alert: "lag_budget", subject: "orders_doc", detail: "pending for 00:00:03 (budget 00:00:02)" }],
  attachments: [],
  nodes: [{ host: "db", port: 5432, role: "primary", active: true, has_shards: true, placements: 32 }],
  proposals_open: 1,
  routes_enabled: 0,
};

const plain = { bold: (s: string) => s, dim: (s: string) => s, red: (s: string) => s, yellow: (s: string) => s, green: (s: string) => s };

test("status shows lag against budget, drift and alerts", () => {
  const out: string = renderStatus(overview, plain);
  assert.match(out, /orders_doc\s+document\s+bounded\s+3\.0s\s+2\.0s\s+▰▰▰▰▰▰▰▰▰▰\s+3\s+0\s+120\s+active/);
  assert.match(out, /counts\s+cache\s+sync\s+in txn/);
  assert.match(out, /1 alert\n  lag_budget  orders_doc/);
  assert.match(out, /1 proposal waiting/);
});

const plan = [
  { ord: 1, phase: "before", item: "Initial copy", status: "ok", detail: "all 2 table(s) copied", sql: null },
  { ord: 2, phase: "before", item: "Tables not copied", status: "blocked", detail: "shop.log (no primary key)", sql: null },
  { ord: 3, phase: "schema", item: "Indexes", status: "todo", detail: "1 missing: shop.i", sql: "CREATE INDEX IF NOT EXISTS i ON shop.t (a);" },
  { ord: 4, phase: "cutover", item: "Cut over", status: "blocked", detail: "1 item(s) above must be fixed first", sql: "SELECT * FROM isomer.cutover('shop');" },
];

test("a cutover plan reads as a checklist, with each step's SQL", () => {
  const out: string = renderPlan("shop", plan, plain);
  assert.match(out, /^Cutover plan: shop\n1 item blocks cutover; 1 to do/);
  assert.match(out, /Before cutover\n  ✓ Initial copy: all 2 table\(s\) copied\n  ✗ Tables not copied: shop.log/);
  assert.match(out, /What the copy does not carry yet\n  ○ Indexes: 1 missing: shop.i\n      CREATE INDEX IF NOT EXISTS i ON shop.t \(a\);/);
});

test("commands talk to the API with the key, and exit codes follow the outcome", async () => {
  const seen: string[] = [];
  const server = createServer((req, res) => {
    seen.push(`${req.method} ${req.url} ${req.headers.authorization}`);
    res.setHeader("content-type", "application/json");
    if (req.url === "/v1/admin/overview") return res.end(JSON.stringify(overview));
    if (req.url === "/v1/admin/proposals/4/accept") {
      let body = "";
      req.on("data", (c) => (body += c));
      return req.on("end", () => res.end(JSON.stringify({ id: 4, result: `accepted with ${body}` })));
    }
    if (req.url === "/v1/admin/attachments/shop/plan") return res.end(JSON.stringify({ attachment: "shop", steps: plan }));
    if (req.url === "/v1/admin/routes/99") {
      res.statusCode = 404;
      return res.end(JSON.stringify({ error: "no route for query 99", code: null }));
    }
    res.statusCode = 404;
    res.end("{}");
  });
  await new Promise<void>((r) => server.listen(0, "127.0.0.1", r));
  const url = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
  const lines: string[] = [];
  const write = (s: string) => lines.push(s);
  try {
    assert.equal(await main(["status", "--url", url, "--key", "k1"], write), 1, "alerts make status exit 1");
    assert.equal(await main(["drift", "--json", "--url", url, "--key", "k1"], write), 1, "drift makes drift exit 1");
    assert.deepEqual(JSON.parse(lines.at(-1)!).map((r: { drift: number }) => r.drift), [0, 2]);
    assert.equal(await main(["accept", "4", "--sync", "--name", "o", "--url", url, "--key", "k1"], write), 0);
    assert.equal(lines.at(-1), 'accepted with {"name":"o","freshness":"sync"}');
    await assert.rejects(main(["disable-route", "99", "--url", url, "--key", "k1"], write), /no route for query 99/);
    await assert.rejects(main(["accept", "x", "--url", url, "--key", "k1"], write), /proposal id must be an integer/);
    assert.equal(await main(["plan", "shop", "--url", url, "--key", "k1"], write), 1, "a blocked plan exits 1");
    assert.ok(seen.every((l) => l.endsWith("Bearer k1")));
  } finally {
    server.close();
  }
});
