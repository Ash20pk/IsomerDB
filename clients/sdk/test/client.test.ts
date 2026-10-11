import { test } from "node:test";
import assert from "node:assert/strict";
import { IsomerClient, IsomerError, currentLagMs, formatMs, lagRatio, type ProjectionStatus } from "../src/index.ts";

function fakeFetch(handler: (url: string, init: RequestInit) => { status: number; body: unknown }): typeof fetch {
  return (async (input: string | URL | Request, init?: RequestInit) => {
    const { status, body } = handler(String(input), init ?? {});
    return new Response(body === undefined ? "" : JSON.stringify(body), { status });
  }) as typeof fetch;
}

test("sends the key and parses JSON", async () => {
  let seen: { url: string; auth: string | null } | undefined;
  const client = new IsomerClient({
    url: "http://api:8080/",
    key: "secret",
    fetch: fakeFetch((url, init) => {
      seen = { url, auth: new Headers(init.headers).get("authorization") };
      return { status: 200, body: { proposals: [] } };
    }),
  });
  assert.deepEqual(await client.proposals(), { proposals: [] });
  assert.deepEqual(seen, { url: "http://api:8080/v1/admin/proposals", auth: "Bearer secret" });
});

test("API errors carry status and SQLSTATE", async () => {
  const client = new IsomerClient({
    url: "",
    key: "k",
    fetch: fakeFetch(() => ({ status: 503, body: { error: "not caught up (not yet)", code: "IS001" } })),
  });
  await assert.rejects(client.doc("p", "1", { after: 42 }), (e: unknown) => {
    assert.ok(e instanceof IsomerError);
    assert.equal(e.status, 503);
    assert.equal(e.code, "IS001");
    return true;
  });
});

test("read-your-writes token goes in X-Isomer-After, keys are escaped", async () => {
  let seen: { url: string; after: string | null } | undefined;
  const client = new IsomerClient({
    url: "http://x",
    key: "k",
    fetch: fakeFetch((url, init) => {
      seen = { url, after: new Headers(init.headers).get("x-isomer-after") };
      return { status: 200, body: {} };
    }),
  });
  await client.doc("orders", "a/b", { after: 123 });
  assert.deepEqual(seen, { url: "http://x/v1/projections/orders/docs/a%2Fb", after: "123" });
});

test("lag helpers", () => {
  const base = { freshness: "bounded", kind: "document", lag_budget_ms: 2000, oldest_pending_ms: 500,
                 search_staleness_ms: null, oldest_embed_job_ms: null } as unknown as ProjectionStatus;
  assert.equal(currentLagMs(base), 500);
  assert.equal(lagRatio(base), 0.25);
  assert.equal(lagRatio({ ...base, freshness: "sync" }), 0);
  assert.equal(currentLagMs({ ...base, kind: "vector", oldest_embed_job_ms: 3000 }), 3000);
  assert.equal(formatMs(350), "350ms");
  assert.equal(formatMs(1500), "1.5s");
  assert.equal(formatMs(250_000), "4m 10s");
  assert.equal(formatMs(null), "—");
});
