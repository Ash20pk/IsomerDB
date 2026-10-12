// Proposals, the shard advisor, routes, attachments and the event log.

import { useEffect, useState } from "preact/hooks";

import { formatMs, type IsomerClient, type UsageRow } from "@isomerdb/sdk";

import { describe, useToast } from "./ui.tsx";

/** Load once, reload after actions; render loading and error states the same way everywhere. */
function useLoad<T>(load: () => Promise<T>): { data: T | null; error: string | null; reload: () => Promise<void> } {
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState<string | null>(null);
  const reload = async () => {
    try {
      setData(await load());
      setError(null);
    } catch (e) {
      setError(describe(e));
    }
  };
  useEffect(() => {
    void reload();
  }, []);
  return { data, error, reload };
}

function Toast({ msg }: { msg: string | null }) {
  return msg ? (
    <div class="toast" role="status">
      {msg}
    </div>
  ) : null;
}

const oneLine = (q: string) => q.replace(/\s+/g, " ").trim();

export function Proposals({ client }: { client: IsomerClient }) {
  const { data, error, reload } = useLoad(() => client.proposals());
  const [toast, show] = useToast();
  const [busy, setBusy] = useState(false);
  const act = async (what: () => Promise<string>) => {
    setBusy(true);
    try {
      show(await what());
      await reload();
    } catch (e) {
      show(describe(e));
    } finally {
      setBusy(false);
    }
  };
  const open = data?.proposals.filter((p) => p.status === "proposed" || p.status === "needs_review") ?? [];
  const decided = data?.proposals.filter((p) => !open.includes(p)) ?? [];
  return (
    <>
      <h1>Proposals</h1>
      <p class="sub">Projections suggested from repeated join-and-nest queries in the query log, with what each would save and cost.</p>
      <div class="actions">
        <button disabled={busy} onClick={() => act(async () => `Found ${(await client.propose()).proposed} new proposals.`)}>
          Look for new proposals
        </button>
      </div>
      {error && <p class="err">{error}</p>}
      {data && open.length === 0 && <p class="quiet">Nothing waiting. Proposals appear once a join query has run often enough.</p>}
      {open.length > 0 && (
        <table style={{ marginTop: "16px" }}>
          <thead>
            <tr>
              <th>Query</th>
              <th class="num">Calls</th>
              <th class="num">Speedup</th>
              <th class="num">Saves</th>
              <th class="num">Costs</th>
              <th />
            </tr>
          </thead>
          <tbody>
            {open.map((p) => (
              <tr key={p.id}>
                <td>
                  <div class="q">{oneLine(p.source_query)}</div>
                  {p.problems.length > 0 && <div class="warn">{p.problems.join("; ")}</div>}
                </td>
                <td class="num">{p.calls ?? "—"}</td>
                <td class="num">{p.est_speedup ? `${p.est_speedup.toFixed(1)}×` : "—"}</td>
                <td class="num">{p.benefit_ms_per_s === null ? "—" : `${p.benefit_ms_per_s.toFixed(1)} ms/s`}</td>
                <td class="num">{p.cost_ms_per_s === null ? "—" : `${p.cost_ms_per_s.toFixed(1)} ms/s`}</td>
                <td>
                  <div class="actions">
                    <button class="primary" disabled={busy || p.problems.length > 0} onClick={() => act(async () => (await client.acceptProposal(p.id)).result)}>
                      Accept
                    </button>
                    <button disabled={busy} onClick={() => act(async () => (await client.rejectProposal(p.id), `Proposal ${p.id} rejected.`))}>
                      Reject
                    </button>
                  </div>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
      {decided.length > 0 && (
        <>
          <h2>Decided</h2>
          <table>
            <thead>
              <tr>
                <th>Query</th>
                <th>Decision</th>
                <th>Projection</th>
                <th />
              </tr>
            </thead>
            <tbody>
              {decided.map((p) => (
                <tr key={p.id}>
                  <td>
                    <div class="q">{oneLine(p.source_query)}</div>
                  </td>
                  <td>{p.status.replace("_", " ")}</td>
                  <td>{p.projection ? <a href={`#/p/${encodeURIComponent(p.projection)}`}>{p.projection}</a> : "—"}</td>
                  <td>
                    {p.projection && (
                      <button disabled={busy} onClick={() => act(async () => (await client.routeProposal(p.id)).result)}>
                        Route the query here
                      </button>
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </>
      )}
      <Toast msg={toast} />
    </>
  );
}

export function Advisor({ client }: { client: IsomerClient }) {
  const { data, error } = useLoad(() => client.advice());
  return (
    <>
      <h1>Shard advisor</h1>
      <p class="sub">Queries that fan out across shards and the key that would keep them on one, tables small enough to copy to every node, and projections co-location would let become sync.</p>
      {error && <p class="err">{error}</p>}
      {data && data.advice.length === 0 && <p class="quiet">Nothing to change: no hot cross-shard queries seen.</p>}
      {data && data.advice.length > 0 && (
        <table>
          <thead>
            <tr>
              <th>Finding</th>
              <th>About</th>
              <th>Recommendation</th>
            </tr>
          </thead>
          <tbody>
            {data.advice.map((a, i) => (
              <tr key={i}>
                <td>{a.advice.replace(/_/g, " ")}</td>
                <td>
                  <div class="q">{oneLine(a.subject)}</div>
                </td>
                <td>
                  {a.recommendation}
                  {a.detail && <div class="quiet">{a.detail}</div>}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </>
  );
}

export function Routes({ client }: { client: IsomerClient }) {
  const { data, error, reload } = useLoad(() => client.routes());
  const [toast, show] = useToast();
  return (
    <>
      <h1>Routes</h1>
      <p class="sub">Application queries answered from a projection instead of the tables, enabled only where the projection returned the same rows and was measurably faster.</p>
      {error && <p class="err">{error}</p>}
      {data && data.routes.length === 0 && <p class="quiet">No queries are routed. Accept a proposal, then route its query from the Proposals page.</p>}
      {data && data.routes.length > 0 && (
        <table>
          <thead>
            <tr>
              <th>Query</th>
              <th>Projection</th>
              <th class="num">Speedup</th>
              <th class="num">Keys verified</th>
              <th>Routing</th>
            </tr>
          </thead>
          <tbody>
            {data.routes.map((r) => (
              <tr key={r.queryid}>
                <td>
                  <div class="q">{oneLine(r.source_query)}</div>
                </td>
                <td>{r.projection && <a href={`#/p/${encodeURIComponent(r.projection)}`}>{r.projection}</a>}</td>
                <td class="num">{r.measured_speedup ? `${r.measured_speedup.toFixed(1)}×` : "—"}</td>
                <td class="num">{r.verified_keys}</td>
                <td>
                  <button
                    aria-pressed={r.enabled}
                    onClick={async () => {
                      try {
                        const x = await client.setRoute(r.queryid, !r.enabled);
                        show(x.enabled ? "Routing turned on." : "Routing turned off: the query reads the tables again.");
                        await reload();
                      } catch (e) {
                        show(describe(e));
                      }
                    }}
                  >
                    {r.enabled ? "Turn off" : "Turn on"}
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
      <Toast msg={toast} />
    </>
  );
}

export function Attachments({ client }: { client: IsomerClient }) {
  const { data, error } = useLoad(() => client.attachments());
  return (
    <>
      <h1>Attachments</h1>
      <p class="sub">Existing Postgres databases IsomerDB follows by logical replication.</p>
      {error && <p class="err">{error}</p>}
      {data && data.attachments.length === 0 && <p class="quiet">None. Follow a database with isomer.attach('name', 'host=… dbname=…').</p>}
      {data && data.attachments.length > 0 && (
        <table>
          <thead>
            <tr>
              <th>Source</th>
              <th>Status</th>
              <th>Slot</th>
              <th class="num">Apply lag</th>
              <th class="num">Tables ready</th>
              <th class="num">Errors</th>
            </tr>
          </thead>
          <tbody>
            {data.attachments.map((a) => (
              <tr key={a.attachment}>
                <td>{a.attachment}</td>
                <td class={a.status === "active" || a.status === "cutover" ? "" : "warn"}>{a.status}</td>
                <td>
                  {a.slot_active ? "streaming" : <span class="warn">not streaming</span>}
                  {a.wal_status && <div class="quiet">WAL {a.wal_status}</div>}
                </td>
                <td class="num">{formatMs(a.apply_lag_ms)}</td>
                <td class="num">
                  {a.tables_ready ?? 0} of {a.tables_total ?? 0}
                </td>
                <td class="num">{(a.apply_errors ?? 0) + (a.sync_errors ?? 0)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </>
  );
}

export function Events({ client }: { client: IsomerClient }) {
  const { data, error } = useLoad(() => client.events({ limit: 200 }));
  return (
    <>
      <h1>Events</h1>
      <p class="sub">What IsomerDB did and noticed, newest first.</p>
      {error && <p class="err">{error}</p>}
      {data && data.events.length === 0 && <p class="quiet">Nothing logged yet.</p>}
      {data && data.events.length > 0 && (
        <table>
          <tbody>
            {data.events.map((e) => (
              <tr key={e.id}>
                <td class="quiet">{new Date(e.at).toLocaleString()}</td>
                <td class={e.level === "error" ? "err" : e.level === "warning" ? "warn" : ""}>{e.level}</td>
                <td>{e.subject}</td>
                <td>{e.message}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </>
  );
}

// ── usage ───────────────────────────────────────────────────────────────────

const METRICS: { metric: string; title: string; unit: string }[] = [
  { metric: "storage_bytes", title: "Storage (peak)", unit: "bytes" },
  { metric: "node_hours", title: "Node-hours", unit: "node-hours" },
  { metric: "documents_written", title: "Documents written by projections", unit: "documents" },
  { metric: "embedding_tokens", title: "Embedding tokens", unit: "tokens" },
  { metric: "api_requests", title: "HTTP API requests", unit: "requests" },
  { metric: "sql_statements", title: "SQL statements", unit: "statements" },
];

export function formatUsage(v: number, unit: string): string {
  if (unit === "bytes") return v >= 1e9 ? `${(v / 1e9).toFixed(2)} GB` : `${(v / 1e6).toFixed(1)} MB`;
  if (unit === "node-hours") return `${v.toFixed(2)} node-h`;
  if (v >= 1e6) return `${(v / 1e6).toFixed(1)}M`;
  if (v >= 1e3) return `${(v / 1e3).toFixed(1)}K`;
  return String(Math.round(v));
}

type Period = "month" | "7d" | "30d";

function range(period: Period): { from: string; by: "day" | "hour" } {
  const now = new Date();
  if (period === "month") return { from: new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), 1)).toISOString(), by: "day" };
  const days = period === "7d" ? 7 : 30;
  return { from: new Date(now.getTime() - days * 86_400_000).toISOString(), by: "day" };
}

/** One metric's periods: a column per period, its value at the tip on hover, a table on request. */
function UsageChart({ title, unit, points }: { title: string; unit: string; points: { period: string; value: number }[] }) {
  const [hover, setHover] = useState<number | null>(null);
  const [asTable, setAsTable] = useState(false);
  const total = unit === "bytes" ? Math.max(0, ...points.map((p) => p.value)) : points.reduce((n, p) => n + p.value, 0);
  const w = 960;
  const h = 110;
  const max = Math.max(1, ...points.map((p) => p.value));
  const slot = points.length ? w / points.length : w;
  const barW = Math.min(24, Math.max(4, slot - 2));
  const label = (p: { period: string }) => p.period.slice(0, 10);
  return (
    <section class="usage">
      <header>
        <h2>{title}</h2>
        <span class="total">
          {formatUsage(total, unit)}
          <span class="quiet"> {unit === "bytes" ? "peak" : "in the period"}</span>
        </span>
        <button class="link" onClick={() => setAsTable(!asTable)}>
          {asTable ? "Show chart" : "Show table"}
        </button>
      </header>
      {points.length === 0 ? (
        <p class="quiet">Nothing recorded in this period.</p>
      ) : asTable ? (
        <table>
          <thead>
            <tr>
              <th>Day</th>
              <th class="num">{unit}</th>
            </tr>
          </thead>
          <tbody>
            {points.map((p) => (
              <tr key={p.period}>
                <td>{label(p)}</td>
                <td class="num">{formatUsage(p.value, unit)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      ) : (
        <div class="chartwrap" onMouseLeave={() => setHover(null)}>
          <svg viewBox={`0 0 ${w} ${h + 18}`} class="bars" role="img" aria-label={`${title}, ${points.length} days`}>
            <line class="baseline" x1="0" x2={w} y1={h} y2={h} />
            {points.map((p, i) => {
              const bh = Math.max(p.value > 0 ? 2 : 0, (p.value / max) * (h - 6));
              const x = i * slot + (slot - barW) / 2;
              const r = Math.min(4, bh / 2, barW / 2);
              // Rounded data-end, square at the baseline.
              const d = `M${x},${h} V${h - bh + r} Q${x},${h - bh} ${x + r},${h - bh} H${x + barW - r} Q${x + barW},${h - bh} ${x + barW},${h - bh + r} V${h} Z`;
              return (
                <g key={p.period} onMouseEnter={() => setHover(i)} onFocus={() => setHover(i)} tabIndex={0}>
                  <rect class="hit" x={i * slot} y="0" width={slot} height={h} />
                  <path class={`bar ${hover === i ? "on" : ""}`} d={d} />
                </g>
              );
            })}
            <text class="tick" x="0" y={h + 14}>{label(points[0]!)}</text>
            <text class="tick" x={w} y={h + 14} text-anchor="end">{label(points[points.length - 1]!)}</text>
          </svg>
          {hover !== null && points[hover] && (
            <div class="tip" style={{ left: `${Math.min(90, Math.max(10, ((hover + 0.5) / points.length) * 100))}%` }}>
              <b>{formatUsage(points[hover].value, unit)}</b> {label(points[hover])}
            </div>
          )}
        </div>
      )}
    </section>
  );
}

export function Usage({ client }: { client: IsomerClient }) {
  const [period, setPeriod] = useState<Period>("month");
  const [rows, setRows] = useState<UsageRow[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  useEffect(() => {
    setRows(null);
    const { from, by } = range(period);
    client.usage({ from, by }).then(
      (r) => (setRows(r.usage), setError(null)),
      (e) => setError(describe(e)),
    );
  }, [period]);
  const download = async () => {
    const { from, by } = range(period);
    const csv = await client.usageCsv({ from, by });
    const a = document.createElement("a");
    a.href = URL.createObjectURL(new Blob([csv], { type: "text/csv" }));
    a.download = `isomer-usage-${period}.csv`;
    a.click();
    URL.revokeObjectURL(a.href);
  };
  // Every day of the period, so the time axis is honest: days with nothing recorded show as 0.
  const days: string[] = [];
  for (let d = new Date(range(period).from.slice(0, 10) + "T00:00:00Z"); d.getTime() <= Date.now(); d = new Date(d.getTime() + 86_400_000)) {
    days.push(d.toISOString().slice(0, 10));
  }
  // Each metric summed over its dimensions per day (storage: the day's peak).
  const series = (metric: string, unit: string) => {
    const byDay = new Map<string, number>();
    for (const r of rows ?? []) {
      if (r.metric !== metric) continue;
      const day = r.period.slice(0, 10);
      const v = byDay.get(day) ?? 0;
      byDay.set(day, unit === "bytes" ? Math.max(v, r.value) : v + r.value);
    }
    if (byDay.size === 0) return [];
    return days.map((day) => ({ period: day, value: byDay.get(day) ?? 0 }));
  };
  return (
    <>
      <h1>Usage</h1>
      <p class="sub">What this cluster used, metered every minute: the figures a bill is made from.</p>
      <div class="actions" style={{ marginBottom: "18px" }}>
        <label class="quiet" for="period">
          Period
        </label>
        <select id="period" value={period} onChange={(e) => setPeriod((e.target as HTMLSelectElement).value as Period)}>
          <option value="month">This month</option>
          <option value="7d">Last 7 days</option>
          <option value="30d">Last 30 days</option>
        </select>
        <button onClick={() => void download()}>Download CSV</button>
      </div>
      {error && <p class="err">{error}</p>}
      {!rows && !error && <p class="quiet">Loading…</p>}
      {rows && METRICS.map((m) => <UsageChart key={m.metric} title={m.title} unit={m.unit} points={series(m.metric, m.unit)} />)}
    </>
  );
}
