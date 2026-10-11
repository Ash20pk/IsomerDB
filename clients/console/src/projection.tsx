// One projection: its lag over the last few minutes against the budget, drift, definition,
// sources, certificate, routes and recent events, with audit and repair actions.

import { useEffect, useState } from "preact/hooks";

import { currentLagMs, formatMs, type IsomerClient, type ProjectionDetail } from "@isomerdb/sdk";

import { Track, describe, useToast, type History, type Sample } from "./ui.tsx";

function Sparkline({ samples, budget }: { samples: Sample[]; budget: number | null }) {
  if (samples.length < 2) return <p class="quiet">Collecting lag samples (one every two seconds).</p>;
  const w = 600;
  const h = 64;
  const max = Math.max(budget ?? 0, ...samples.map((s) => s.lag), 1) * 1.1;
  const t0 = samples[0]!.t;
  const span = Math.max(samples[samples.length - 1]!.t - t0, 1);
  const x = (t: number) => ((t - t0) / span) * w;
  const y = (lag: number) => h - (lag / max) * h;
  const d = samples.map((s, i) => `${i ? "L" : "M"}${x(s.t).toFixed(1)},${y(s.lag).toFixed(1)}`).join(" ");
  const peak = Math.max(...samples.map((s) => s.lag));
  return (
    <figure style={{ margin: 0 }}>
      <svg class="spark" viewBox={`0 0 ${w} ${h}`} preserveAspectRatio="none" role="img" aria-label={`lag over the last ${formatMs(span)}, peak ${formatMs(peak)}`}>
        {budget ? <line class="budget" x1="0" x2={w} y1={y(budget)} y2={y(budget)} vector-effect="non-scaling-stroke" /> : null}
        <path class="line" d={d} vector-effect="non-scaling-stroke" />
      </svg>
      <figcaption class="quiet">
        Lag over the last {formatMs(span)}: peak {formatMs(peak)}
        {budget ? `, budget ${formatMs(budget)} (dashed)` : ""}.
      </figcaption>
    </figure>
  );
}

export function ProjectionPage({ client, name, history }: { client: IsomerClient; name: string; history: History }) {
  const [d, setD] = useState<ProjectionDetail | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [toast, show] = useToast();
  const [, setTick] = useState(0);

  const load = () =>
    client.projection(name).then(
      (x) => (setD(x), setError(null)),
      (e) => setError(describe(e)),
    );
  useEffect(() => {
    void load();
    const id = setInterval(() => setTick((n) => n + 1), 2000); // redraw the sparkline as samples arrive
    return () => clearInterval(id);
  }, [name]);

  const act = async (what: () => Promise<string>) => {
    setBusy(true);
    try {
      show(await what());
      await load();
    } catch (e) {
      show(describe(e));
    } finally {
      setBusy(false);
    }
  };

  if (error) return <p class="err">{error}</p>;
  if (!d) return <p class="quiet">Loading {name}…</p>;
  const st = d.status;
  return (
    <>
      <p class="quiet">
        <a href="#/">Projections</a> / {name}
      </p>
      <h1>{name}</h1>
      <p class="sub">
        A {d.definition.freshness} {d.definition.kind} projection
        {d.definition.freshness === "bounded" && st ? ` with a ${formatMs(st.lag_budget_ms)} lag budget` : ", updated in the writing transaction"}.
      </p>
      {st && (
        <>
          <Track p={st} />
          <dl class="kv" style={{ marginTop: "18px" }}>
            <dt>Behind by</dt>
            <dd>{st.freshness === "sync" ? "nothing (sync)" : formatMs(currentLagMs(st))}</dd>
            <dt>Queued</dt>
            <dd>
              {st.pending_keys} keys{st.kind === "vector" ? `, ${st.embed_backlog} embeddings, ${st.embed_dead_letters} failed` : ""}
            </dd>
            <dt>Budget breaches</dt>
            <dd>{st.budget_breaches ?? 0}</dd>
            <dt>Drift</dt>
            <dd class={st.drift > 0 ? "err" : ""}>{st.drift_summary}</dd>
            <dt>Fresh through token</dt>
            <dd>{d.freshness_token}</dd>
            {st.kind === "vector" && (
              <>
                <dt>Model</dt>
                <dd>
                  {st.model}
                  {st.pending_model ? `, moving to ${st.pending_model}` : ""}
                </dd>
                <dt>Embedding cost this month</dt>
                <dd>
                  ${st.embed_cost_this_month ?? 0}
                  {st.monthly_cap_usd ? ` of a $${st.monthly_cap_usd} cap` : ""}
                </dd>
              </>
            )}
            <dt>History</dt>
            <dd>{st.history ? `kept for ${st.history}` : "off"}</dd>
          </dl>
          {st.freshness === "bounded" && (
            <>
              <h2>Lag</h2>
              <Sparkline samples={history.get(name) ?? []} budget={st.lag_budget_ms} />
            </>
          )}
        </>
      )}
      <div class="actions" style={{ marginTop: "20px" }}>
        <button disabled={busy} onClick={() => act(async () => `Repaired ${(await client.repair(name)).repaired} drifted keys.`)}>
          Repair drifted keys
        </button>
        <button
          disabled={busy}
          onClick={() =>
            act(async () => {
              const r = await client.audit();
              return r.drift_found === 0 ? "Audit found no drift." : `Audit found ${r.drift_found} drifted rows.`;
            })
          }
        >
          Audit now
        </button>
      </div>

      <h2>Query</h2>
      <pre>{String(d.definition.query).trim()}</pre>

      {d.sources.length > 0 && (
        <>
          <h2>Source tables</h2>
          <table>
            <thead>
              <tr>
                <th>Table</th>
                <th>Maps to the key through</th>
              </tr>
            </thead>
            <tbody>
              {d.sources.map((s) => (
                <tr key={s.table}>
                  <td>{s.table}</td>
                  <td>{s.key_column ?? <span class="quiet">nothing (reference table: a change re-derives every key)</span>}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </>
      )}

      <h2>Correctness certificate</h2>
      <pre>{JSON.stringify(d.certificate, null, 2)}</pre>

      {d.routes.length > 0 && (
        <>
          <h2>Queries routed here</h2>
          <table>
            <thead>
              <tr>
                <th>Query</th>
                <th class="num">Speedup</th>
                <th>Routing</th>
              </tr>
            </thead>
            <tbody>
              {d.routes.map((r) => (
                <tr key={r.queryid}>
                  <td>
                    <div class="q">{r.source_query}</div>
                  </td>
                  <td class="num">{r.measured_speedup ? `${r.measured_speedup.toFixed(1)}×` : "—"}</td>
                  <td>{r.enabled ? "on" : <span class="quiet">off</span>}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </>
      )}

      <h2>Recent events</h2>
      {d.events.length === 0 ? (
        <p class="quiet">Nothing logged for this projection.</p>
      ) : (
        <table>
          <tbody>
            {d.events.map((e) => (
              <tr key={e.id}>
                <td class="quiet">{new Date(e.at).toLocaleString()}</td>
                <td class={e.level === "error" ? "err" : e.level === "warning" ? "warn" : ""}>{e.level}</td>
                <td>{e.message}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
      {toast && (
        <div class="toast" role="status">
          {toast}
        </div>
      )}
    </>
  );
}
