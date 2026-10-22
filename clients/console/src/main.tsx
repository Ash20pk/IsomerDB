// The IsomerDB console (PRD FR-O3). Served by the API at /console; talks to /v1/admin/* on the same
// origin with an API key whose role is a member of isomer_admin.

import { render } from "preact";
import { useEffect, useRef, useState } from "preact/hooks";

import { IsomerClient, IsomerError, currentLagMs, formatMs, lagRatio, type Overview } from "@isomerdb/sdk";

import { Advisor, Attachments, Events, Proposals, Routes, Usage } from "./pages.tsx";
import { ProjectionPage } from "./projection.tsx";
import { Track, describe, type History } from "./ui.tsx";

const KEY = "isomer.console.key";

function storedKey(): string | null {
  try {
    return localStorage.getItem(KEY);
  } catch {
    return null;
  }
}

function storeKey(key: string | null): void {
  try {
    if (key === null) localStorage.removeItem(KEY);
    else localStorage.setItem(KEY, key);
  } catch {
    // private mode: the key lasts for this page only
  }
}

const HISTORY_POINTS = 150; // five minutes at one sample every two seconds

function useHash(): string {
  const [hash, setHash] = useState(location.hash || "#/");
  useEffect(() => {
    const on = () => setHash(location.hash || "#/");
    addEventListener("hashchange", on);
    return () => removeEventListener("hashchange", on);
  }, []);
  return hash;
}

/** Open alerts, one line per alert and subject: repeated events collapse into a count. */
function Alerts({ alerts }: { alerts: Overview["alerts"] }) {
  const [all, setAll] = useState(false);
  const groups = new Map<string, { alert: string; subject: string; detail: string; n: number }>();
  for (const a of alerts) {
    const k = `${a.alert}\u0000${a.subject}`;
    const g = groups.get(k);
    if (g) g.n++;
    else groups.set(k, { ...a, n: 1 });
  }
  const list = [...groups.values()];
  if (list.length === 0) return null;
  const shown = all ? list : list.slice(0, 4);
  return (
    <ul class="alerts" aria-label="Alerts">
      {shown.map((a) => (
        <li key={`${a.alert}:${a.subject}`}>
          <span class="kind">{a.alert.replace(/_/g, " ")}</span>
          <a href={`#/p/${encodeURIComponent(a.subject)}`}>{a.subject}</a>{" "}
          <span class="quiet">
            {a.detail}
            {a.n > 1 ? ` (and ${a.n - 1} more like it in the last 15 minutes)` : ""}
          </span>
        </li>
      ))}
      {list.length > shown.length && (
        <li>
          <button class="link" onClick={() => setAll(true)}>
            Show all {list.length}
          </button>
        </li>
      )}
    </ul>
  );
}

function Board({ o }: { o: Overview }) {
  const live = o.projections.filter((p) => p.freshness === "bounded");
  const over = live.filter((p) => (lagRatio(p) ?? 0) > 1).length;
  const drifting = o.projections.filter((p) => p.drift > 0).length;
  const checks = o.projections.reduce((n, p) => n + p.checks, 0);
  return (
    <>
      <h1>Projections</h1>
      <p class="sub">
        {o.projections.length === 0
          ? "None yet. Declare one with isomer.create_projection, or accept a proposal."
          : over === 0 && drifting === 0
            ? "Every projection is within its lag budget and matches its tables."
            : `${over} over budget, ${drifting} drifting.`}
      </p>
      <div class="facts">
        <span><b>{o.projections.length}</b> {o.projections.length === 1 ? "projection" : "projections"}</span>
        <span><b>{checks.toLocaleString()}</b> audit checks</span>
        <span><b>{o.nodes.length}</b> {o.nodes.length === 1 ? "node" : "nodes"}</span>
        <span><b>{o.routes_enabled}</b> {o.routes_enabled === 1 ? "routed query" : "routed queries"}</span>
        <span>isomer {o.version}</span>
      </div>
      <Alerts alerts={o.alerts} />
      <div class="board">
        {o.projections.map((p) => (
          <a class="prow" key={p.name} href={`#/p/${encodeURIComponent(p.name)}`}>
            <div>
              <div class="pname">{p.name}</div>
              <div class="pmeta">
                {p.kind}, {p.freshness}
                {p.status !== "active" && <span class="warn"> ({p.status})</span>}
              </div>
            </div>
            <Track p={p} />
            <div class="lagnum">
              {p.freshness === "sync" ? <span class="quiet">in txn</span> : <b>{formatMs(currentLagMs(p))}</b>}
              <div class="pmeta">{p.pending_keys + p.embed_backlog} queued</div>
            </div>
            <div class="drift">
              <span class={`n ${p.drift > 0 ? "bad" : ""}`}>{p.drift === 0 ? "no drift" : `${p.drift} drifted`}</span>
              {p.checks.toLocaleString()} checks
            </div>
          </a>
        ))}
      </div>
    </>
  );
}

function Login({ onKey, error }: { onKey: (k: string) => void; error: string | null }) {
  const [key, setKey] = useState("");
  return (
    <form
      class="login"
      onSubmit={(e) => {
        e.preventDefault();
        if (key.trim()) onKey(key.trim());
      }}
    >
      <h1>IsomerDB console</h1>
      <p class="sub">Sign in with an API key for a role that is a member of isomer_admin.</p>
      <label for="key">API key</label>
      <input id="key" type="password" autocomplete="off" value={key} onInput={(e) => setKey((e.target as HTMLInputElement).value)} />
      {error && <p class="err">{error}</p>}
      <button class="primary" type="submit">Sign in</button>
      <p class="quiet">
        To create one, run <code>GRANT isomer_admin TO ops;</code> then <code>SELECT isomer.create_api_key('console', 'ops');</code>
      </p>
    </form>
  );
}

const SECTIONS: [string, string][] = [
  ["#/", "Projections"],
  ["#/proposals", "Proposals"],
  ["#/advisor", "Shard advisor"],
  ["#/routes", "Routes"],
  ["#/attachments", "Attachments"],
  ["#/events", "Events"],
  ["#/usage", "Usage"],
];

function App() {
  const [key, setKey] = useState<string | null>(storedKey());
  const [overview, setOverview] = useState<Overview | null>(null);
  const [error, setError] = useState<string | null>(null);
  const history = useRef<History>(new Map());
  const hash = useHash();
  const client = key ? new IsomerClient({ url: "", key }) : null;

  useEffect(() => {
    if (!key) return;
    const c = new IsomerClient({ url: "", key });
    let stop = false;
    const tick = async () => {
      try {
        const o = await c.overview();
        if (stop) return;
        const t = Date.now();
        for (const p of o.projections) {
          const h = history.current.get(p.name) ?? [];
          h.push({ t, lag: currentLagMs(p) });
          if (h.length > HISTORY_POINTS) h.shift();
          history.current.set(p.name, h);
        }
        setOverview(o);
        setError(null);
      } catch (e) {
        if (stop) return;
        if (e instanceof IsomerError && (e.status === 401 || e.status === 403)) {
          storeKey(null);
          setKey(null);
        }
        setError(describe(e));
      }
    };
    void tick();
    const id = setInterval(tick, 2000);
    return () => {
      stop = true;
      clearInterval(id);
    };
  }, [key]);

  if (!key || !client) {
    return (
      <Login
        error={error}
        onKey={(k) => {
          storeKey(k);
          setKey(k);
        }}
      />
    );
  }

  const section = hash.startsWith("#/p/") ? "#/" : hash;
  let page;
  if (hash.startsWith("#/p/")) {
    page = <ProjectionPage client={client} name={decodeURIComponent(hash.slice(4))} history={history.current} />;
  } else if (hash === "#/proposals") page = <Proposals client={client} />;
  else if (hash === "#/advisor") page = <Advisor client={client} />;
  else if (hash === "#/routes") page = <Routes client={client} />;
  else if (hash === "#/attachments") page = <Attachments client={client} />;
  else if (hash === "#/events") page = <Events client={client} />;
  else if (hash === "#/usage") page = <Usage client={client} />;
  else page = overview ? <Board o={overview} /> : <p class="quiet">{error ?? "Loading…"}</p>;

  return (
    <div class="frame">
      <nav class="rail" aria-label="Sections">
        <div class="mark">
          Isomer<span>DB</span>
        </div>
        {SECTIONS.map(([href, label]) => (
          <a key={href} href={href} aria-current={section === href ? "page" : undefined}>
            {label}
            {href === "#/proposals" && overview && overview.proposals_open > 0 && <span class="count">{overview.proposals_open}</span>}
          </a>
        ))}
        <div class="foot">
          {error ? <span class="err">{error}</span> : overview ? <>token {overview.token}</> : null}
          <br />
          <a
            href="#/"
            onClick={() => {
              storeKey(null);
              setKey(null);
            }}
          >
            Sign out
          </a>
        </div>
      </nav>
      <main>{page}</main>
    </div>
  );
}

render(<App />, document.getElementById("app")!);
