// isomer: the IsomerDB command line (PRD FR-O3).
//
//   isomer login --url http://localhost:8080 --key <api key>
//   isomer status                      projections: lag against budget, backlog, drift; alerts; nodes
//   isomer watch [--interval 2]        the same, refreshed
//   isomer projection <name>           definition, sources, certificate, routes, recent events
//   isomer get <projection> <key> [--after <token>] [--as-of <time>]
//   isomer drift | alerts | attachments | advice | routes | events [--subject s] [--limit n]
//   isomer audit [--sample n] | repair <projection>   (repair: re-derive the keys the auditor found drifted)
//   isomer proposals | propose | accept <id> [--name n] [--sync] [--database db] | reject <id> | route <id>
//   isomer enable-route <queryid> | disable-route <queryid>
//
// Every command takes --json for machine-readable output, and --url / --key, which override
// ISOMER_URL / ISOMER_KEY and the file `isomer login` writes (~/.config/isomer/credentials.json).

import { chmodSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { homedir } from "node:os";
import { dirname, join } from "node:path";
import { parseArgs } from "node:util";

import { IsomerClient, IsomerError, currentLagMs, formatMs, lagRatio, type Overview } from "@isomerdb/sdk";

import { bar, plural, style, table, type Style } from "./format.ts";

const USAGE = `usage: isomer <command> [options]

  login --url <url> --key <key>     save where the API is and the key to use
  status                            projections with lag, backlog and drift; alerts; nodes
  watch [--interval <s>]            status, refreshed every few seconds
  projection <name>                 one projection in full
  get <projection> <key>            a document (--after <token> waits for a write; --as-of <time>)
  drift | alerts | attachments      the auditor's counters, open alerts, attach-mode sources
  advice                            the shard advisor
  audit [--sample <n>]              run the auditor now
  repair <projection>               re-derive the keys the auditor found drifted
  proposals | propose               projections mined from the query log; mine now
  accept <id> [--name n] [--sync] [--database db]
  reject <id> | route <id>          reject a proposal; route its query onto the projection
  routes                            routed queries
  enable-route <queryid> | disable-route <queryid>
  events [--subject s] [--limit n]  the event log

options: --url, --key (or ISOMER_URL, ISOMER_KEY), --json`;

interface Credentials {
  url?: string;
  key?: string;
}

const CREDENTIALS = join(process.env.XDG_CONFIG_HOME ?? join(homedir(), ".config"), "isomer", "credentials.json");

function saved(): Credentials {
  try {
    return JSON.parse(readFileSync(CREDENTIALS, "utf8")) as Credentials;
  } catch {
    return {};
  }
}

class UsageError extends Error {}

function need(arg: string | undefined, what: string): string {
  if (!arg) throw new UsageError(`missing ${what}`);
  return arg;
}

function int(arg: string | undefined, what: string): number {
  const n = Number(need(arg, what));
  if (!Number.isInteger(n)) throw new UsageError(`${what} must be an integer`);
  return n;
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

export function renderStatus(o: Overview, s: Style): string {
  const out: string[] = [];
  out.push(`${s.bold("IsomerDB")} ${o.version}  ·  token ${o.token}  ·  ${plural(o.projections.length, "projection")}  ·  ${plural(o.nodes.length, "node")}`);
  out.push("");
  if (o.projections.length === 0) {
    out.push(s.dim("no projections yet (isomer.create_projection)"));
  } else {
    out.push(
      table(
        ["PROJECTION", "KIND", "FRESHNESS", "LAG", "BUDGET", "", "BACKLOG", "DRIFT", "CHECKS", "STATUS"],
        o.projections.map((p) => {
          const ratio = lagRatio(p);
          const backlog = p.pending_keys + p.embed_backlog;
          return [
            p.name,
            p.kind,
            p.freshness,
            p.freshness === "sync" ? s.dim("in txn") : formatMs(currentLagMs(p)),
            p.freshness === "sync" ? s.dim("—") : formatMs(p.lag_budget_ms),
            bar(ratio, s),
            String(backlog),
            p.drift > 0 ? s.red(String(p.drift)) : String(p.drift),
            String(p.checks),
            p.status === "active" ? p.status : s.yellow(p.status),
          ];
        }),
        { right: [3, 4, 6, 7, 8], s },
      ),
    );
  }
  out.push("");
  if (o.alerts.length === 0) {
    out.push(s.green("no alerts"));
  } else {
    out.push(s.bold(`${plural(o.alerts.length, "alert")}`));
    for (const a of o.alerts) out.push(`  ${s.red(a.alert)}  ${a.subject}  ${s.dim(a.detail)}`);
  }
  if (o.proposals_open > 0) out.push(s.yellow(`${plural(o.proposals_open, "proposal")} waiting (isomer proposals)`));
  if (o.nodes.length > 1) {
    out.push("");
    out.push(
      table(
        ["NODE", "ROLE", "ACTIVE", "SHARDS"],
        o.nodes.map((n) => [`${n.host}:${n.port}`, n.role, n.active ? "yes" : s.red("no"), n.has_shards ? String(n.placements) : s.dim("none")]),
        { right: [3], s },
      ),
    );
  }
  return out.join("\n");
}

export async function main(argv: string[], write: (s: string) => void = (s) => process.stdout.write(`${s}\n`)): Promise<number> {
  const { values, positionals } = parseArgs({
    args: argv,
    allowPositionals: true,
    options: {
      url: { type: "string" },
      key: { type: "string" },
      json: { type: "boolean", default: false },
      interval: { type: "string" },
      sample: { type: "string" },
      name: { type: "string" },
      sync: { type: "boolean", default: false },
      database: { type: "string" },
      after: { type: "string" },
      "as-of": { type: "string" },
      subject: { type: "string" },
      limit: { type: "string" },
      help: { type: "boolean", short: "h", default: false },
    },
  });
  const [command, ...args] = positionals;
  if (values.help || !command) {
    write(USAGE);
    return command || values.help ? 0 : 2;
  }

  if (command === "login") {
    const url = need(values.url, "--url");
    const key = need(values.key, "--key");
    const client = new IsomerClient({ url, key });
    await client.overview(); // fails now, with a reason, if the key can't use the admin API
    mkdirSync(dirname(CREDENTIALS), { recursive: true });
    writeFileSync(CREDENTIALS, JSON.stringify({ url, key }, null, 2), { mode: 0o600 });
    chmodSync(CREDENTIALS, 0o600);
    write(`saved to ${CREDENTIALS}`);
    return 0;
  }

  const file = saved();
  const url = values.url ?? process.env.ISOMER_URL ?? file.url ?? "http://localhost:8080";
  const key = values.key ?? process.env.ISOMER_KEY ?? file.key;
  if (!key) throw new UsageError("no API key: pass --key, set ISOMER_KEY, or run isomer login");
  const client = new IsomerClient({ url, key });
  const s = style(!values.json && process.stdout.isTTY === true && !process.env.NO_COLOR);
  const json = (v: unknown) => write(JSON.stringify(v, null, 2));

  switch (command) {
    case "status": {
      const o = await client.overview();
      if (values.json) json(o);
      else write(renderStatus(o, s));
      return o.alerts.length > 0 ? 1 : 0;
    }
    case "watch": {
      const every = Math.max(0.5, Number(values.interval ?? "2")) * 1000;
      for (;;) {
        const o = await client.overview();
        write(`\u001b[2J\u001b[H${renderStatus(o, s)}\n\n${s.dim(`every ${every / 1000}s · ctrl-c to stop`)}`);
        await sleep(every);
      }
    }
    case "projection": {
      const d = await client.projection(need(args[0], "projection name"));
      if (values.json) return json(d), 0;
      const st = d.status;
      write(`${s.bold(d.definition.name)}  ${d.definition.kind} · ${d.definition.freshness}`);
      if (st) {
        write(`lag ${formatMs(currentLagMs(st))} of ${formatMs(st.lag_budget_ms)} budget  ${bar(lagRatio(st), s)}  backlog ${st.pending_keys + st.embed_backlog}  breaches ${st.budget_breaches ?? 0}`);
        write(st.drift_summary);
      }
      write(`fresh through token ${d.freshness_token}`);
      write("");
      write(s.bold("query"));
      write(String(d.definition.query).trim());
      if (d.sources.length) {
        write("");
        write(table(["SOURCE", "KEY COLUMN"], d.sources.map((x) => [x.table, x.key_column ?? s.dim("(reference: any change re-derives all)")]), { s }));
      }
      write("");
      write(s.bold("certificate"));
      write(JSON.stringify(d.certificate, null, 2));
      if (d.routes.length) {
        write("");
        write(table(["ROUTED QUERY", "SPEEDUP", "ENABLED"], d.routes.map((r) => [r.source_query.replace(/\s+/g, " "), r.measured_speedup ? `${r.measured_speedup.toFixed(1)}×` : "—", r.enabled ? "yes" : "no"]), { right: [1], s }));
      }
      if (d.events.length) {
        write("");
        write(table(["AT", "LEVEL", "MESSAGE"], d.events.slice(0, 10).map((e) => [e.at.slice(0, 19).replace("T", " "), e.level, e.message]), { s, max: 100 }));
      }
      return 0;
    }
    case "get": {
      const doc = await client.doc(need(args[0], "projection name"), need(args[1], "key"), {
        after: values.after,
        asOf: values["as-of"],
      });
      json(doc);
      return 0;
    }
    case "drift": {
      const o = await client.overview();
      const rows = o.projections.map((p) => ({ projection: p.name, drift: p.drift, checks: p.checks, since: p.drift_since, summary: p.drift_summary }));
      if (values.json) json(rows);
      else write(table(["PROJECTION", "DRIFT", "CHECKS", "SUMMARY"], rows.map((r) => [r.projection, r.drift > 0 ? s.red(String(r.drift)) : String(r.drift), String(r.checks), r.summary]), { right: [1, 2], s, max: 80 }));
      return rows.some((r) => r.drift > 0) ? 1 : 0;
    }
    case "alerts": {
      const { alerts } = await client.overview();
      if (values.json) json(alerts);
      else write(alerts.length ? table(["ALERT", "SUBJECT", "DETAIL"], alerts.map((a) => [a.alert, a.subject, a.detail]), { s, max: 100 }) : s.green("no alerts"));
      return alerts.length ? 1 : 0;
    }
    case "attachments": {
      const { attachments } = await client.attachments();
      if (values.json) return json(attachments), 0;
      write(attachments.length
        ? table(["ATTACHMENT", "STATUS", "SLOT", "WAL", "APPLY LAG", "TABLES", "ERRORS"], attachments.map((a) => [
            a.attachment, a.status, a.slot_active ? "active" : s.yellow("inactive"), a.wal_status ?? "—", formatMs(a.apply_lag_ms),
            `${a.tables_ready ?? 0}/${a.tables_total ?? 0}`, String((a.apply_errors ?? 0) + (a.sync_errors ?? 0))]), { right: [4, 6], s })
        : s.dim("no attachments (isomer.attach)"));
      return 0;
    }
    case "advice": {
      const { advice } = await client.advice();
      if (values.json) return json(advice), 0;
      write(advice.length ? table(["ADVICE", "SUBJECT", "RECOMMENDATION", "DETAIL"], advice.map((a) => [a.advice, a.subject, a.recommendation, a.detail]), { s, max: 70 }) : s.green("nothing to advise"));
      return 0;
    }
    case "audit": {
      const r = await client.audit(values.sample ? int(values.sample, "--sample") : undefined);
      if (values.json) return json(r), r.drift_found === 0 ? 0 : 1;
      write(`${r.drift_found === 0 ? s.green("no drift") : s.red(`${plural(r.drift_found, "row")} drifted`)}`);
      write(table(["PROJECTION", "DRIFT", "CHECKS"], r.report.map((d) => [d.projection, String(d.drift), String(d.checks)]), { right: [1, 2], s }));
      return r.drift_found === 0 ? 0 : 1;
    }
    case "repair": {
      const r = await client.repair(need(args[0], "projection name"));
      if (values.json) return json(r), 0;
      write(`repaired ${plural(r.repaired, "drifted key")} of ${r.projection}`);
      return 0;
    }
    case "proposals": {
      const { proposals } = await client.proposals();
      if (values.json) return json(proposals), 0;
      write(proposals.length
        ? table(["ID", "STATUS", "CALLS", "MEAN", "SPEEDUP", "SAVES", "COSTS", "QUERY"], proposals.map((p) => [
            String(p.id), p.status === "proposed" ? s.yellow(p.status) : p.status, String(p.calls ?? "—"),
            formatMs(p.mean_ms), p.est_speedup ? `${p.est_speedup.toFixed(1)}×` : "—",
            p.benefit_ms_per_s === null ? "—" : `${p.benefit_ms_per_s.toFixed(1)}ms/s`,
            p.cost_ms_per_s === null ? "—" : `${p.cost_ms_per_s.toFixed(1)}ms/s`,
            p.source_query.replace(/\s+/g, " ")]), { right: [0, 2, 3, 4, 5, 6], s, max: 70 })
        : s.dim("no proposals (isomer propose mines the query log)"));
      return 0;
    }
    case "propose": {
      const r = await client.propose();
      if (values.json) return json(r), 0;
      write(`${plural(r.proposed, "new proposal")}`);
      return 0;
    }
    case "accept": {
      const r = await client.acceptProposal(int(args[0], "proposal id"), {
        name: values.name,
        freshness: values.sync ? "sync" : "bounded",
        database: values.database,
      });
      if (values.json) return json(r), 0;
      write(r.result);
      return 0;
    }
    case "reject": {
      const r = await client.rejectProposal(int(args[0], "proposal id"));
      if (values.json) return json(r), 0;
      write(`proposal ${r.id} rejected`);
      return 0;
    }
    case "route": {
      const r = await client.routeProposal(int(args[0], "proposal id"));
      if (values.json) return json(r), 0;
      write(r.result);
      return 0;
    }
    case "routes": {
      const { routes } = await client.routes();
      if (values.json) return json(routes), 0;
      write(routes.length
        ? table(["QUERYID", "PROJECTION", "SPEEDUP", "KEYS", "ENABLED", "QUERY"], routes.map((r) => [
            r.queryid, r.projection ?? "", r.measured_speedup ? `${r.measured_speedup.toFixed(1)}×` : "—", String(r.verified_keys),
            r.enabled ? "yes" : s.dim("no"), r.source_query.replace(/\s+/g, " ")]), { right: [2, 3], s, max: 70 })
        : s.dim("no routes (isomer route <proposal id>)"));
      return 0;
    }
    case "enable-route":
    case "disable-route": {
      const r = await client.setRoute(need(args[0], "queryid"), command === "enable-route");
      if (values.json) return json(r), 0;
      write(`route ${r.queryid} ${r.enabled ? "enabled" : "disabled"}`);
      return 0;
    }
    case "events": {
      const { events } = await client.events({ subject: values.subject, limit: values.limit ? int(values.limit, "--limit") : 30 });
      if (values.json) return json(events), 0;
      write(events.length ? table(["AT", "LEVEL", "SUBJECT", "MESSAGE"], events.map((e) => [
        e.at.slice(0, 19).replace("T", " "), e.level === "error" ? s.red(e.level) : e.level === "warning" ? s.yellow(e.level) : e.level,
        e.subject, e.message]), { s, max: 100 }) : s.dim("no events"));
      return 0;
    }
    default:
      throw new UsageError(`unknown command ${command}`);
  }
}

if (import.meta.url === `file://${process.argv[1]}` || process.argv[1]?.endsWith("isomer.mjs") || process.argv[1]?.endsWith("/isomer")) {
  main(process.argv.slice(2)).then(
    (code) => process.exit(code),
    (e: unknown) => {
      if (e instanceof UsageError) {
        process.stderr.write(`isomer: ${e.message}\n\n${USAGE}\n`);
        process.exit(2);
      }
      if (e instanceof IsomerError) {
        process.stderr.write(`isomer: ${e.message}${e.code ? ` (${e.code})` : ""}\n`);
        process.exit(e.status === 503 ? 75 : 1);
      }
      process.stderr.write(`isomer: ${e instanceof Error ? e.message : String(e)}\n`);
      process.exit(1);
    },
  );
}
