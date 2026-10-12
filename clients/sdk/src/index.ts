// A typed client for the IsomerDB HTTP API: projections (any key) and the admin endpoints the
// console and CLI use (keys whose role is a superuser or a member of isomer_admin).

export interface ProjectionStatus {
  name: string;
  kind: "document" | "search" | "vector" | "cache";
  freshness: "sync" | "bounded";
  status: string;
  lag_budget: string;
  lag_budget_ms: number | null;
  pending_keys: number;
  oldest_pending_ms: number | null;
  last_lag_ms: number | null;
  max_lag_ms: number | null;
  budget_breaches: number | null;
  documents_written: number | null;
  documents_deleted: number | null;
  search_staleness_ms: number | null;
  search_errors: number | null;
  model: string | null;
  pending_model: string | null;
  embed_backlog: number;
  oldest_embed_job_ms: number | null;
  embed_dead_letters: number;
  embed_cost_this_month: number | null;
  monthly_cap_usd: number | null;
  drift: number;
  checks: number;
  drift_since: string | null;
  drift_summary: string;
  history: string | null;
  native_view: string | null;
  target_schema: string | null;
  mongo_database: string | null;
}

export interface Alert {
  alert: string;
  subject: string;
  detail: string;
}

export interface Node {
  host: string;
  port: number;
  role: string;
  active: boolean;
  has_shards: boolean;
  placements: number;
}

export interface Overview {
  version: string;
  token: number;
  at: string;
  projections: ProjectionStatus[];
  alerts: Alert[];
  attachments: { name: string; status: string }[];
  nodes: Node[];
  proposals_open: number;
  routes_enabled: number;
}

export interface Route {
  queryid: string;
  proposal?: number | null;
  projection?: string;
  source_query: string;
  measured_speedup: number | null;
  verified_keys: number;
  enabled: boolean;
  created_at: string;
}

export interface IsomerEvent {
  id: number;
  at: string;
  level: "info" | "warning" | "error";
  subject: string;
  message: string;
}

export interface ProjectionDetail {
  definition: Record<string, unknown> & { name: string; kind: string; query: string; freshness: string };
  status: ProjectionStatus | null;
  freshness_token: number;
  sources: { table: string; key_column: string | null }[];
  certificate: Record<string, unknown>;
  routes: Route[];
  events: IsomerEvent[];
}

export interface Proposal {
  id: number;
  queryid: number | null;
  source_query: string;
  calls: number | null;
  mean_ms: number | null;
  est_speedup: number | null;
  benefit_ms_per_s: number | null;
  cost_ms_per_s: number | null;
  draft: Record<string, unknown> | null;
  problems: string[];
  status: "proposed" | "needs_review" | "accepted" | "auto_accepted" | "rejected";
  projection: string | null;
  created_at: string;
  decided_at: string | null;
}

export interface Advice {
  advice: string;
  subject: string;
  recommendation: string;
  detail: string;
}

export interface AttachmentStatus {
  attachment: string;
  status: string;
  slot_active: boolean | null;
  wal_status: string | null;
  retained_wal_bytes: number | null;
  safe_wal_bytes: number | null;
  apply_lag: string | null;
  apply_lag_ms: number | null;
  tables_ready: number | null;
  tables_total: number | null;
  apply_errors: number | null;
  sync_errors: number | null;
}

export interface DriftRow {
  projection: string;
  kind: string;
  freshness: string;
  drift: number;
  checks: number;
  since: string | null;
  last_run: string | null;
  summary: string;
}

export interface UsageRow {
  period: string;
  metric: string;
  dimension: string;
  unit: string;
  value: number;
}

export interface UsageOptions {
  from?: string;
  to?: string;
  by?: "hour" | "day" | "month";
}

/** An error the API answered with, carrying its HTTP status and IsomerDB SQLSTATE (e.g. IS001). */
export class IsomerError extends Error {
  readonly status: number;
  readonly code: string | null;

  constructor(message: string, status: number, code: string | null) {
    super(message);
    this.name = "IsomerError";
    this.status = status;
    this.code = code;
  }
}

export interface ClientOptions {
  /** Base URL of the API, e.g. http://localhost:8080 (empty for same-origin in the console). */
  url: string;
  /** An API key from isomer.create_api_key(name, role). */
  key: string;
  fetch?: typeof fetch;
}

export class IsomerClient {
  private readonly url: string;
  private readonly key: string;
  private readonly doFetch: typeof fetch;

  constructor(opts: ClientOptions) {
    this.url = opts.url.replace(/\/+$/, "");
    this.key = opts.key;
    this.doFetch = opts.fetch ?? globalThis.fetch.bind(globalThis);
  }

  private async request<T>(method: string, path: string, body?: unknown, headers: Record<string, string> = {}): Promise<T> {
    const res = await this.doFetch(this.url + path, {
      method,
      headers: {
        authorization: `Bearer ${this.key}`,
        ...(body === undefined ? {} : { "content-type": "application/json" }),
        ...headers,
      },
      body: body === undefined ? undefined : JSON.stringify(body),
    });
    const text = await res.text();
    let parsed: unknown = undefined;
    try {
      parsed = text ? JSON.parse(text) : undefined;
    } catch {
      parsed = undefined;
    }
    if (!res.ok) {
      const err = (parsed ?? {}) as { error?: string; code?: string | null };
      throw new IsomerError(err.error ?? (text || res.statusText), res.status, err.code ?? null);
    }
    return parsed as T;
  }

  // ── projections ─────────────────────────────────────────────────────────────

  projections(): Promise<{ projections: { name: string; kind: string; freshness: string; history: string | null }[] }> {
    return this.request("GET", "/v1/projections");
  }

  /** A document by key; `after` is a write token to wait for (read your writes). */
  doc(projection: string, key: string, opts: { asOf?: string; after?: number | string } = {}): Promise<unknown> {
    const q = opts.asOf ? `?as_of=${encodeURIComponent(opts.asOf)}` : "";
    const headers: Record<string, string> = opts.after === undefined ? {} : { "x-isomer-after": String(opts.after) };
    return this.request("GET", `/v1/projections/${enc(projection)}/docs/${enc(key)}${q}`, undefined, headers);
  }

  // ── admin ───────────────────────────────────────────────────────────────────

  overview(): Promise<Overview> {
    return this.request("GET", "/v1/admin/overview");
  }

  projection(name: string): Promise<ProjectionDetail> {
    return this.request("GET", `/v1/admin/projections/${enc(name)}`);
  }

  repair(name: string): Promise<{ projection: string; repaired: number }> {
    return this.request("POST", `/v1/admin/projections/${enc(name)}/repair`);
  }

  audit(sample?: number): Promise<{ drift_found: number; report: DriftRow[] }> {
    return this.request("POST", "/v1/admin/audit", sample === undefined ? {} : { sample });
  }

  proposals(): Promise<{ proposals: Proposal[] }> {
    return this.request("GET", "/v1/admin/proposals");
  }

  propose(): Promise<{ proposed: number }> {
    return this.request("POST", "/v1/admin/propose");
  }

  acceptProposal(id: number, opts: { name?: string; freshness?: "sync" | "bounded"; database?: string } = {}): Promise<{ id: number; result: string }> {
    return this.request("POST", `/v1/admin/proposals/${id}/accept`, opts);
  }

  rejectProposal(id: number): Promise<{ id: number; status: string }> {
    return this.request("POST", `/v1/admin/proposals/${id}/reject`);
  }

  routeProposal(id: number): Promise<{ id: number; result: string }> {
    return this.request("POST", `/v1/admin/proposals/${id}/route`);
  }

  routes(): Promise<{ routes: Route[] }> {
    return this.request("GET", "/v1/admin/routes");
  }

  setRoute(queryid: string, enabled: boolean): Promise<{ queryid: string; enabled: boolean }> {
    return this.request("POST", `/v1/admin/routes/${enc(queryid)}`, { enabled });
  }

  advice(): Promise<{ advice: Advice[] }> {
    return this.request("GET", "/v1/admin/advice");
  }

  attachments(): Promise<{ attachments: AttachmentStatus[] }> {
    return this.request("GET", "/v1/admin/attachments");
  }

  /** Metered usage (storage, node-hours, documents written, embeddings, statements, API requests). */
  usage(opts: UsageOptions = {}): Promise<{ by: string; usage: UsageRow[] }> {
    return this.request("GET", `/v1/admin/usage${usageQuery(opts)}`);
  }

  /** The same, as CSV for a billing system. */
  async usageCsv(opts: UsageOptions = {}): Promise<string> {
    const res = await this.doFetch(`${this.url}/v1/admin/usage${usageQuery({ ...opts }, "csv")}`, {
      headers: { authorization: `Bearer ${this.key}` },
    });
    const text = await res.text();
    if (!res.ok) {
      let err: { error?: string; code?: string | null } = {};
      try {
        err = JSON.parse(text) as typeof err;
      } catch {
        // not JSON
      }
      throw new IsomerError(err.error ?? (text || res.statusText), res.status, err.code ?? null);
    }
    return text;
  }

  events(opts: { after?: number; limit?: number; subject?: string } = {}): Promise<{ events: IsomerEvent[] }> {
    const q = new URLSearchParams();
    if (opts.after !== undefined) q.set("after", String(opts.after));
    if (opts.limit !== undefined) q.set("limit", String(opts.limit));
    if (opts.subject !== undefined) q.set("subject", opts.subject);
    const qs = q.toString();
    return this.request("GET", `/v1/admin/events${qs ? `?${qs}` : ""}`);
  }
}

function usageQuery(opts: UsageOptions, format?: string): string {
  const q = new URLSearchParams();
  if (opts.from) q.set("from", opts.from);
  if (opts.to) q.set("to", opts.to);
  if (opts.by) q.set("by", opts.by);
  if (format) q.set("format", format);
  const qs = q.toString();
  return qs ? `?${qs}` : "";
}

function enc(s: string): string {
  return encodeURIComponent(s);
}

/** The lag a projection is behind right now, in milliseconds (0 when nothing is pending). */
export function currentLagMs(p: ProjectionStatus): number {
  if (p.freshness === "sync") return 0;
  if (p.kind === "search") return p.search_staleness_ms ?? 0;
  if (p.kind === "vector") return Math.max(p.oldest_pending_ms ?? 0, p.oldest_embed_job_ms ?? 0);
  return p.oldest_pending_ms ?? 0;
}

/** Lag as a fraction of the projection's budget (0 = current, 1 = at budget), or null if unknown. */
export function lagRatio(p: ProjectionStatus): number | null {
  if (p.freshness === "sync") return 0;
  if (!p.lag_budget_ms) return null;
  return currentLagMs(p) / p.lag_budget_ms;
}

/** "1.2s", "350ms", "4m 10s" */
export function formatMs(ms: number | null | undefined): string {
  if (ms === null || ms === undefined || Number.isNaN(ms)) return "—";
  if (ms < 1000) return `${Math.round(ms)}ms`;
  if (ms < 60_000) return `${(ms / 1000).toFixed(ms < 10_000 ? 1 : 0)}s`;
  const m = Math.floor(ms / 60_000);
  const s = Math.round((ms % 60_000) / 1000);
  if (m < 60) return s ? `${m}m ${s}s` : `${m}m`;
  const h = Math.floor(m / 60);
  return `${h}h ${m % 60}m`;
}
