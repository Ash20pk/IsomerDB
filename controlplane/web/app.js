// IsomerDB Cloud: sign up, log in, and run clusters. Everything here goes through the public
// API (/v1/...) with the caller's token, which lives in sessionStorage and is revoked on log out.

const app = document.getElementById("app");
const TOKEN = "isomer.token";

const store = {
  get() { try { return sessionStorage.getItem(TOKEN); } catch { return null; } },
  set(t) { try { sessionStorage.setItem(TOKEN, t); } catch {} },
  clear() { try { sessionStorage.removeItem(TOKEN); } catch {} },
};

// h(tag, props, ...children): elements built with text nodes only, never HTML from the server.
function h(tag, props = {}, ...children) {
  const el = document.createElement(tag);
  for (const [k, v] of Object.entries(props || {})) {
    if (v == null || v === false) continue;
    if (k.startsWith("on")) el.addEventListener(k.slice(2), v);
    else if (k === "class") el.className = v;
    else el.setAttribute(k, v === true ? "" : v);
  }
  for (const c of children.flat()) if (c != null && c !== false) el.append(c instanceof Node ? c : String(c));
  return el;
}

class ApiError extends Error {
  constructor(status, message) { super(message); this.status = status; }
}

async function api(method, path, body) {
  const headers = { "Content-Type": "application/json" };
  const token = store.get();
  if (token) headers.Authorization = `Bearer ${token}`;
  const res = await fetch(path, { method, headers, body: body ? JSON.stringify(body) : undefined });
  if (res.status === 401 && token) {
    store.clear();
    render();
    throw new ApiError(401, "Your session has ended. Log in again.");
  }
  const text = await res.text();
  const data = text ? JSON.parse(text) : null;
  if (!res.ok) throw new ApiError(res.status, (data && data.error) || `${res.status} ${res.statusText}`);
  return data;
}

// ── signed out ────────────────────────────────────────────────────────────────

function gate(mode) {
  const signup = mode === "signup";
  const error = h("p", { class: "err", role: "alert" });
  const fields = signup
    ? [
        h("label", {}, "Organisation", h("input", { name: "organization", required: true, maxlength: 100, autocomplete: "organization" })),
        h("label", {}, "Work email", h("input", { name: "email", type: "email", required: true, autocomplete: "email" })),
        h("label", {}, "Password (at least 12 characters)", h("input", { name: "password", type: "password", required: true, minlength: 12, autocomplete: "new-password" })),
      ]
    : [
        h("label", {}, "Email", h("input", { name: "email", type: "email", required: true, autocomplete: "email" })),
        h("label", {}, "Password", h("input", { name: "password", type: "password", required: true, autocomplete: "current-password" })),
      ];
  const submit = h("button", { class: "primary", type: "submit" }, signup ? "Create account" : "Log in");
  const form = h("form", {
    async onsubmit(e) {
      e.preventDefault();
      error.textContent = "";
      submit.disabled = true;
      const f = Object.fromEntries(new FormData(form));
      try {
        const out = await api("POST", signup ? "/v1/signup" : "/v1/tokens", f);
        store.set(out.token);
        render();
      } catch (err) {
        error.textContent = err.status === 401 ? "That email and password don't match an account." : err.message;
        submit.disabled = false;
      }
    },
  }, fields, error, submit);
  return h("div", { class: "gate" },
    h("div", { class: "card" },
      h("div", { class: "mark" }, "Isomer", h("span", {}, "DB"), " Cloud"),
      h("h1", {}, signup ? "Create your account" : "Log in"),
      h("p", {}, signup
        ? "Postgres with projections for documents, search and vectors, kept in step with your tables."
        : "Welcome back."),
      form,
      h("div", { class: "switch" },
        signup ? "Already have an account? " : "New here? ",
        h("button", { class: "link", type: "button", onclick: () => { location.hash = signup ? "login" : "signup"; } },
          signup ? "Log in" : "Create an account")),
    ));
}

// ── signed in ─────────────────────────────────────────────────────────────────

const usd = (x) => x.toLocaleString(undefined, { style: "currency", currency: "USD", minimumFractionDigits: 2, maximumFractionDigits: 4 });

let poll = null;

async function dashboard() {
  const [me, regions, plans] = await Promise.all([api("GET", "/v1/me"), api("GET", "/v1/regions"), api("GET", "/v1/plans")]);
  const clusters = h("div");
  const bill = h("div");
  const notice = h("p", { class: "err", role: "alert" });

  const say = (err) => { notice.textContent = err ? err.message || String(err) : ""; };

  async function refresh() {
    try {
      const list = await api("GET", "/v1/clusters");
      clusters.replaceChildren(clusterTable(list, plans, refresh, say));
      clearTimeout(poll);
      if (list.some((c) => c.phase !== "Ready")) poll = setTimeout(refresh, 5000);
    } catch (err) { say(err); }
  }

  async function refreshBill() {
    const month = new Date().toISOString().slice(0, 7);
    try { bill.replaceChildren(invoiceTable(await api("GET", `/v1/invoices/${month}`))); } catch (err) { say(err); }
  }

  const logout = async () => {
    clearTimeout(poll);
    try { await api("DELETE", "/v1/tokens/current"); } catch {}
    store.clear();
    location.hash = "login";
    render();
  };

  app.replaceChildren(
    h("header", {},
      h("div", { class: "mark" }, "Isomer", h("span", {}, "DB"), " Cloud"),
      h("div", { class: "who" }, `${me.organization} · ${me.email}`),
      h("button", { onclick: logout }, "Log out")),
    h("main", {},
      notice,
      h("h2", {}, "New cluster"),
      createForm(regions, plans, refresh, say),
      h("h2", {}, "Clusters"),
      h("div", { class: "panel" }, clusters),
      h("h2", {}, "This month"),
      h("div", { class: "panel" }, bill),
      h("p", { class: "quiet" }, "Usage is collected from each cluster every few minutes. Invoices are computed here; no card is charged.")));
  await Promise.all([refresh(), refreshBill()]);
}

function planNote(p) {
  const nodes = 1 + p.workers;
  return `${p.workers === 0 ? "One node" : `A coordinator and ${p.workers} workers`} (up to ${p.maxWorkers} workers)` +
    `${p.standbys ? ", each with a standby and failover" : ""}; ${p.cpu} CPU, ${p.memory} memory and ${p.storage} storage per node. ` +
    `${usd(p.nodeHourUSD)} per node-hour (${usd(p.nodeHourUSD * nodes * (1 + p.standbys) * 730)} a month as created) ` +
    `and ${usd(p.storageGBMonthUSD)} per GB-month.`;
}

function createForm(regions, plans, refresh, say) {
  const name = h("input", { name: "name", required: true, pattern: "[a-z][a-z0-9\\-]{0,23}", maxlength: 24, placeholder: "app", title: "Lowercase letters, digits and hyphens, starting with a letter" });
  const region = h("select", { name: "region" }, regions.map((r) => h("option", { value: r.name }, r.display || r.name)));
  const plan = h("select", { name: "plan" }, plans.map((p) => h("option", { value: p.name }, p.name)));
  const note = h("p", { class: "plan-note" });
  const showPlan = () => { const p = plans.find((x) => x.name === plan.value); note.textContent = p ? planNote(p) : ""; };
  plan.addEventListener("change", showPlan);
  showPlan();
  const submit = h("button", { class: "primary", type: "submit" }, "Create");
  const form = h("form", {
    class: "panel create",
    async onsubmit(e) {
      e.preventDefault();
      say(null);
      submit.disabled = true;
      try {
        await api("POST", "/v1/clusters", { name: name.value, region: region.value, plan: plan.value });
        name.value = "";
        await refresh();
      } catch (err) { say(err); }
      submit.disabled = false;
    },
  },
  h("label", {}, "Name", name),
  h("label", {}, "Region", region),
  h("label", {}, "Plan", plan),
  submit,
  note);
  return form;
}

function clusterTable(list, plans, refresh, say) {
  if (!list.length) return h("div", { class: "empty" }, "No clusters yet.");
  const body = h("tbody");
  for (const c of list) {
    const detail = h("tr", { class: "detail", hidden: true }, h("td", { colspan: 6 }));
    const open = (content) => {
      const cell = detail.firstChild;
      if (!detail.hidden && cell.dataset.what === content.dataset.what) { detail.hidden = true; return; }
      cell.dataset.what = content.dataset.what;
      cell.replaceChildren(content);
      detail.hidden = false;
    };
    const plan = plans.find((p) => p.name === c.plan);
    body.append(
      h("tr", {},
        h("td", { class: "name" }, c.name),
        h("td", {}, c.region),
        h("td", {}, c.plan),
        h("td", {}, h("span", { class: `phase ${c.phase}` }, c.phase)),
        h("td", { class: "num" }, `${c.ready_workers}/${c.workers} workers`),
        h("td", {}, h("div", { class: "actions" },
          h("button", { onclick: () => open(connection(c, say)) }, "Connect"),
          h("button", { onclick: () => open(scale(c, plan, refresh, say)) }, "Scale"),
          h("button", { class: "danger", onclick: () => open(remove(c, refresh, say)) }, "Delete")))),
      detail);
  }
  return h("table", { class: "table" },
    h("thead", {}, h("tr", {}, ["Name", "Region", "Plan", "Status", "Workers", ""].map((t) => h("th", { class: t === "Workers" ? "num" : null }, t)))),
    body);
}

function connection(c, say) {
  const box = h("div", { class: "grid", "data-what": "connect" }, h("span", { class: "quiet" }, "Loading credentials…"));
  if (c.phase !== "Ready") {
    box.replaceChildren(h("span", { class: "quiet" }, "Credentials are available once the cluster is ready."));
    return box;
  }
  api("GET", `/v1/clusters/${encodeURIComponent(c.name)}/credentials`).then((cr) => {
    const rows = [h("label", {}, "Postgres", h("pre", {}, cr.postgres_url))];
    if (cr.mongodb_url) rows.push(h("label", {}, "MongoDB", h("pre", {}, cr.mongodb_url)));
    if (c.api) rows.push(h("label", {}, "HTTP API", h("pre", {}, `https://${c.api}`)));
    box.replaceChildren(...rows);
  }, (err) => { box.replaceChildren(h("span", { class: "err" }, err.message)); say(null); });
  return box;
}

function scale(c, plan, refresh, say) {
  const max = plan ? plan.maxWorkers : 0;
  const n = h("input", { type: "number", min: 0, max, value: c.workers, required: true });
  const submit = h("button", { class: "primary", type: "submit" }, "Apply");
  return h("form", {
    class: "inline", "data-what": "scale",
    async onsubmit(e) {
      e.preventDefault();
      submit.disabled = true;
      try { await api("PATCH", `/v1/clusters/${encodeURIComponent(c.name)}`, { workers: Number(n.value) }); await refresh(); }
      catch (err) { say(err); submit.disabled = false; }
    },
  }, h("label", {}, `Workers (0 to ${max})`, n), submit,
  h("span", { class: "quiet" }, "Shards move online; reads and writes continue."));
}

function remove(c, refresh, say) {
  const confirm = h("input", { placeholder: c.name, autocomplete: "off", "aria-label": `Type ${c.name} to confirm` });
  const submit = h("button", { class: "danger", type: "submit", disabled: true }, "Delete cluster");
  confirm.addEventListener("input", () => { submit.disabled = confirm.value !== c.name; });
  return h("form", {
    class: "inline", "data-what": "delete",
    async onsubmit(e) {
      e.preventDefault();
      if (confirm.value !== c.name) return;
      submit.disabled = true;
      try { await api("DELETE", `/v1/clusters/${encodeURIComponent(c.name)}`); await refresh(); }
      catch (err) { say(err); submit.disabled = false; }
    },
  }, h("span", {}, `This deletes ${c.name} and its data. Usage up to now stays on the invoice. Type its name to confirm:`), confirm, submit);
}

function invoiceTable(inv) {
  if (!inv.lines.length) return h("div", { class: "empty" }, "No usage this month yet.");
  return h("table", { class: "table" },
    h("thead", {}, h("tr", {}, ["Cluster", "Item", "Quantity", "Unit price", "Amount"].map((t, i) => h("th", { class: i >= 2 ? "num" : null }, t)))),
    h("tbody", {},
      inv.lines.map((l) => h("tr", {},
        h("td", { class: "name" }, l.cluster),
        h("td", {}, l.item, l.description ? h("div", { class: "quiet" }, l.description) : null),
        h("td", { class: "num" }, `${l.quantity} ${l.unit}`),
        h("td", { class: "num" }, usd(l.unit_price_usd)),
        h("td", { class: "num" }, usd(l.amount_usd)))),
      h("tr", { class: "total" }, h("td", { colspan: 4 }, `Total for ${inv.month}`), h("td", { class: "num" }, usd(inv.total_usd)))));
}

// ── routing ───────────────────────────────────────────────────────────────────

async function render() {
  clearTimeout(poll);
  if (!store.get()) {
    app.replaceChildren(gate(location.hash === "#login" ? "login" : "signup"));
    return;
  }
  try {
    await dashboard();
  } catch (err) {
    if (err.status !== 401) app.replaceChildren(h("main", {}, h("p", { class: "err" }, err.message)));
  }
}

addEventListener("hashchange", () => { if (!store.get()) render(); });
render();
