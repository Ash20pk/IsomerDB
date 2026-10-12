package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/mail"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

type api struct {
	cfg        Config
	store      *Store
	regions    regions
	collector  *collector
	adminToken string
	log        *slog.Logger
}

func newAPI(cfg Config, store *Store, rs regions, col *collector, adminToken string, log *slog.Logger) http.Handler {
	a := &api{cfg: cfg, store: store, regions: rs, collector: col, adminToken: adminToken, log: log}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	mux.HandleFunc("POST /v1/signup", a.signup)
	mux.HandleFunc("POST /v1/tokens", a.login)
	mux.HandleFunc("GET /v1/regions", a.listRegions)
	mux.HandleFunc("GET /v1/plans", a.listPlans)
	mux.HandleFunc("GET /v1/me", a.authed(a.me))
	mux.HandleFunc("POST /v1/clusters", a.authed(a.createCluster))
	mux.HandleFunc("GET /v1/clusters", a.authed(a.listClusters))
	mux.HandleFunc("GET /v1/clusters/{name}", a.authed(a.getCluster))
	mux.HandleFunc("PATCH /v1/clusters/{name}", a.authed(a.scaleCluster))
	mux.HandleFunc("DELETE /v1/clusters/{name}", a.authed(a.deleteCluster))
	mux.HandleFunc("GET /v1/clusters/{name}/credentials", a.authed(a.credentials))
	mux.HandleFunc("GET /v1/usage", a.authed(a.usage))
	mux.HandleFunc("GET /v1/invoices/{month}", a.authed(a.invoice))
	mux.HandleFunc("POST /v1/admin/collect", a.admin(a.collectNow))
	return mux
}

// ── plumbing ──────────────────────────────────────────────────────────────────

func reply(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func problem(w http.ResponseWriter, status int, format string, args ...any) {
	reply(w, status, map[string]string{"error": fmt.Sprintf(format, args...)})
}

func (a *api) fail(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, errNotFound), apierrors.IsNotFound(err):
		problem(w, http.StatusNotFound, "%v", err)
	case errors.Is(err, errExists), apierrors.IsAlreadyExists(err):
		problem(w, http.StatusConflict, "%v", err)
	case errors.Is(err, errAuth):
		problem(w, http.StatusUnauthorized, "%v", err)
	default:
		a.log.Error("request failed", "err", err)
		problem(w, http.StatusInternalServerError, "internal error")
	}
}

func decode(r *http.Request, v any) error {
	dec := json.NewDecoder(http.MaxBytesReader(nil, r.Body, 1<<16))
	dec.DisallowUnknownFields()
	return dec.Decode(v)
}

type handler func(w http.ResponseWriter, r *http.Request, c Caller)

func bearer(r *http.Request) string {
	h := r.Header.Get("Authorization")
	if !strings.HasPrefix(h, "Bearer ") {
		return ""
	}
	return strings.TrimPrefix(h, "Bearer ")
}

func (a *api) authed(h handler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		token := bearer(r)
		if token == "" {
			problem(w, http.StatusUnauthorized, "an API token is required (Authorization: Bearer <token>)")
			return
		}
		c, err := a.store.Authenticate(r.Context(), token)
		if err != nil {
			if errors.Is(err, errAuth) {
				problem(w, http.StatusUnauthorized, "unknown API token")
				return
			}
			a.fail(w, err)
			return
		}
		h(w, r, c)
	}
}

func (a *api) admin(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if a.adminToken == "" || bearer(r) != a.adminToken {
			problem(w, http.StatusForbidden, "admin only")
			return
		}
		h(w, r)
	}
}

// ── accounts ──────────────────────────────────────────────────────────────────

func (a *api) signup(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Organization string `json:"organization"`
		Email        string `json:"email"`
		Password     string `json:"password"`
	}
	if err := decode(r, &req); err != nil {
		problem(w, http.StatusBadRequest, "body: %v", err)
		return
	}
	if _, err := mail.ParseAddress(req.Email); err != nil {
		problem(w, http.StatusBadRequest, "email: %v", err)
		return
	}
	if len(req.Password) < 12 {
		problem(w, http.StatusBadRequest, "password: at least 12 characters")
		return
	}
	if strings.TrimSpace(req.Organization) == "" || len(req.Organization) > 100 {
		problem(w, http.StatusBadRequest, "organization: 1 to 100 characters")
		return
	}
	token, err := a.store.Signup(r.Context(), req.Organization, strings.ToLower(req.Email), req.Password)
	if err != nil {
		a.fail(w, err)
		return
	}
	reply(w, http.StatusCreated, map[string]string{"token": token, "organization": req.Organization})
}

func (a *api) login(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Email    string `json:"email"`
		Password string `json:"password"`
	}
	if err := decode(r, &req); err != nil {
		problem(w, http.StatusBadRequest, "body: %v", err)
		return
	}
	token, err := a.store.Login(r.Context(), strings.ToLower(req.Email), req.Password)
	if err != nil {
		a.fail(w, err)
		return
	}
	reply(w, http.StatusCreated, map[string]string{"token": token})
}

func (a *api) me(w http.ResponseWriter, _ *http.Request, c Caller) {
	reply(w, http.StatusOK, map[string]string{"organization": c.Org, "email": c.Email})
}

func (a *api) listRegions(w http.ResponseWriter, _ *http.Request) {
	out := []map[string]string{}
	for _, r := range a.cfg.Regions {
		out = append(out, map[string]string{"name": r.Name, "display": r.Display})
	}
	reply(w, http.StatusOK, out)
}

func (a *api) listPlans(w http.ResponseWriter, _ *http.Request) {
	reply(w, http.StatusOK, a.cfg.Plans)
}

// ── clusters ──────────────────────────────────────────────────────────────────

// ClusterView is a cluster as its owner sees it: what was asked for, and what is running.
type ClusterView struct {
	Name          string    `json:"name"`
	Region        string    `json:"region"`
	Plan          string    `json:"plan"`
	CreatedAt     time.Time `json:"created_at"`
	Phase         string    `json:"phase"`
	Workers       int32     `json:"workers"`
	ReadyWorkers  int32     `json:"ready_workers"`
	Standbys      int32     `json:"standbys"`
	IsomerVersion string    `json:"isomer_version,omitempty"`
	Postgres      string    `json:"postgres,omitempty"`
	MongoDB       string    `json:"mongodb,omitempty"`
	API           string    `json:"api,omitempty"`
}

func (a *api) view(ctx context.Context, row ClusterRow) (ClusterView, error) {
	v := ClusterView{Name: row.Name, Region: row.Region, Plan: row.Plan, CreatedAt: row.CreatedAt, Phase: "Provisioning"}
	k8s, ok := a.regions[row.Region]
	if !ok {
		return v, fmt.Errorf("unknown region %s", row.Region)
	}
	ic, err := getCluster(ctx, k8s, row.Namespace, row.Name)
	if err != nil {
		return v, err
	}
	v.Workers, v.ReadyWorkers, v.Standbys = ic.Spec.Workers, ic.Status.ReadyWorkers, ic.Spec.HA.Standbys
	if ic.Status.Phase != "" {
		v.Phase = ic.Status.Phase
	}
	v.IsomerVersion = ic.Status.IsomerVersion
	v.Postgres, v.MongoDB, v.API = ic.Status.Endpoints.Postgres, ic.Status.Endpoints.MongoDB, ic.Status.Endpoints.API
	return v, nil
}

func (a *api) createCluster(w http.ResponseWriter, r *http.Request, c Caller) {
	var req struct {
		Name   string `json:"name"`
		Region string `json:"region"`
		Plan   string `json:"plan"`
	}
	if err := decode(r, &req); err != nil {
		problem(w, http.StatusBadRequest, "body: %v", err)
		return
	}
	if !validName.MatchString(req.Name) {
		problem(w, http.StatusBadRequest, "name: lowercase letters, digits and hyphens, starting with a letter, at most 24 characters")
		return
	}
	region, ok := a.cfg.region(req.Region)
	if !ok {
		problem(w, http.StatusBadRequest, "region: no region named %q (GET /v1/regions)", req.Region)
		return
	}
	plan, ok := a.cfg.plan(req.Plan)
	if !ok {
		problem(w, http.StatusBadRequest, "plan: no plan named %q (GET /v1/plans)", req.Plan)
		return
	}
	if err := plan.validate(); err != nil {
		a.fail(w, err)
		return
	}
	ns := namespaceFor(c.OrgID)
	row, err := a.store.CreateCluster(r.Context(), c.OrgID, req.Name, region.Name, plan.Name, ns)
	if err != nil {
		a.fail(w, err)
		return
	}
	if err := provision(r.Context(), a.regions[region.Name], a.cfg, region, plan, c.OrgID, ns, req.Name); err != nil {
		_ = a.store.DeleteCluster(r.Context(), row.ID)
		a.fail(w, err)
		return
	}
	a.log.Info("cluster created", "org", c.Org, "cluster", req.Name, "region", region.Name, "plan", plan.Name)
	v, err := a.view(r.Context(), row)
	if err != nil {
		a.fail(w, err)
		return
	}
	reply(w, http.StatusCreated, v)
}

func (a *api) listClusters(w http.ResponseWriter, r *http.Request, c Caller) {
	rows, err := a.store.Clusters(r.Context(), c.OrgID)
	if err != nil {
		a.fail(w, err)
		return
	}
	out := []ClusterView{}
	for _, row := range rows {
		v, err := a.view(r.Context(), row)
		if err != nil {
			v.Phase = "Unknown"
		}
		out = append(out, v)
	}
	reply(w, http.StatusOK, out)
}

func (a *api) clusterOf(w http.ResponseWriter, r *http.Request, c Caller) (ClusterRow, bool) {
	row, err := a.store.Cluster(r.Context(), c.OrgID, r.PathValue("name"))
	if err != nil {
		a.fail(w, err)
		return row, false
	}
	return row, true
}

func (a *api) getCluster(w http.ResponseWriter, r *http.Request, c Caller) {
	row, ok := a.clusterOf(w, r, c)
	if !ok {
		return
	}
	v, err := a.view(r.Context(), row)
	if err != nil {
		a.fail(w, err)
		return
	}
	reply(w, http.StatusOK, v)
}

func (a *api) scaleCluster(w http.ResponseWriter, r *http.Request, c Caller) {
	row, ok := a.clusterOf(w, r, c)
	if !ok {
		return
	}
	var req struct {
		Workers *int32 `json:"workers"`
	}
	if err := decode(r, &req); err != nil || req.Workers == nil {
		problem(w, http.StatusBadRequest, "body: {\"workers\": n}")
		return
	}
	plan, _ := a.cfg.plan(row.Plan)
	if *req.Workers < 0 || *req.Workers > plan.MaxWorkers {
		problem(w, http.StatusBadRequest, "workers: 0 to %d on the %s plan", plan.MaxWorkers, plan.Name)
		return
	}
	if err := scaleCluster(r.Context(), a.regions[row.Region], row.Namespace, row.Name, *req.Workers); err != nil {
		a.fail(w, err)
		return
	}
	a.log.Info("cluster scaled", "org", c.Org, "cluster", row.Name, "workers", *req.Workers)
	v, err := a.view(r.Context(), row)
	if err != nil {
		a.fail(w, err)
		return
	}
	reply(w, http.StatusOK, v)
}

func (a *api) deleteCluster(w http.ResponseWriter, r *http.Request, c Caller) {
	row, ok := a.clusterOf(w, r, c)
	if !ok {
		return
	}
	// Usage up to now is read first: it is billed even though the cluster goes.
	if err := a.collector.collect(r.Context(), row); err != nil {
		a.log.Warn("final usage collection before delete", "cluster", row.Name, "err", err)
	}
	if err := deleteCluster(r.Context(), a.regions[row.Region], row.Namespace, row.Name); err != nil {
		a.fail(w, err)
		return
	}
	if err := a.store.DeleteCluster(r.Context(), row.ID); err != nil {
		a.fail(w, err)
		return
	}
	a.log.Info("cluster deleted", "org", c.Org, "cluster", row.Name)
	w.WriteHeader(http.StatusNoContent)
}

func (a *api) credentials(w http.ResponseWriter, r *http.Request, c Caller) {
	row, ok := a.clusterOf(w, r, c)
	if !ok {
		return
	}
	k8s := a.regions[row.Region]
	ic, err := getCluster(r.Context(), k8s, row.Namespace, row.Name)
	if err != nil {
		a.fail(w, err)
		return
	}
	pw, err := password(r.Context(), k8s, ic)
	if err != nil {
		a.fail(w, err)
		return
	}
	mu, mp, err := mongoCredentials(r.Context(), k8s, ic)
	if err != nil {
		a.fail(w, err)
		return
	}
	out := map[string]string{
		"postgres_user": "postgres", "postgres_password": pw,
		"postgres_url": fmt.Sprintf("postgres://postgres:%s@%s/postgres", pw, ic.Status.Endpoints.Postgres),
	}
	if ic.Status.Endpoints.MongoDB != "" {
		out["mongodb_user"], out["mongodb_password"] = mu, mp
		out["mongodb_url"] = fmt.Sprintf("mongodb://%s:%s@%s/?tls=true", mu, mp, ic.Status.Endpoints.MongoDB)
	}
	reply(w, http.StatusOK, out)
}

// ── usage and invoices ────────────────────────────────────────────────────────

func (a *api) usage(w http.ResponseWriter, r *http.Request, c Caller) {
	to := time.Now().UTC()
	from := to.AddDate(0, 0, -1)
	var err error
	if v := r.URL.Query().Get("from"); v != "" {
		if from, err = time.Parse(time.RFC3339, v); err != nil {
			problem(w, http.StatusBadRequest, "from: %v", err)
			return
		}
	}
	if v := r.URL.Query().Get("to"); v != "" {
		if to, err = time.Parse(time.RFC3339, v); err != nil {
			problem(w, http.StatusBadRequest, "to: %v", err)
			return
		}
	}
	u, err := a.store.Usage(r.Context(), c.OrgID, from, to)
	if err != nil {
		a.fail(w, err)
		return
	}
	reply(w, http.StatusOK, map[string]any{"from": from, "to": to, "clusters": u})
}

func (a *api) invoice(w http.ResponseWriter, r *http.Request, c Caller) {
	month, err := time.Parse("2006-01", r.PathValue("month"))
	if err != nil {
		problem(w, http.StatusBadRequest, "month: YYYY-MM")
		return
	}
	u, err := a.store.Usage(r.Context(), c.OrgID, month, month.AddDate(0, 1, 0))
	if err != nil {
		a.fail(w, err)
		return
	}
	plans, err := a.store.ClusterPlans(r.Context(), c.OrgID)
	if err != nil {
		a.fail(w, err)
		return
	}
	reply(w, http.StatusOK, invoice(month.Format("2006-01"), u, plans, a.cfg))
}

func (a *api) collectNow(w http.ResponseWriter, r *http.Request) {
	n, failed := a.collector.collectAll(r.Context())
	reply(w, http.StatusOK, map[string]any{"collected": n, "failed": failed})
}
