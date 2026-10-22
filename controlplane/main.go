// The IsomerDB cloud control plane (PRD FR-CL1, FR-CL2): the managed service on top of the
// operator. Teams sign up, create clusters in a region on a plan, scale and delete them, read
// their credentials, and see their usage and invoices. Clusters are IsomerCluster resources in a
// namespace per organisation, in the Kubernetes cluster that serves the region; the operator does
// the rest (provisioning, failover, backups, upgrades). Usage is read from each cluster's own
// metering (isomer.usage_report) and priced by the cluster's plan.
//
// Configuration (environment):
//
//	CP_DATABASE_URL   the control plane's own Postgres (organisations, users, clusters, usage)
//	CP_LISTEN         address to serve the API on (default :8095)
//	CP_CONFIG         JSON file with images, regions and plans (default: built in)
//	CP_COLLECT_EVERY  how often usage is collected from every cluster (default 5m)
//	CP_ADMIN_TOKEN    bearer token for /v1/admin/* (collect now); unset disables them
package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func env(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, nil))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg := defaultConfig()
	if path := os.Getenv("CP_CONFIG"); path != "" {
		data, err := os.ReadFile(path)
		if err == nil {
			err = json.Unmarshal(data, &cfg)
		}
		if err != nil {
			log.Error("reading configuration", "path", path, "err", err)
			os.Exit(1)
		}
	}

	store, err := openStore(ctx, env("CP_DATABASE_URL", "postgres://postgres@localhost:5432/controlplane"))
	if err != nil {
		log.Error("opening the store", "err", err)
		os.Exit(1)
	}
	regions, err := connectRegions(cfg.Regions)
	if err != nil {
		log.Error("connecting to regions", "err", err)
		os.Exit(1)
	}

	every, err := time.ParseDuration(env("CP_COLLECT_EVERY", "5m"))
	if err != nil {
		log.Error("CP_COLLECT_EVERY", "err", err)
		os.Exit(1)
	}
	collector := &collector{store: store, regions: regions, log: log}
	go collector.run(ctx, every)

	srv := &http.Server{
		Addr:              env("CP_LISTEN", ":8095"),
		Handler:           newAPI(cfg, store, regions, collector, os.Getenv("CP_ADMIN_TOKEN"), log),
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdown)
	}()
	log.Info("control plane listening", "addr", srv.Addr, "regions", len(cfg.Regions), "plans", len(cfg.Plans))
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Error("serving", "err", err)
		os.Exit(1)
	}
}
