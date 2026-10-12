package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

// collector reads every cluster's usage from its own metering: isomer.usage_report over the last
// two days, hour by hour. Hours are replaced as they fill up, so it can run as often as wanted.
type collector struct {
	store   *Store
	regions regions
	log     *slog.Logger
	mu      sync.Mutex
}

func (c *collector) run(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		c.collectAll(ctx)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// collectAll returns how many clusters were read, and the errors of those that couldn't be.
func (c *collector) collectAll(ctx context.Context) (int, map[string]string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	failed := map[string]string{}
	clusters, err := c.store.AllClusters(ctx)
	if err != nil {
		c.log.Error("listing clusters", "err", err)
		return 0, failed
	}
	n := 0
	for _, cl := range clusters {
		if err := c.collect(ctx, cl); err != nil {
			failed[cl.Name] = err.Error()
			_ = c.store.CollectionFailed(ctx, cl.ID, err.Error())
			c.log.Warn("collecting usage", "cluster", cl.Name, "namespace", cl.Namespace, "err", err)
			continue
		}
		n++
	}
	return n, failed
}

func (c *collector) collect(ctx context.Context, cl ClusterRow) error {
	k8s, ok := c.regions[cl.Region]
	if !ok {
		return fmt.Errorf("unknown region %s", cl.Region)
	}
	ic, err := getCluster(ctx, k8s, cl.Namespace, cl.Name)
	if err != nil {
		return err
	}
	if ic.Status.Endpoints.Postgres == "" {
		return fmt.Errorf("not provisioned yet")
	}
	pw, err := password(ctx, k8s, ic)
	if err != nil {
		return err
	}
	dsn := fmt.Sprintf("postgres://postgres:%s@%s/postgres?sslmode=prefer&connect_timeout=10", url.QueryEscape(pw), ic.Status.Endpoints.Postgres)
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)
	rows, err := conn.Query(ctx, `SELECT period, metric, dimension, unit, value::float8
		FROM isomer.usage_report(now() - interval '48 hours', now(), 'hour')`)
	if err != nil {
		return err
	}
	usage, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (UsageRow, error) {
		var u UsageRow
		err := r.Scan(&u.Hour, &u.Metric, &u.Dimension, &u.Unit, &u.Value)
		return u, err
	})
	if err != nil {
		return err
	}
	return c.store.PutUsage(ctx, cl.ID, usage)
}
