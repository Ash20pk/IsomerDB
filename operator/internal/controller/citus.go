package controller

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// citus talks to the coordinator: node membership, rebalancing, draining.
type citus struct {
	conn *pgx.Conn
}

func connectCoordinator(ctx context.Context, host, password string) (*citus, error) {
	cfg, err := pgx.ParseConfig(fmt.Sprintf("host=%s port=5432 user=postgres dbname=postgres sslmode=disable connect_timeout=5", host))
	if err != nil {
		return nil, err
	}
	cfg.Password = password
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &citus{conn: conn}, nil
}

func (c *citus) close(ctx context.Context) { _ = c.conn.Close(ctx) }

// node is a registered worker.
type node struct {
	host             string
	shouldHaveShards bool
	// shards counts placements of distributed (not reference) tables: what draining must move.
	shards int64
}

func (c *citus) isomerVersion(ctx context.Context) (string, error) {
	var v string
	err := c.conn.QueryRow(ctx, `SELECT extversion FROM pg_extension WHERE extname = 'isomer'`).Scan(&v)
	return v, err
}

func (c *citus) workers(ctx context.Context) (map[string]node, error) {
	rows, err := c.conn.Query(ctx, `
		SELECT n.nodename, n.shouldhaveshards,
		       (SELECT count(*) FROM pg_dist_placement p
		        JOIN pg_dist_shard s USING (shardid)
		        JOIN pg_dist_partition t USING (logicalrelid)
		        WHERE p.groupid = n.groupid AND t.partmethod <> 'n')
		FROM pg_dist_node n
		WHERE n.groupid <> 0 AND n.noderole = 'primary'`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]node{}
	for rows.Next() {
		var n node
		if err := rows.Scan(&n.host, &n.shouldHaveShards, &n.shards); err != nil {
			return nil, err
		}
		out[n.host] = n
	}
	return out, rows.Err()
}

// rebalance returns the latest background rebalance job's id and state ("" if there has been none).
func (c *citus) rebalance(ctx context.Context) (int64, string, error) {
	var id int64
	var state string
	err := c.conn.QueryRow(ctx, `SELECT job_id, state::text FROM pg_dist_background_job ORDER BY job_id DESC LIMIT 1`).Scan(&id, &state)
	if err == pgx.ErrNoRows {
		return 0, "", nil
	}
	return id, state, err
}

func rebalanceActive(state string) bool {
	switch state {
	case "scheduled", "running", "cancelling", "failing":
		return true
	}
	return false
}

// addNode registers a worker without starting a rebalance (one is started once every new node is in).
func (c *citus) addNode(ctx context.Context, host string) error {
	_, err := c.conn.Exec(ctx, `SELECT isomer.add_node($1, 5432, false)`, host)
	return err
}

func (c *citus) startRebalance(ctx context.Context, drainOnly bool) error {
	_, err := c.conn.Exec(ctx, `SELECT citus_rebalance_start(drain_only => $1)`, drainOnly)
	return err
}

// rebalanceNeeded: would a rebalance move anything?
func (c *citus) rebalanceNeeded(ctx context.Context) (bool, error) {
	var n int64
	err := c.conn.QueryRow(ctx, `SELECT count(*) FROM get_rebalance_table_shards_plan()`).Scan(&n)
	return n > 0, err
}

// markDraining stops new shards landing on a node, so a drain-only rebalance moves its shards off.
func (c *citus) markDraining(ctx context.Context, host string) error {
	_, err := c.conn.Exec(ctx, `SELECT citus_set_node_property($1, 5432, 'shouldhaveshards', false)`, host)
	return err
}

func (c *citus) removeNode(ctx context.Context, host string) error {
	_, err := c.conn.Exec(ctx, `SELECT citus_remove_node($1, 5432)`, host)
	return err
}

// repairAfterRebalance re-checks capture triggers on every placement (isomer.rebalance_finished).
func (c *citus) repairAfterRebalance(ctx context.Context) (int, error) {
	var n int
	err := c.conn.QueryRow(ctx, `SELECT count(*) FROM isomer.rebalance_finished()`).Scan(&n)
	return n, err
}
