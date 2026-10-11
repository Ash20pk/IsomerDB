package controller

import (
	"context"
	"crypto/tls"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// citus talks to the coordinator: node membership, rebalancing, draining.
type citus struct {
	conn *pgx.Conn
}

// connectCoordinator connects as postgres over TLS, verifying the coordinator's certificate.
func connectCoordinator(ctx context.Context, host, password string, tlsConfig *tls.Config) (*citus, error) {
	cfg, err := pgx.ParseConfig(fmt.Sprintf("host=%s port=5432 user=postgres dbname=postgres connect_timeout=5", host))
	if err != nil {
		return nil, err
	}
	cfg.Password = password
	cfg.TLSConfig = tlsConfig
	cfg.Fallbacks = nil
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

func (c *citus) reload(ctx context.Context) error {
	_, err := c.conn.Exec(ctx, `SELECT pg_reload_conf()`)
	return err
}

// reloadAll reloads configuration (and TLS files) on the coordinator and every worker.
func (c *citus) reloadAll(ctx context.Context) error {
	_, err := c.conn.Exec(ctx, `SELECT pg_reload_conf(); SELECT run_command_on_workers('SELECT pg_reload_conf()')`)
	return err
}

// ── high availability (on individual nodes) ─────────────────────────────────

// waitReplayed waits, on a standby whose primary has shut down, until it has replayed everything
// it received and its WAL receiver has stopped.
func (c *citus) waitReplayed(ctx context.Context, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		var done bool
		err := c.conn.QueryRow(ctx, `
			SELECT pg_last_wal_receive_lsn() IS NOT NULL
			   AND pg_last_wal_replay_lsn() >= pg_last_wal_receive_lsn()
			   AND NOT EXISTS (SELECT 1 FROM pg_stat_wal_receiver WHERE status = 'streaming')`).Scan(&done)
		if err != nil {
			return err
		}
		if done {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("standby did not finish replaying within %s", timeout)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func (c *citus) promote(ctx context.Context) error {
	var ok bool
	if err := c.conn.QueryRow(ctx, `SELECT pg_promote(true, 60)`).Scan(&ok); err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("pg_promote did not finish within 60s")
	}
	return nil
}

func (c *citus) setCoordinatorHost(ctx context.Context, host string) error {
	_, err := c.conn.Exec(ctx, `SELECT citus_set_coordinator_host($1, 5432)`, host)
	return err
}

// updateNode repoints a worker's metadata from its old primary to the promoted standby.
func (c *citus) updateNode(ctx context.Context, oldHost, newHost string) error {
	_, err := c.conn.Exec(ctx, `
		SELECT citus_update_node(nodeid, $2, 5432, true)
		FROM pg_dist_node WHERE nodename = $1 AND nodeport = 5432`, oldHost, newHost)
	return err
}

// dropOtherSlots drops inactive physical slots of standbys that are no longer this node's.
func (c *citus) dropOtherSlots(ctx context.Context, keep string) error {
	_, err := c.conn.Exec(ctx, `
		SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots
		WHERE slot_type = 'physical' AND NOT active AND slot_name <> $1
		  AND slot_name ~ '_(coordinator|worker)(_replica)?_[0-9]+$'`, keep)
	return err
}

func (c *citus) dropSlot(ctx context.Context, slot string) error {
	_, err := c.conn.Exec(ctx, `
		SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1 AND NOT active`, slot)
	return err
}

// replicationOf reports a standby's slot on this primary.
func (c *citus) replicationOf(ctx context.Context, slot string) (replication, error) {
	var r replication
	err := c.conn.QueryRow(ctx, `
		SELECT coalesce(s.wal_status = 'lost', false), coalesce(rep.state = 'streaming', false),
		       coalesce(pg_wal_lsn_diff(pg_current_wal_lsn(), rep.replay_lsn), 0)::bigint
		FROM pg_replication_slots s LEFT JOIN pg_stat_replication rep ON rep.pid = s.active_pid
		WHERE s.slot_name = $1`, slot).Scan(&r.slotLost, &r.streaming, &r.lagBytes)
	if err == pgx.ErrNoRows {
		return replication{}, nil
	}
	return r, err
}

// replicationSettings sets a primary's WAL retention for standbys and its synchronous standby
// (by application_name, which is the standby's slot name; "" for asynchronous), reloading only
// when something changed.
func (c *citus) replicationSettings(ctx context.Context, maxRetainedWAL, syncStandby string) error {
	var changed bool
	err := c.conn.QueryRow(ctx, `
		SELECT current_setting('max_slot_wal_keep_size') <> pg_size_pretty(pg_size_bytes($1))
		    OR current_setting('synchronous_standby_names') <> $2`, maxRetainedWAL, syncStandby).Scan(&changed)
	if err != nil || !changed {
		return err
	}
	if _, err := c.conn.Exec(ctx, fmt.Sprintf(`ALTER SYSTEM SET max_slot_wal_keep_size = '%s'`, escapeLiteral(maxRetainedWAL))); err != nil {
		return err
	}
	if _, err := c.conn.Exec(ctx, fmt.Sprintf(`ALTER SYSTEM SET synchronous_standby_names = '%s'`, escapeLiteral(syncStandby))); err != nil {
		return err
	}
	_, err = c.conn.Exec(ctx, `SELECT pg_reload_conf()`)
	return err
}

func escapeLiteral(s string) string { return strings.ReplaceAll(s, "'", "''") }

// coordinatorHost is the coordinator's own entry in pg_dist_node ("" if it has none).
func (c *citus) coordinatorHost(ctx context.Context) (string, error) {
	var host string
	err := c.conn.QueryRow(ctx, `SELECT nodename FROM pg_dist_node WHERE groupid = 0`).Scan(&host)
	if err == pgx.ErrNoRows {
		return "", nil
	}
	return host, err
}

// syncMetadata brings every metadata node's copy of the metadata up to date.
func (c *citus) syncMetadata(ctx context.Context) error {
	_, err := c.conn.Exec(ctx, `SELECT start_metadata_sync_to_all_nodes()`)
	return err
}
