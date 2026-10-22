package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/crypto/bcrypt"
)

// Store is the control plane's own database.
type Store struct {
	db *pgxpool.Pool
}

const schema = `
CREATE TABLE IF NOT EXISTS orgs (
    id         bigserial PRIMARY KEY,
    name       text NOT NULL UNIQUE,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS users (
    id            bigserial PRIMARY KEY,
    org_id        bigint NOT NULL REFERENCES orgs (id),
    email         text NOT NULL UNIQUE,
    password_hash text NOT NULL,
    created_at    timestamptz NOT NULL DEFAULT now()
);
-- API tokens, kept only as SHA-256 hashes.
CREATE TABLE IF NOT EXISTS tokens (
    hash         text PRIMARY KEY,
    user_id      bigint NOT NULL REFERENCES users (id) ON DELETE CASCADE,
    created_at   timestamptz NOT NULL DEFAULT now(),
    last_used_at timestamptz
);
CREATE TABLE IF NOT EXISTS clusters (
    id         bigserial PRIMARY KEY,
    org_id     bigint NOT NULL REFERENCES orgs (id),
    name       text NOT NULL,
    region     text NOT NULL,
    plan       text NOT NULL,
    namespace  text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    deleted_at timestamptz
);
CREATE UNIQUE INDEX IF NOT EXISTS clusters_live_name ON clusters (org_id, name) WHERE deleted_at IS NULL;
-- Hourly usage read from each cluster's own metering (isomer.usage_report), replaced as an hour
-- fills up, so collecting twice never counts twice.
CREATE TABLE IF NOT EXISTS usage (
    cluster_id bigint NOT NULL REFERENCES clusters (id),
    hour       timestamptz NOT NULL,
    metric     text NOT NULL,
    dimension  text NOT NULL,
    unit       text NOT NULL,
    value      numeric NOT NULL,
    PRIMARY KEY (cluster_id, hour, metric, dimension)
);
CREATE TABLE IF NOT EXISTS collections (
    cluster_id bigint PRIMARY KEY REFERENCES clusters (id),
    at         timestamptz NOT NULL,
    error      text
);
`

func openStore(ctx context.Context, url string) (*Store, error) {
	var db *pgxpool.Pool
	var err error
	for attempt := 0; attempt < 60; attempt++ {
		if db, err = pgxpool.New(ctx, url); err == nil {
			if err = db.Ping(ctx); err == nil {
				break
			}
			db.Close()
		}
		time.Sleep(2 * time.Second)
	}
	if err != nil {
		return nil, fmt.Errorf("connecting: %w", err)
	}
	if _, err := db.Exec(ctx, schema); err != nil {
		return nil, fmt.Errorf("creating the schema: %w", err)
	}
	return &Store{db: db}, nil
}

var (
	errExists   = errors.New("already exists")
	errNotFound = errors.New("not found")
	errAuth     = errors.New("email or password not recognised")
)

func isUnique(err error) bool {
	var pg *pgconn.PgError
	return errors.As(err, &pg) && pg.Code == "23505"
}

func newToken() (token, hash string) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	token = "iso_" + hex.EncodeToString(b)
	sum := sha256.Sum256([]byte(token))
	return token, hex.EncodeToString(sum[:])
}

// Signup creates an organisation and its first user, and returns an API token for the user.
func (s *Store) Signup(ctx context.Context, org, email, password string) (string, error) {
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return "", err
	}
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return "", err
	}
	defer tx.Rollback(ctx)
	var orgID, userID int64
	if err := tx.QueryRow(ctx, "INSERT INTO orgs (name) VALUES ($1) RETURNING id", org).Scan(&orgID); err != nil {
		if isUnique(err) {
			return "", fmt.Errorf("organisation %q %w", org, errExists)
		}
		return "", err
	}
	if err := tx.QueryRow(ctx, "INSERT INTO users (org_id, email, password_hash) VALUES ($1, $2, $3) RETURNING id",
		orgID, email, string(hash)).Scan(&userID); err != nil {
		if isUnique(err) {
			return "", fmt.Errorf("a user with email %q %w", email, errExists)
		}
		return "", err
	}
	token, tokenHash := newToken()
	if _, err := tx.Exec(ctx, "INSERT INTO tokens (hash, user_id) VALUES ($1, $2)", tokenHash, userID); err != nil {
		return "", err
	}
	return token, tx.Commit(ctx)
}

// Login checks a password and returns a new API token.
func (s *Store) Login(ctx context.Context, email, password string) (string, error) {
	var userID int64
	var hash string
	err := s.db.QueryRow(ctx, "SELECT id, password_hash FROM users WHERE email = $1", email).Scan(&userID, &hash)
	if errors.Is(err, pgx.ErrNoRows) || (err == nil && bcrypt.CompareHashAndPassword([]byte(hash), []byte(password)) != nil) {
		return "", errAuth
	}
	if err != nil {
		return "", err
	}
	token, tokenHash := newToken()
	_, err = s.db.Exec(ctx, "INSERT INTO tokens (hash, user_id) VALUES ($1, $2)", tokenHash, userID)
	return token, err
}

// Revoke deletes an API token (logging out).
func (s *Store) Revoke(ctx context.Context, token string) error {
	sum := sha256.Sum256([]byte(token))
	_, err := s.db.Exec(ctx, "DELETE FROM tokens WHERE hash = $1", hex.EncodeToString(sum[:]))
	return err
}

// Caller is who a request comes from.
type Caller struct {
	UserID int64
	OrgID  int64
	Org    string
	Email  string
}

func (s *Store) Authenticate(ctx context.Context, token string) (Caller, error) {
	sum := sha256.Sum256([]byte(token))
	var c Caller
	err := s.db.QueryRow(ctx, `
		UPDATE tokens t SET last_used_at = now() FROM users u JOIN orgs o ON o.id = u.org_id
		WHERE t.hash = $1 AND u.id = t.user_id
		RETURNING u.id, o.id, o.name, u.email`, hex.EncodeToString(sum[:])).Scan(&c.UserID, &c.OrgID, &c.Org, &c.Email)
	if errors.Is(err, pgx.ErrNoRows) {
		return c, errAuth
	}
	return c, err
}

// ClusterRow is a cluster as the control plane records it.
type ClusterRow struct {
	ID        int64     `json:"-"`
	OrgID     int64     `json:"-"`
	Name      string    `json:"name"`
	Region    string    `json:"region"`
	Plan      string    `json:"plan"`
	Namespace string    `json:"-"`
	CreatedAt time.Time `json:"created_at"`
}

func (s *Store) CreateCluster(ctx context.Context, orgID int64, name, region, plan, namespace string) (ClusterRow, error) {
	c := ClusterRow{OrgID: orgID, Name: name, Region: region, Plan: plan, Namespace: namespace}
	err := s.db.QueryRow(ctx, `INSERT INTO clusters (org_id, name, region, plan, namespace) VALUES ($1, $2, $3, $4, $5)
		RETURNING id, created_at`, orgID, name, region, plan, namespace).Scan(&c.ID, &c.CreatedAt)
	if isUnique(err) {
		return c, fmt.Errorf("a cluster named %q %w", name, errExists)
	}
	return c, err
}

func (s *Store) clusters(ctx context.Context, where string, args ...any) ([]ClusterRow, error) {
	rows, err := s.db.Query(ctx, "SELECT id, org_id, name, region, plan, namespace, created_at FROM clusters WHERE deleted_at IS NULL "+where+" ORDER BY created_at", args...)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (ClusterRow, error) {
		var c ClusterRow
		err := r.Scan(&c.ID, &c.OrgID, &c.Name, &c.Region, &c.Plan, &c.Namespace, &c.CreatedAt)
		return c, err
	})
}

func (s *Store) Clusters(ctx context.Context, orgID int64) ([]ClusterRow, error) {
	return s.clusters(ctx, "AND org_id = $1", orgID)
}

func (s *Store) AllClusters(ctx context.Context) ([]ClusterRow, error) {
	return s.clusters(ctx, "")
}

func (s *Store) Cluster(ctx context.Context, orgID int64, name string) (ClusterRow, error) {
	cs, err := s.clusters(ctx, "AND org_id = $1 AND name = $2", orgID, name)
	if err != nil {
		return ClusterRow{}, err
	}
	if len(cs) == 0 {
		return ClusterRow{}, fmt.Errorf("cluster %q %w", name, errNotFound)
	}
	return cs[0], nil
}

func (s *Store) DeleteCluster(ctx context.Context, id int64) error {
	_, err := s.db.Exec(ctx, "UPDATE clusters SET deleted_at = now() WHERE id = $1", id)
	return err
}

// UsageRow is one hour of one metric for one cluster.
type UsageRow struct {
	Hour      time.Time `json:"hour"`
	Metric    string    `json:"metric"`
	Dimension string    `json:"dimension"`
	Unit      string    `json:"unit"`
	Value     float64   `json:"value"`
}

// PutUsage replaces the hours it is given for a cluster.
func (s *Store) PutUsage(ctx context.Context, clusterID int64, rows []UsageRow) error {
	batch := &pgx.Batch{}
	for _, r := range rows {
		batch.Queue(`INSERT INTO usage (cluster_id, hour, metric, dimension, unit, value) VALUES ($1, $2, $3, $4, $5, $6)
			ON CONFLICT (cluster_id, hour, metric, dimension) DO UPDATE SET value = EXCLUDED.value, unit = EXCLUDED.unit`,
			clusterID, r.Hour, r.Metric, r.Dimension, r.Unit, r.Value)
	}
	batch.Queue(`INSERT INTO collections (cluster_id, at, error) VALUES ($1, now(), NULL)
		ON CONFLICT (cluster_id) DO UPDATE SET at = now(), error = NULL`, clusterID)
	return s.db.SendBatch(ctx, batch).Close()
}

func (s *Store) CollectionFailed(ctx context.Context, clusterID int64, message string) error {
	_, err := s.db.Exec(ctx, `INSERT INTO collections (cluster_id, at, error) VALUES ($1, now(), $2)
		ON CONFLICT (cluster_id) DO UPDATE SET at = now(), error = EXCLUDED.error`, clusterID, message)
	return err
}

// Usage returns an organisation's usage per cluster between two times, hour by hour.
func (s *Store) Usage(ctx context.Context, orgID int64, from, to time.Time) (map[string][]UsageRow, error) {
	rows, err := s.db.Query(ctx, `
		SELECT c.name, u.hour, u.metric, u.dimension, u.unit, u.value::float8
		FROM usage u JOIN clusters c ON c.id = u.cluster_id
		WHERE c.org_id = $1 AND u.hour >= $2 AND u.hour < $3
		ORDER BY c.name, u.hour, u.metric, u.dimension`, orgID, from, to)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string][]UsageRow{}
	for rows.Next() {
		var name string
		var r UsageRow
		if err := rows.Scan(&name, &r.Hour, &r.Metric, &r.Dimension, &r.Unit, &r.Value); err != nil {
			return nil, err
		}
		out[name] = append(out[name], r)
	}
	return out, rows.Err()
}

// ClusterPlans maps an organisation's clusters (including deleted ones, which still bill for the
// time they ran) to their plans.
func (s *Store) ClusterPlans(ctx context.Context, orgID int64) (map[string]string, error) {
	rows, err := s.db.Query(ctx, "SELECT DISTINCT ON (name) name, plan FROM clusters WHERE org_id = $1 ORDER BY name, created_at DESC", orgID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var name, plan string
		if err := rows.Scan(&name, &plan); err != nil {
			return nil, err
		}
		out[name] = plan
	}
	return out, rows.Err()
}
