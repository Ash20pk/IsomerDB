package controller

import (
	"context"
	"crypto/tls"
	"fmt"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	isomerv1 "github.com/Ash20pk/IsomerDB/operator/api/v1alpha1"
)

// Upgrades. The database StatefulSets use the OnDelete update strategy: the operator, not the
// StatefulSet controller, decides when each pod moves to a new template (a new image, or any other
// change), so it can do it in the order that keeps the cluster available:
//
//   - one group at a time, and only while every group is healthy;
//   - with standbys: the standby first, then, once it streams again on the new template, a planned
//     switchover moves the primary onto it (the old primary shuts down cleanly, sending all its WAL,
//     so no commit is lost) and comes back on the new template as the standby;
//   - without standbys: workers one by one, then the coordinator (each is briefly unavailable).
//
// Once every pod runs the current template, extensions are updated on each primary
// (ALTER EXTENSION ... UPDATE, in dependency order); standbys receive the change by replication.

// extensionOrder: dependencies before the extensions that need them; isomer last.
var extensionOrder = []string{
	"citus_columnar", "citus", "vector", "pg_cron", "dblink", "pg_stat_statements", "postgis",
	"documentdb_core", "documentdb", "documentdb_distributed", "documentdb_extended_rum",
	"pg_search", "pg_trickle", "isomer",
}

const revisionLabel = "controller-revision-hash"

// updateRevisions maps each database StatefulSet to the revision its pods should run. ok is false
// while a StatefulSet has not yet processed its latest template.
func (r *IsomerClusterReconciler) updateRevisions(ctx context.Context, c *isomerv1.IsomerCluster) (map[string]string, bool, error) {
	n := names{c}
	out := map[string]string{}
	for _, role := range []string{roleCoordinator, roleCoordinatorReplica, roleWorker, roleWorkerReplica} {
		sts := &appsv1.StatefulSet{}
		err := r.Get(ctx, client.ObjectKey{Namespace: c.Namespace, Name: n.setFor(role)}, sts)
		if apierrors.IsNotFound(err) {
			continue
		}
		if err != nil {
			return nil, false, err
		}
		if sts.Status.ObservedGeneration < sts.Generation || sts.Status.UpdateRevision == "" {
			return nil, false, nil
		}
		out[sts.Name] = sts.Status.UpdateRevision
	}
	return out, true, nil
}

// outdated: the pod exists and runs an older revision of its StatefulSet's template.
func outdated(pods map[string]corev1.Pod, revisions map[string]string, m member) bool {
	p, ok := pods[m.pod]
	if !ok {
		return false
	}
	want, ok := revisions[m.pod[:strings.LastIndex(m.pod, "-")]]
	return ok && p.Labels[revisionLabel] != want
}

// rollOne moves at most one pod (or one group, by switchover) to the current template. It returns
// a description of what it did, or "" when every pod is current, or a wait reason.
func (r *IsomerClusterReconciler) rollOne(ctx context.Context, c *isomerv1.IsomerCluster, gs []group,
	pods map[string]corev1.Pod, streaming map[string]bool, password string,
	tlsFor func(string) (*tls.Config, error)) (string, *isomerv1.Failover, error) {
	revisions, ok, err := r.updateRevisions(ctx, c)
	if err != nil || !ok {
		return "waiting for the StatefulSets to take the new template", nil, err
	}
	var pending []group
	for _, g := range gs {
		if outdated(pods, revisions, g.primary) || (g.standby != nil && outdated(pods, revisions, *g.standby)) {
			pending = append(pending, g)
		}
	}
	if len(pending) == 0 {
		return "", nil, nil
	}
	// One group at a time, and only while all are healthy.
	for _, g := range gs {
		if !g.primary.ready {
			return fmt.Sprintf("upgrade waiting: %s's primary is not ready", g.name), nil, nil
		}
		if g.standby != nil && c.Spec.HA.Standbys > 0 && (!g.standby.ready || !streaming[g.name]) {
			return fmt.Sprintf("upgrade waiting: %s's standby is not streaming yet", g.name), nil, nil
		}
	}
	// Workers before the coordinator.
	g := pending[0]
	for _, p := range pending {
		if p.kind == roleWorker {
			g = p
			break
		}
	}
	logger := log.FromContext(ctx).WithValues("group", g.name)
	if g.standby != nil && c.Spec.HA.Standbys > 0 {
		if outdated(pods, revisions, *g.standby) {
			logger.Info("upgrading standby", "pod", g.standby.pod)
			return "upgrading " + g.standby.pod, nil, r.deletePod(ctx, c, g.standby.pod)
		}
		// The standby is current and streaming: move the primary onto it.
		f, err := r.promote(ctx, c, g, true, "upgrade: moving the primary onto its upgraded standby", password, tlsFor)
		if err != nil {
			return "", nil, fmt.Errorf("%s switchover for the upgrade: %w", g.name, err)
		}
		return "switched " + g.name + " over to " + g.standby.pod, f, nil
	}
	logger.Info("upgrading primary without a standby (briefly unavailable)", "pod", g.primary.pod)
	return "upgrading " + g.primary.pod, nil, r.deletePod(ctx, c, g.primary.pod)
}

func (r *IsomerClusterReconciler) deletePod(ctx context.Context, c *isomerv1.IsomerCluster, pod string) error {
	return client.IgnoreNotFound(r.Delete(ctx, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: pod, Namespace: c.Namespace}}))
}

// updateExtensions runs ALTER EXTENSION ... UPDATE on every group's primary for each extension
// whose installed version is older than the image's. It returns how many it updated.
func updateExtensions(ctx context.Context, gs []group, password string, tlsFor func(string) (*tls.Config, error)) (int, error) {
	updated := 0
	for _, g := range gs {
		db, err := nodeConn(ctx, g.primary.host, password, tlsFor)
		if err != nil {
			return updated, fmt.Errorf("connecting to %s: %w", g.primary.pod, err)
		}
		stale, err := db.staleExtensions(ctx)
		if err != nil {
			db.close(ctx)
			return updated, err
		}
		for _, ext := range extensionOrder {
			if !stale[ext] {
				continue
			}
			log.FromContext(ctx).Info("updating extension", "node", g.primary.pod, "extension", ext)
			if err := db.updateExtension(ctx, ext); err != nil {
				db.close(ctx)
				return updated, fmt.Errorf("updating %s on %s: %w", ext, g.primary.pod, err)
			}
			updated++
		}
		db.close(ctx)
	}
	return updated, nil
}
