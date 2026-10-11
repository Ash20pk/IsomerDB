package controller

import (
	"context"
	"crypto/tls"
	"fmt"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	isomerv1 "github.com/Ash20pk/IsomerDB/operator/api/v1alpha1"
)

// High availability (spec.ha). Every node — the coordinator and each worker — is a group of two
// pods: pod i of <name>-<kind> and pod i of <name>-<kind>-replica. One is the group's primary and
// the other streams from it as a hot standby. Which is which lives in the topology ConfigMap
// (group name -> primary pod), read by each pod's init container (isomer-node-init) and labelled
// on the pods (isomerdb.io/primary) so the client Service follows the primary coordinator.
//
// Failover: when a group's primary has been unready for spec.ha.failoverAfter and its standby is
// ready, the operator (1) records the standby as primary in the topology, (2) deletes the old
// primary's pod, so it cannot keep serving and comes back as a standby (rewound by its init
// container), (3) promotes the standby. Every pass then relabels the pods and brings Citus's
// metadata in line with the topology (reconcileMetadata): the coordinator's own host, and each
// worker's entry in pg_dist_node.

// member is one pod of a group as the operator sees it.
type member struct {
	pod     string
	host    string
	exists  bool
	ready   bool
	since   time.Time // when it last became unready (or was created, if never ready)
	primary bool      // the pod's isomerdb.io/primary label
}

type group struct {
	name    string // coordinator-0, worker-3
	kind    string // coordinator | worker
	ordinal int
	primary member
	standby *member // nil without HA
}

func groupName(kind string, ordinal int) string { return fmt.Sprintf("%s-%d", kind, ordinal) }

// podState collects every database pod of the cluster.
func (r *IsomerClusterReconciler) podState(ctx context.Context, c *isomerv1.IsomerCluster) (map[string]corev1.Pod, error) {
	pods := &corev1.PodList{}
	if err := r.List(ctx, pods, client.InNamespace(c.Namespace), client.MatchingLabels{"app.kubernetes.io/instance": c.Name}); err != nil {
		return nil, err
	}
	out := map[string]corev1.Pod{}
	for _, p := range pods.Items {
		if p.DeletionTimestamp == nil {
			out[p.Name] = p
		}
	}
	return out, nil
}

func memberOf(n names, pods map[string]corev1.Pod, pod string) member {
	m := member{pod: pod, host: n.podHost(pod)}
	p, ok := pods[pod]
	if !ok {
		return m
	}
	m.exists = true
	m.since = p.CreationTimestamp.Time
	m.primary = p.Labels[labelPrimary] == "true"
	for _, cond := range p.Status.Conditions {
		if cond.Type == corev1.PodReady {
			m.ready = cond.Status == corev1.ConditionTrue
			if !m.ready && !cond.LastTransitionTime.IsZero() {
				m.since = cond.LastTransitionTime.Time
			}
		}
	}
	return m
}

// ensureTopology returns the topology (group -> primary pod), creating the ConfigMap if needed.
func (r *IsomerClusterReconciler) ensureTopology(ctx context.Context, c *isomerv1.IsomerCluster) (map[string]string, error) {
	n := names{c}
	cm := &corev1.ConfigMap{}
	err := r.Get(ctx, client.ObjectKey{Namespace: c.Namespace, Name: n.topology()}, cm)
	if apierrors.IsNotFound(err) {
		cm = &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: n.topology(), Namespace: c.Namespace, Labels: labels(c, roleCoordinator)},
			Data:       map[string]string{},
		}
		if err := controllerutil.SetControllerReference(c, cm, r.Scheme); err != nil {
			return nil, err
		}
		if err := r.Create(ctx, cm); err != nil {
			return nil, err
		}
		return map[string]string{}, nil
	}
	if err != nil {
		return nil, err
	}
	if cm.Data == nil {
		return map[string]string{}, nil
	}
	return cm.Data, nil
}

func (r *IsomerClusterReconciler) setTopology(ctx context.Context, c *isomerv1.IsomerCluster, groupName, primary string) error {
	cm := &corev1.ConfigMap{}
	if err := r.Get(ctx, client.ObjectKey{Namespace: c.Namespace, Name: names{c}.topology()}, cm); err != nil {
		return err
	}
	if cm.Data == nil {
		cm.Data = map[string]string{}
	}
	cm.Data[groupName] = primary
	return r.Update(ctx, cm)
}

// groups lists the coordinator group and worker groups 0..workers-1 with their current members.
func groups(c *isomerv1.IsomerCluster, topology map[string]string, pods map[string]corev1.Pod, workers int) []group {
	n := names{c}
	ha := c.Spec.HA.Standbys > 0
	var out []group
	add := func(kind string, i int) {
		g := group{name: groupName(kind, i), kind: kind, ordinal: i}
		mainPod := fmt.Sprintf("%s-%d", n.setFor(kind), i)
		replicaPod := fmt.Sprintf("%s-%d", n.setFor(kind+"-replica"), i)
		primaryPod := topology[g.name]
		if primaryPod == "" {
			primaryPod = mainPod
		}
		g.primary = memberOf(n, pods, primaryPod)
		other := replicaPod
		if primaryPod == replicaPod {
			other = mainPod
		}
		if ha || primaryPod == replicaPod {
			s := memberOf(n, pods, other)
			g.standby = &s
		}
		out = append(out, g)
	}
	add(roleCoordinator, 0)
	for i := 0; i < workers; i++ {
		add(roleWorker, i)
	}
	return out
}

// labelPrimaries sets isomerdb.io/primary on every group member to match the topology.
func (r *IsomerClusterReconciler) labelPrimaries(ctx context.Context, c *isomerv1.IsomerCluster, gs []group) error {
	set := func(m member, primary bool) error {
		if !m.exists || m.primary == primary {
			return nil
		}
		patch := fmt.Sprintf(`{"metadata":{"labels":{%q:%q}}}`, labelPrimary, fmt.Sprint(primary))
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: m.pod, Namespace: c.Namespace}}
		return client.IgnoreNotFound(r.Patch(ctx, pod, client.RawPatch(types.StrategicMergePatchType, []byte(patch))))
	}
	for _, g := range gs {
		if err := set(g.primary, true); err != nil {
			return err
		}
		if g.standby != nil {
			if err := set(*g.standby, false); err != nil {
				return err
			}
		}
	}
	return nil
}

// missingSince remembers when a primary pod was first seen missing (a StatefulSet briefly has no
// pod while it replaces one, so a missing pod gets the same grace as an unready one). In memory:
// after an operator restart the clock starts again, which only delays a failover.
var missingSince sync.Map // "<namespace>/<cluster>/<group>" -> time.Time

// failoverDue: the primary has been unready (or missing) long enough, and the standby can take over.
func failoverDue(c *isomerv1.IsomerCluster, g group, now time.Time) (bool, string) {
	key := c.Namespace + "/" + c.Name + "/" + g.name
	if g.primary.exists {
		missingSince.Delete(key)
	}
	if c.Spec.HA.Standbys == 0 || g.standby == nil || !g.standby.ready || g.primary.ready {
		return false, ""
	}
	after := c.Spec.HA.FailoverAfter.Duration
	if after <= 0 {
		after = 30 * time.Second
	}
	since, what := g.primary.since, "unready"
	if !g.primary.exists {
		first, _ := missingSince.LoadOrStore(key, now)
		since, what = first.(time.Time), "missing"
	}
	if down := now.Sub(since); down >= after {
		return true, fmt.Sprintf("primary %s for %s", what, down.Round(time.Second))
	}
	return false, ""
}

// nodeConn connects straight to one database pod as postgres, verifying it, with the node
// certificate (workers accept it; coordinators also take the password).
func nodeConn(ctx context.Context, host, password string, tlsFor func(string) (*tls.Config, error)) (*citus, error) {
	cfg, err := tlsFor(host)
	if err != nil {
		return nil, err
	}
	return connectCoordinator(ctx, host, password, cfg)
}

// promote makes g's standby its primary. planned: the old primary is shut down cleanly first, and
// the standby replays everything it sent before being promoted (no committed write is lost).
func (r *IsomerClusterReconciler) promote(ctx context.Context, c *isomerv1.IsomerCluster, g group, planned bool, reason string,
	password string, tlsFor func(string) (*tls.Config, error)) (*isomerv1.Failover, error) {
	logger := log.FromContext(ctx).WithValues("group", g.name, "from", g.primary.pod, "to", g.standby.pod, "planned", planned)
	logger.Info("promoting standby", "reason", reason)

	// 1. The topology names the new primary first: the old pod restarts as its standby.
	if err := r.setTopology(ctx, c, g.name, g.standby.pod); err != nil {
		return nil, fmt.Errorf("recording the new primary: %w", err)
	}
	// 2. Fence the old primary.
	if g.primary.exists {
		opts := []client.DeleteOption{}
		if !planned {
			opts = append(opts, client.GracePeriodSeconds(0))
		}
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: g.primary.pod, Namespace: c.Namespace}}
		if err := client.IgnoreNotFound(r.Delete(ctx, pod, opts...)); err != nil {
			return nil, fmt.Errorf("fencing %s: %w", g.primary.pod, err)
		}
	}
	standby, err := nodeConn(ctx, g.standby.host, password, tlsFor)
	if err != nil {
		return nil, fmt.Errorf("connecting to standby %s: %w", g.standby.pod, err)
	}
	defer standby.close(ctx)
	if planned {
		// A clean shutdown sends all WAL to the standby before the old primary exits; wait until
		// the standby has stopped receiving and replayed all of it.
		if err := r.waitGone(ctx, c, g.primary.pod, 2*time.Minute); err != nil {
			return nil, err
		}
		if err := standby.waitReplayed(ctx, time.Minute); err != nil {
			return nil, err
		}
	}
	// 3. Promote. Citus is pointed at it by reconcileMetadata, on this pass and every one after,
	// so a step that fails now (a metadata node briefly out of sync, say) is simply retried.
	if err := standby.promote(ctx); err != nil {
		return nil, fmt.Errorf("promoting %s: %w", g.standby.pod, err)
	}
	logger.Info("standby promoted")
	return &isomerv1.Failover{Group: g.name, From: g.primary.pod, To: g.standby.pod, At: metav1.Now(), Reason: reason}, nil
}

// waitGone waits for a pod to be deleted (its replacement, if any, has a new uid).
func (r *IsomerClusterReconciler) waitGone(ctx context.Context, c *isomerv1.IsomerCluster, pod string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var uid types.UID
	for {
		p := &corev1.Pod{}
		err := r.Get(ctx, client.ObjectKey{Namespace: c.Namespace, Name: pod}, p)
		if apierrors.IsNotFound(err) {
			return nil
		}
		if err != nil {
			return err
		}
		if uid == "" {
			uid = p.UID
		} else if p.UID != uid {
			return nil // replaced: the old one is gone
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("pod %s still running after %s", pod, timeout)
		}
		time.Sleep(time.Second)
	}
}

// replication is what a primary reports about its standby.
type replication struct {
	streaming bool
	lagBytes  int64
	slotLost  bool
}

// standbySlot is the physical replication slot a standby pod streams through.
func standbySlot(pod string) string { return strings.ReplaceAll(pod, "-", "_") }

// tendGroup checks a group's replication on its primary: streaming state and lag for the status,
// slots of standbys that no longer exist are dropped (they would hold WAL forever), and a standby
// whose slot was invalidated (it fell behind past maxRetainedWAL) is re-cloned.
func (r *IsomerClusterReconciler) tendGroup(ctx context.Context, c *isomerv1.IsomerCluster, g group, password string,
	tlsFor func(string) (*tls.Config, error)) (replication, error) {
	var rep replication
	if !g.primary.ready {
		return rep, nil
	}
	db, err := nodeConn(ctx, g.primary.host, password, tlsFor)
	if err != nil {
		return rep, err
	}
	defer db.close(ctx)
	keep := ""
	if g.standby != nil && c.Spec.HA.Standbys > 0 {
		keep = standbySlot(g.standby.pod)
	}
	defer func() {
		// Commits wait for the standby only while it is streaming, so a standby that is down
		// never stops writes (a failover in that window can lose the commits since it left).
		sync := ""
		if c.Spec.HA.Synchronous && keep != "" && rep.streaming {
			sync = standbySlot(g.standby.pod)
		}
		wal := c.Spec.HA.MaxRetainedWAL
		if wal == "" {
			wal = "4GB"
		}
		if err := db.replicationSettings(ctx, wal, sync); err != nil {
			log.FromContext(ctx).Error(err, "setting replication settings", "group", g.name)
		}
	}()
	if err := db.dropOtherSlots(ctx, keep); err != nil {
		return rep, err
	}
	if keep == "" {
		return rep, nil
	}
	rep, err = db.replicationOf(ctx, keep)
	if err != nil {
		return rep, err
	}
	if rep.slotLost && g.standby.exists {
		log.FromContext(ctx).Info("standby fell too far behind; re-cloning it", "group", g.name, "standby", g.standby.pod)
		if err := db.dropSlot(ctx, keep); err != nil {
			return rep, err
		}
		pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "data-" + g.standby.pod, Namespace: c.Namespace}}
		if err := client.IgnoreNotFound(r.Delete(ctx, pvc)); err != nil {
			return rep, err
		}
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: g.standby.pod, Namespace: c.Namespace}}
		if err := client.IgnoreNotFound(r.Delete(ctx, pod)); err != nil {
			return rep, err
		}
	}
	return rep, nil
}

// reconcileMetadata points Citus at each group's current primary: first every registered worker
// whose entry names another host (the other member of its group after a failover, or the source
// cluster's host after a restore), then the coordinator's own entry, syncing metadata after each
// step so the next one can reach every node. It returns whether anything changed.
func reconcileMetadata(ctx context.Context, n names, db *citus, gs []group, registered map[string]node) (bool, error) {
	logger := log.FromContext(ctx)
	changed := false
	for host := range registered {
		i, ok := workerIndex(n, host)
		if !ok || i+1 >= len(gs) {
			continue
		}
		want := gs[i+1].primary.host
		if host == want {
			continue
		}
		logger.Info("pointing Citus at a worker's primary", "group", gs[i+1].name, "from", host, "to", want)
		if err := db.updateNode(ctx, host, want); err != nil {
			return changed, fmt.Errorf("repointing %s: %w", gs[i+1].name, err)
		}
		changed = true
	}
	coordinatorHost, err := db.coordinatorHost(ctx)
	if err != nil {
		return changed, err
	}
	want := gs[0].primary.host
	if coordinatorHost == want && !changed {
		return false, nil
	}
	// citus_update_node and citus_set_coordinator_host need metadata nodes in sync.
	if err := db.syncMetadata(ctx); err != nil {
		return changed, err
	}
	if coordinatorHost != "" && coordinatorHost != want {
		logger.Info("pointing Citus at the coordinator", "from", coordinatorHost, "to", want)
		if err := db.setCoordinatorHost(ctx, want); err != nil {
			return changed, fmt.Errorf("setting the coordinator host: %w", err)
		}
		if err := db.syncMetadata(ctx); err != nil {
			return true, err
		}
		changed = true
	}
	return changed, nil
}
