package controller

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"fmt"
	"regexp"
	"strconv"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	isomerv1 "github.com/Ash20pk/IsomerDB/operator/api/v1alpha1"
)

const fieldOwner = client.FieldOwner("isomerdb-operator")

// certReloadWindow is how long nodes keep reloading after their certificates are reissued.
const certReloadWindow = 3 * time.Minute

// IsomerClusterReconciler runs IsomerDB clusters.
type IsomerClusterReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=isomerdb.io,resources=isomerclusters,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=isomerdb.io,resources=isomerclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=isomerdb.io,resources=isomerclusters/scale,verbs=get;update;patch
// +kubebuilder:rbac:groups=apps,resources=statefulsets;deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=services;secrets;configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;patch;delete
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;delete
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=networking.k8s.io,resources=networkpolicies,verbs=get;list;watch;create;update;patch;delete

func (r *IsomerClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	c := &isomerv1.IsomerCluster{}
	if err := r.Get(ctx, req.NamespacedName, c); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	n := names{c}
	status := isomerv1.IsomerClusterStatus{
		ObservedGeneration: c.Generation,
		Conditions:         c.Status.Conditions,
		Endpoints: isomerv1.Endpoints{
			Postgres:          fmt.Sprintf("%s:5432", n.clientHost()),
			CredentialsSecret: n.credentials(),
		},
		LastRepairedJob:      c.Status.LastRepairedJob,
		CertificatesIssuedAt: c.Status.CertificatesIssuedAt,
	}
	if c.Spec.Gateway.Enabled {
		status.Endpoints.MongoDB = fmt.Sprintf("%s:10260", n.clientHost())
	}
	if c.Spec.API.Enabled {
		status.Endpoints.API = fmt.Sprintf("http://%s.%s.svc.cluster.local:8080", n.api(), c.Namespace)
	}

	password, err := r.ensureCredentials(ctx, c)
	if err != nil {
		return ctrl.Result{}, err
	}
	ca, leaf, rotated, err := r.ensureTLS(ctx, c)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("node certificates: %w", err)
	}
	if rotated {
		status.CertificatesIssuedAt = &metav1.Time{Time: time.Now()}
	}

	topology, err := r.ensureTopology(ctx, c)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("topology: %w", err)
	}
	tlsFor := func(host string) (*tls.Config, error) { return clientTLS(ca, leaf, host) }
	ha := c.Spec.HA.Standbys > 0
	status.LastFailover = c.Status.LastFailover
	// Passes that stop early (coordinator not ready, a failover) keep the last observed groups;
	// a full pass replaces them.
	status.Groups = c.Status.Groups

	// Objects that don't depend on cluster state.
	objects := []client.Object{
		authConfigMap(c),
		headlessService(c, n.coordinator(), roleCoordinator),
		headlessService(c, n.worker(), roleWorker),
		headlessService(c, n.coordinatorReplica(), roleCoordinatorReplica),
		headlessService(c, n.workerReplica(), roleWorkerReplica),
		workerNetworkPolicy(c),
		databaseStatefulSet(c, roleCoordinator, 1),
		databaseStatefulSet(c, roleCoordinatorReplica, replicaCount(c, topology, roleCoordinator, 1)),
	}
	if c.Spec.API.Enabled {
		objects = append(objects, apiService(c), serviceDeployment(c, roleAPI, c.Spec.API))
	}
	if c.Spec.Embedder.Enabled {
		objects = append(objects, serviceDeployment(c, roleEmbedder, c.Spec.Embedder))
	}
	for _, o := range objects {
		if err := r.apply(ctx, c, o); err != nil {
			return ctrl.Result{}, fmt.Errorf("applying %T %s: %w", o, o.GetName(), err)
		}
	}
	if !c.Spec.API.Enabled {
		if err := r.deleteIfExists(ctx, &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: n.api(), Namespace: c.Namespace}}); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.deleteIfExists(ctx, &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: n.api(), Namespace: c.Namespace}}); err != nil {
			return ctrl.Result{}, err
		}
	}
	if !c.Spec.Embedder.Enabled {
		if err := r.deleteIfExists(ctx, &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: n.embedder(), Namespace: c.Namespace}}); err != nil {
			return ctrl.Result{}, err
		}
	}

	pods, err := r.podState(ctx, c)
	if err != nil {
		return ctrl.Result{}, err
	}
	desired := int(c.Spec.Workers)

	// The coordinator group first: nothing else can be done without a coordinator.
	coordGroup := groups(c, topology, pods, 0)[0]
	if err := r.labelPrimaries(ctx, c, []group{coordGroup}); err != nil {
		return ctrl.Result{}, err
	}
	// The client Service selects the labelled primary, so it is applied after the labels.
	if err := r.apply(ctx, c, clientService(c)); err != nil {
		return ctrl.Result{}, err
	}
	if due, reason := failoverDue(c, coordGroup, time.Now()); due {
		f, err := r.promote(ctx, c, coordGroup, false, reason, password, tlsFor)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("coordinator failover: %w", err)
		}
		status.LastFailover = f
		return r.finish(ctx, c, status, time.Second)
	}
	if !coordGroup.primary.ready {
		// Workers can start alongside the coordinator: they wait to be registered.
		if err := r.applyWorkers(ctx, c, topology, desired); err != nil {
			return ctrl.Result{}, err
		}
		status.Phase = "Provisioning"
		reason, msg := "CoordinatorStarting", "waiting for the coordinator to be ready"
		if c.Status.Phase == "Ready" || c.Status.Phase == "Degraded" {
			status.Phase = "Degraded"
			reason, msg = "CoordinatorNotReady", fmt.Sprintf("coordinator %s is not ready", coordGroup.primary.pod)
		}
		setCondition(&status, "Ready", metav1.ConditionFalse, reason, msg)
		return r.finish(ctx, c, status, 5*time.Second)
	}

	tlsConfig, err := clientTLS(ca, leaf, n.clientHost())
	if err != nil {
		return ctrl.Result{}, err
	}
	db, err := connectCoordinator(ctx, n.clientHost(), password, tlsConfig)
	if err != nil {
		status.Phase = "Degraded"
		setCondition(&status, "Ready", metav1.ConditionFalse, "CoordinatorUnreachable", err.Error())
		return r.finish(ctx, c, status, 5*time.Second)
	}
	defer db.close(ctx)

	if status.IsomerVersion, err = db.isomerVersion(ctx); err != nil {
		return ctrl.Result{}, err
	}
	// New certificates reach the pods' volumes a minute or so after the Secret changes, so nodes
	// reload on every pass for a few minutes after an issue.
	if at := status.CertificatesIssuedAt; at != nil && time.Since(at.Time) < certReloadWindow {
		if err := db.reloadAll(ctx); err != nil {
			logger.Error(err, "reloading nodes after certificate rotation")
		}
	}
	registered, err := db.workers(ctx)
	if err != nil {
		return ctrl.Result{}, err
	}
	// After a failover, Citus is pointed at the groups' current primaries.
	{
		groupsNow := groups(c, topology, pods, workerGroups(n, registered, desired))
		changed, err := reconcileMetadata(ctx, n, db, groupsNow, registered)
		if err != nil {
			logger.Error(err, "bringing Citus metadata in line with the primaries; retrying")
			return r.finish(ctx, c, status, 3*time.Second)
		}
		if changed {
			if registered, err = db.workers(ctx); err != nil {
				return ctrl.Result{}, err
			}
		}
		status.Restored = c.Spec.Restore != nil && !changed
	}
	if b := c.Spec.Backup; b != nil {
		every := b.RestorePointEvery.Duration
		if every <= 0 {
			every = 15 * time.Minute
		}
		status.LastRestorePoint, status.LastRestorePointAt = c.Status.LastRestorePoint, c.Status.LastRestorePointAt
		if at := status.LastRestorePointAt; at == nil || time.Since(at.Time) >= every {
			name, err := db.createRestorePoint(ctx)
			if err != nil {
				logger.Error(err, "creating a restore point")
			} else {
				status.LastRestorePoint, status.LastRestorePointAt = name, &metav1.Time{Time: time.Now()}
			}
		}
	}
	jobID, jobState, err := db.rebalance(ctx)
	if err != nil {
		return ctrl.Result{}, err
	}
	busy := rebalanceActive(jobState)

	// The worker StatefulSets keep every group that is still registered: a node above the
	// desired count is only removed once its shards are drained.
	replicas := workerGroups(n, registered, desired)
	if err := r.applyWorkers(ctx, c, topology, replicas); err != nil {
		return ctrl.Result{}, err
	}
	gs := groups(c, topology, pods, replicas)
	if err := r.labelPrimaries(ctx, c, gs); err != nil {
		return ctrl.Result{}, err
	}

	// Worker failover: the standby takes over, and Citus is pointed at it.
	for _, g := range gs[1:] {
		if due, reason := failoverDue(c, g, time.Now()); due {
			f, err := r.promote(ctx, c, g, false, reason, password, tlsFor)
			if err != nil {
				return ctrl.Result{}, fmt.Errorf("%s failover: %w", g.name, err)
			}
			status.LastFailover = f
			return r.finish(ctx, c, status, time.Second)
		}
	}
	// Without HA, a group whose primary is still the replica pod (left from a failover) switches
	// back to the main pod, so the replica sets can be removed.
	if !ha {
		for _, g := range gs {
			if g.standby != nil && g.primary.pod != n.setFor(g.kind)+fmt.Sprintf("-%d", g.ordinal) && g.standby.ready && g.primary.ready {
				f, err := r.promote(ctx, c, g, true, "standbys turned off: moving the primary back", password, tlsFor)
				if err != nil {
					return ctrl.Result{}, fmt.Errorf("%s switchover: %w", g.name, err)
				}
				status.LastFailover = f
				return r.finish(ctx, c, status, time.Second)
			}
		}
	}

	// Replication, as each primary sees it now (tendGroup also keeps slots and settings right).
	reps := map[string]replication{}
	for _, g := range gs {
		if !g.primary.ready {
			continue
		}
		rep, err := r.tendGroup(ctx, c, g, password, tlsFor)
		if err != nil {
			logger.Error(err, "checking replication", "group", g.name)
		}
		reps[g.name] = rep
	}

	// Upgrades: one pod (or one switchover) at a time to the current template, then extensions.
	streaming := map[string]bool{}
	for name, rep := range reps {
		streaming[name] = rep.streaming
	}
	desc, f, err := r.rollOne(ctx, c, gs, pods, streaming, password, tlsFor)
	if err != nil {
		return ctrl.Result{}, err
	}
	if f != nil {
		status.LastFailover = f
	}
	if desc != "" {
		status.Phase = "Upgrading"
		setCondition(&status, "Ready", metav1.ConditionFalse, "Upgrading", desc)
		return r.finish(ctx, c, status, 3*time.Second)
	}
	allReady := true
	for _, g := range gs {
		allReady = allReady && g.primary.ready
	}
	if allReady {
		if n, err := updateExtensions(ctx, gs, password, tlsFor); err != nil {
			logger.Error(err, "updating extensions")
		} else if n > 0 {
			logger.Info("updated extensions", "count", n)
			if status.IsomerVersion, err = db.isomerVersion(ctx); err != nil {
				return ctrl.Result{}, err
			}
		}
	}

	scaling := false
	// Scale up: register every ready new worker (its group's primary), then one rebalance.
	added := 0
	for _, g := range gs[1:] {
		if g.ordinal >= desired {
			continue
		}
		if _, ok := registered[g.primary.host]; ok {
			continue
		}
		scaling = true
		if !g.primary.ready {
			continue
		}
		logger.Info("registering worker", "host", g.primary.host)
		if err := db.addNode(ctx, g.primary.host); err != nil {
			return ctrl.Result{}, fmt.Errorf("registering %s: %w", g.primary.host, err)
		}
		added++
	}
	if added > 0 {
		if registered, err = db.workers(ctx); err != nil {
			return ctrl.Result{}, err
		}
	}

	// Scale down: drain, then remove, nodes above the desired count (highest index first, the
	// same order the StatefulSets remove pods in).
	draining := 0
	for host, nd := range registered {
		i, ok := workerIndex(n, host)
		if !ok || i < desired {
			continue
		}
		scaling = true
		draining++
		if nd.shouldHaveShards {
			logger.Info("draining worker", "host", host)
			if err := db.markDraining(ctx, host); err != nil {
				return ctrl.Result{}, err
			}
			continue
		}
		if nd.shards == 0 && !busy {
			logger.Info("removing drained worker", "host", host)
			if err := db.removeNode(ctx, host); err != nil {
				return ctrl.Result{}, fmt.Errorf("removing %s: %w", host, err)
			}
			if err := r.forgetGroup(ctx, c, groupName(roleWorker, i)); err != nil {
				return ctrl.Result{}, err
			}
		}
	}

	// One rebalance at a time: drain first if any node is leaving, otherwise spread shards onto
	// new nodes.
	if !busy {
		if draining > 0 {
			if err := db.startRebalance(ctx, true); err != nil {
				return ctrl.Result{}, fmt.Errorf("starting drain: %w", err)
			}
			busy = true
		} else if needed, err := db.rebalanceNeeded(ctx); err != nil {
			// Planning sizes every shard; while a node restarts that fails. Try again next pass.
			logger.Info("cannot plan a rebalance yet", "reason", err.Error())
		} else if needed {
			logger.Info("starting rebalance")
			if err := db.startRebalance(ctx, false); err != nil {
				return ctrl.Result{}, fmt.Errorf("starting rebalance: %w", err)
			}
			busy = true
		}
	}
	if jobID, jobState, err = db.rebalance(ctx); err != nil {
		return ctrl.Result{}, err
	}
	status.Rebalance = jobState
	// After each finished rebalance, re-check IsomerDB's capture triggers on moved shards.
	if !rebalanceActive(jobState) && jobID > status.LastRepairedJob {
		repaired, err := db.repairAfterRebalance(ctx)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("checking capture triggers after rebalance %d: %w", jobID, err)
		}
		if repaired > 0 {
			logger.Info("repaired capture triggers after rebalance", "job", jobID, "projections", repaired)
		}
		status.LastRepairedJob = jobID
	}

	// Status.
	if registered, err = db.workers(ctx); err != nil {
		return ctrl.Result{}, err
	}
	degraded := false
	standbysDown := 0
	status.Groups = nil
	for _, g := range gs {
		gsStatus := isomerv1.GroupStatus{Name: g.name, Primary: g.primary.pod}
		if g.standby != nil {
			gsStatus.Standby = g.standby.pod
			if at := status.CertificatesIssuedAt; at != nil && time.Since(at.Time) < certReloadWindow && g.standby.ready {
				// Citus only knows the primaries (reloadAll above); standbys reload here.
				if sb, err := nodeConn(ctx, g.standby.host, password, tlsFor); err == nil {
					_ = sb.reload(ctx)
					sb.close(ctx)
				}
			}
			rep := reps[g.name]
			gsStatus.StandbyStreaming, gsStatus.ReplayLagBytes = rep.streaming, rep.lagBytes
			if ha && !rep.streaming {
				standbysDown++
			}
		}
		status.Groups = append(status.Groups, gsStatus)
		if g.kind != roleWorker {
			continue
		}
		nd, isRegistered := registered[g.primary.host]
		w := isomerv1.WorkerStatus{
			Name:       g.primary.pod,
			Ready:      g.primary.ready,
			Registered: isRegistered,
			Draining:   isRegistered && g.ordinal >= desired,
			Shards:     nd.shards,
		}
		if w.Ready && w.Registered && !w.Draining {
			status.ReadyWorkers++
		}
		if w.Registered && !w.Ready {
			degraded = true
		}
		status.Workers = append(status.Workers, w)
	}
	switch {
	case degraded:
		status.Phase = "Degraded"
		setCondition(&status, "Ready", metav1.ConditionFalse, "WorkerNotReady", "a registered worker is not ready; queries touching its shards fail")
	case scaling || busy || rebalanceActive(status.Rebalance):
		status.Phase = "Scaling"
		setCondition(&status, "Ready", metav1.ConditionFalse, "Scaling",
			fmt.Sprintf("%d of %d workers ready; rebalance %s", status.ReadyWorkers, desired, orNone(status.Rebalance)))
	default:
		status.Phase = "Ready"
		setCondition(&status, "Ready", metav1.ConditionTrue, "Ready", fmt.Sprintf("coordinator and %d workers ready", status.ReadyWorkers))
	}
	if ha {
		if standbysDown == 0 {
			setCondition(&status, "StandbysStreaming", metav1.ConditionTrue, "Streaming", "every node's standby is streaming")
		} else {
			setCondition(&status, "StandbysStreaming", metav1.ConditionFalse, "NotStreaming",
				fmt.Sprintf("%d of %d standbys are not streaming: a failover there would have nowhere to go", standbysDown, len(gs)))
		}
	} else {
		meta.RemoveStatusCondition(&status.Conditions, "StandbysStreaming")
	}
	if status.Phase == "Ready" && (!ha || standbysDown == 0) {
		return r.finish(ctx, c, status, 15*time.Second)
	}
	return r.finish(ctx, c, status, 5*time.Second)
}

// workerGroups: the desired worker count, or more while registered nodes above it are drained.
func workerGroups(n names, registered map[string]node, desired int) int {
	count := desired
	for host := range registered {
		if i, ok := workerIndex(n, host); ok && i+1 > count {
			count = i + 1
		}
	}
	return count
}

// replicaCount is how many pods a replica StatefulSet needs: one per group with HA on, and
// otherwise enough to keep any group whose primary is (still) its replica pod.
func replicaCount(c *isomerv1.IsomerCluster, topology map[string]string, kind string, groupsOfKind int) int32 {
	if c.Spec.HA.Standbys > 0 {
		return int32(groupsOfKind)
	}
	n := names{c}
	count := 0
	for i := 0; i < groupsOfKind; i++ {
		if topology[groupName(kind, i)] == fmt.Sprintf("%s-%d", n.setFor(kind+"-replica"), i) {
			count = i + 1
		}
	}
	return int32(count)
}

// applyWorkers sizes both worker StatefulSets for count groups.
func (r *IsomerClusterReconciler) applyWorkers(ctx context.Context, c *isomerv1.IsomerCluster, topology map[string]string, count int) error {
	if err := r.apply(ctx, c, databaseStatefulSet(c, roleWorker, int32(count))); err != nil {
		return err
	}
	return r.apply(ctx, c, databaseStatefulSet(c, roleWorkerReplica, replicaCount(c, topology, roleWorker, count)))
}

// forgetGroup removes a group from the topology once its node has left the cluster.
func (r *IsomerClusterReconciler) forgetGroup(ctx context.Context, c *isomerv1.IsomerCluster, name string) error {
	cm := &corev1.ConfigMap{}
	if err := r.Get(ctx, client.ObjectKey{Namespace: c.Namespace, Name: names{c}.topology()}, cm); err != nil {
		return client.IgnoreNotFound(err)
	}
	if _, ok := cm.Data[name]; !ok {
		return nil
	}
	delete(cm.Data, name)
	return r.Update(ctx, cm)
}

func orNone(s string) string {
	if s == "" {
		return "none"
	}
	return s
}

// workerHostRE matches a worker's host name: <cluster>-worker[-replica]-<i>.<set>.<namespace>...
// The cluster name is not checked: after a restore, Citus still names the source's hosts.
var workerHostRE = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?-worker(-replica)?-([0-9]+)\.`)

// workerIndex parses i from the host name of either member of worker group i.
func workerIndex(_ names, host string) (int, bool) {
	m := workerHostRE.FindStringSubmatch(host)
	if m == nil {
		return 0, false
	}
	i, err := strconv.Atoi(m[3])
	return i, err == nil
}

func (r *IsomerClusterReconciler) finish(ctx context.Context, c *isomerv1.IsomerCluster, status isomerv1.IsomerClusterStatus, after time.Duration) (ctrl.Result, error) {
	c.Status = status
	if err := r.Status().Update(ctx, c); err != nil {
		if apierrors.IsConflict(err) {
			return ctrl.Result{Requeue: true}, nil
		}
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: after}, nil
}

func setCondition(s *isomerv1.IsomerClusterStatus, t string, st metav1.ConditionStatus, reason, msg string) {
	meta.SetStatusCondition(&s.Conditions, metav1.Condition{Type: t, Status: st, Reason: reason, Message: msg, ObservedGeneration: s.ObservedGeneration})
}

// apply server-side-applies an owned object.
func (r *IsomerClusterReconciler) apply(ctx context.Context, c *isomerv1.IsomerCluster, o client.Object) error {
	gvk, err := r.GroupVersionKindFor(o)
	if err != nil {
		return err
	}
	o.GetObjectKind().SetGroupVersionKind(gvk)
	if err := controllerutil.SetControllerReference(c, o, r.Scheme); err != nil {
		return err
	}
	return r.Patch(ctx, o, client.Apply, fieldOwner, client.ForceOwnership)
}

func (r *IsomerClusterReconciler) deleteIfExists(ctx context.Context, o client.Object) error {
	return client.IgnoreNotFound(r.Delete(ctx, o))
}

// ensureCredentials creates the credentials Secret once and never rewrites it.
func (r *IsomerClusterReconciler) ensureCredentials(ctx context.Context, c *isomerv1.IsomerCluster) (string, error) {
	n := names{c}
	s := &corev1.Secret{}
	err := r.Get(ctx, client.ObjectKey{Namespace: c.Namespace, Name: n.credentials()}, s)
	if err == nil {
		return string(s.Data[keyPostgresPassword]), nil
	}
	if !apierrors.IsNotFound(err) {
		return "", err
	}
	data := map[string]string{
		keyPostgresPassword: randomSecret(),
		keyMongoUser:        "isomer",
		keyMongoPassword:    randomSecret(),
	}
	if rs := c.Spec.Restore; rs != nil {
		// A restored cluster's data carries the source's passwords.
		src := &corev1.Secret{}
		if err := r.Get(ctx, client.ObjectKey{Namespace: c.Namespace, Name: rs.CredentialsSecret}, src); err != nil {
			return "", fmt.Errorf("restore credentials %s: %w", rs.CredentialsSecret, err)
		}
		for _, k := range []string{keyPostgresPassword, keyMongoUser, keyMongoPassword} {
			if v, ok := src.Data[k]; ok {
				data[k] = string(v)
			}
		}
	}
	s = &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: n.credentials(), Namespace: c.Namespace, Labels: labels(c, roleCoordinator)},
		StringData: data,
	}
	if err := controllerutil.SetControllerReference(c, s, r.Scheme); err != nil {
		return "", err
	}
	if err := r.Create(ctx, s); err != nil {
		return "", err
	}
	return data[keyPostgresPassword], nil
}

func randomSecret() string {
	b := make([]byte, 18)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}

func (r *IsomerClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&isomerv1.IsomerCluster{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&appsv1.Deployment{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.ConfigMap{}).
		Owns(&networkingv1.NetworkPolicy{}).
		Complete(r)
}
