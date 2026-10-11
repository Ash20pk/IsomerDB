package controller

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
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

// IsomerClusterReconciler runs IsomerDB clusters.
type IsomerClusterReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=isomerdb.io,resources=isomerclusters,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=isomerdb.io,resources=isomerclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=isomerdb.io,resources=isomerclusters/scale,verbs=get;update;patch
// +kubebuilder:rbac:groups=apps,resources=statefulsets;deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=services;secrets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
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
		LastRepairedJob: c.Status.LastRepairedJob,
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

	// Objects that don't depend on cluster state.
	objects := []client.Object{
		headlessService(c, n.coordinator(), roleCoordinator),
		headlessService(c, n.worker(), roleWorker),
		clientService(c),
		workerNetworkPolicy(c),
		databaseStatefulSet(c, roleCoordinator, 1),
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

	ready, err := r.podReadiness(ctx, c)
	if err != nil {
		return ctrl.Result{}, err
	}
	desired := int(c.Spec.Workers)

	if !ready[n.coordinator()+"-0"] {
		// Workers can start alongside the coordinator: they wait to be registered.
		if err := r.apply(ctx, c, databaseStatefulSet(c, roleWorker, int32(desired))); err != nil {
			return ctrl.Result{}, err
		}
		status.Phase = "Provisioning"
		setCondition(&status, "Ready", metav1.ConditionFalse, "CoordinatorStarting", "waiting for the coordinator to be ready")
		return r.finish(ctx, c, status, 5*time.Second)
	}

	db, err := connectCoordinator(ctx, n.clientHost(), password)
	if err != nil {
		status.Phase = "Degraded"
		setCondition(&status, "Ready", metav1.ConditionFalse, "CoordinatorUnreachable", err.Error())
		return r.finish(ctx, c, status, 5*time.Second)
	}
	defer db.close(ctx)

	if status.IsomerVersion, err = db.isomerVersion(ctx); err != nil {
		return ctrl.Result{}, err
	}
	registered, err := db.workers(ctx)
	if err != nil {
		return ctrl.Result{}, err
	}
	jobID, jobState, err := db.rebalance(ctx)
	if err != nil {
		return ctrl.Result{}, err
	}
	busy := rebalanceActive(jobState)

	// The worker StatefulSet keeps every pod that is still registered: a node above the desired
	// count is only removed once its shards are drained.
	replicas := desired
	for host := range registered {
		if i, ok := workerIndex(n, host); ok && i+1 > replicas {
			replicas = i + 1
		}
	}
	if err := r.apply(ctx, c, databaseStatefulSet(c, roleWorker, int32(replicas))); err != nil {
		return ctrl.Result{}, err
	}

	scaling := false
	// Scale up: register every ready new worker, then one rebalance for all of them.
	added := 0
	for i := 0; i < desired; i++ {
		host := n.workerHost(i)
		if _, ok := registered[host]; ok {
			continue
		}
		scaling = true
		if !ready[fmt.Sprintf("%s-%d", n.worker(), i)] {
			continue
		}
		logger.Info("registering worker", "host", host)
		if err := db.addNode(ctx, host); err != nil {
			return ctrl.Result{}, fmt.Errorf("registering %s: %w", host, err)
		}
		added++
	}
	if added > 0 {
		if registered, err = db.workers(ctx); err != nil {
			return ctrl.Result{}, err
		}
	}

	// Scale down: drain, then remove, nodes above the desired count (highest index first, the
	// same order the StatefulSet removes pods in).
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
			return ctrl.Result{}, err
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
	for i := 0; i < replicas; i++ {
		host := n.workerHost(i)
		nd, isRegistered := registered[host]
		w := isomerv1.WorkerStatus{
			Name:       fmt.Sprintf("%s-%d", n.worker(), i),
			Ready:      ready[fmt.Sprintf("%s-%d", n.worker(), i)],
			Registered: isRegistered,
			Draining:   isRegistered && i >= desired,
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
	if status.Phase == "Ready" {
		return r.finish(ctx, c, status, 30*time.Second)
	}
	return r.finish(ctx, c, status, 5*time.Second)
}

func orNone(s string) string {
	if s == "" {
		return "none"
	}
	return s
}

// workerIndex parses i from worker i's host name.
func workerIndex(n names, host string) (int, bool) {
	prefix := n.worker() + "-"
	suffix := "." + n.worker() + "." + n.c.Namespace + ".svc.cluster.local"
	if !strings.HasPrefix(host, prefix) || !strings.HasSuffix(host, suffix) {
		return 0, false
	}
	i, err := strconv.Atoi(strings.TrimSuffix(strings.TrimPrefix(host, prefix), suffix))
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
	s = &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: n.credentials(), Namespace: c.Namespace, Labels: labels(c, roleCoordinator)},
		StringData: map[string]string{
			keyPostgresPassword: randomSecret(),
			keyMongoUser:        "isomer",
			keyMongoPassword:    randomSecret(),
		},
	}
	if err := controllerutil.SetControllerReference(c, s, r.Scheme); err != nil {
		return "", err
	}
	if err := r.Create(ctx, s); err != nil {
		return "", err
	}
	return s.StringData[keyPostgresPassword], nil
}

func randomSecret() string {
	b := make([]byte, 18)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}

// podReadiness maps each database pod's name to whether it is ready.
func (r *IsomerClusterReconciler) podReadiness(ctx context.Context, c *isomerv1.IsomerCluster) (map[string]bool, error) {
	pods := &corev1.PodList{}
	if err := r.List(ctx, pods, client.InNamespace(c.Namespace), client.MatchingLabels{"app.kubernetes.io/instance": c.Name}); err != nil {
		return nil, err
	}
	out := map[string]bool{}
	for _, p := range pods.Items {
		if p.DeletionTimestamp != nil {
			continue
		}
		for _, cond := range p.Status.Conditions {
			if cond.Type == corev1.PodReady {
				out[p.Name] = cond.Status == corev1.ConditionTrue
			}
		}
	}
	return out, nil
}

func (r *IsomerClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&isomerv1.IsomerCluster{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&appsv1.Deployment{}).
		Owns(&corev1.Service{}).
		Owns(&networkingv1.NetworkPolicy{}).
		Complete(r)
}
