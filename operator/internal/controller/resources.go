package controller

import (
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	isomerv1 "github.com/Ash20pk/IsomerDB/operator/api/v1alpha1"
)

const (
	roleCoordinator = "coordinator"
	roleWorker      = "worker"
	roleAPI         = "api"
	roleEmbedder    = "embedder"

	keyPostgresPassword = "postgres-password"
	keyMongoUser        = "mongo-user"
	keyMongoPassword    = "mongo-password"

	// The Postgres image runs as uid/gid 999.
	postgresUID = 999
)

// names derives every object name from the cluster's name.
type names struct {
	c *isomerv1.IsomerCluster
}

func (n names) credentials() string { return n.c.Name + "-credentials" }
func (n names) coordinator() string { return n.c.Name + "-coordinator" }
func (n names) worker() string      { return n.c.Name + "-worker" }
func (n names) client() string      { return n.c.Name }
func (n names) api() string         { return n.c.Name + "-api" }
func (n names) embedder() string    { return n.c.Name + "-embedder" }

// coordinatorHost is the coordinator pod's stable DNS name, which workers use to reach it.
func (n names) coordinatorHost() string {
	return fmt.Sprintf("%s-0.%s.%s.svc.cluster.local", n.coordinator(), n.coordinator(), n.c.Namespace)
}

// workerHost is worker i's stable DNS name, as registered with Citus.
func (n names) workerHost(i int) string {
	return fmt.Sprintf("%s-%d.%s.%s.svc.cluster.local", n.worker(), i, n.worker(), n.c.Namespace)
}

// clientHost is the Service clients (and the operator) use for Postgres.
func (n names) clientHost() string {
	return fmt.Sprintf("%s.%s.svc.cluster.local", n.client(), n.c.Namespace)
}

func labels(c *isomerv1.IsomerCluster, role string) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":       "isomerdb",
		"app.kubernetes.io/instance":   c.Name,
		"app.kubernetes.io/managed-by": "isomerdb-operator",
		"isomerdb.io/role":             role,
	}
}

func selector(c *isomerv1.IsomerCluster, role string) map[string]string {
	return map[string]string{"app.kubernetes.io/instance": c.Name, "isomerdb.io/role": role}
}

func secretEnv(name, secret, key string) corev1.EnvVar {
	return corev1.EnvVar{Name: name, ValueFrom: &corev1.EnvVarSource{
		SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: secret}, Key: key},
	}}
}

// ── services ────────────────────────────────────────────────────────────────

// headlessService gives each pod of a StatefulSet a stable DNS name, published before the pod is
// ready: Citus and the init scripts need to resolve nodes that are still starting.
func headlessService(c *isomerv1.IsomerCluster, name, role string) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: c.Namespace, Labels: labels(c, role)},
		Spec: corev1.ServiceSpec{
			ClusterIP:                corev1.ClusterIPNone,
			PublishNotReadyAddresses: true,
			Selector:                 selector(c, role),
			Ports:                    []corev1.ServicePort{{Name: "postgres", Port: 5432, TargetPort: intstr.FromInt32(5432)}},
		},
	}
}

func clientService(c *isomerv1.IsomerCluster) *corev1.Service {
	n := names{c}
	ports := []corev1.ServicePort{{Name: "postgres", Port: 5432, TargetPort: intstr.FromInt32(5432)}}
	if c.Spec.Gateway.Enabled {
		ports = append(ports, corev1.ServicePort{Name: "mongodb", Port: 10260, TargetPort: intstr.FromInt32(10260)})
	}
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: n.client(), Namespace: c.Namespace, Labels: labels(c, roleCoordinator)},
		Spec:       corev1.ServiceSpec{Selector: selector(c, roleCoordinator), Ports: ports},
	}
}

func apiService(c *isomerv1.IsomerCluster) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: names{c}.api(), Namespace: c.Namespace, Labels: labels(c, roleAPI)},
		Spec: corev1.ServiceSpec{
			Selector: selector(c, roleAPI),
			Ports:    []corev1.ServicePort{{Name: "http", Port: 8080, TargetPort: intstr.FromInt32(8080)}},
		},
	}
}

// ── database nodes ──────────────────────────────────────────────────────────

// isomerReady passes once the server accepts TCP connections and the isomer extension exists. The
// image's first-boot setup runs against a socket-only server, so this stays false until it is done.
var isomerReady = []string{"sh", "-c",
	`pg_isready -U postgres -h 127.0.0.1 -q && psql -U postgres -h /var/run/postgresql -tAc "SELECT 1 FROM pg_extension WHERE extname = 'isomer'" | grep -q 1`}

func databaseStatefulSet(c *isomerv1.IsomerCluster, role string, replicas int32) *appsv1.StatefulSet {
	n := names{c}
	name := n.coordinator()
	if role == roleWorker {
		name = n.worker()
	}
	env := []corev1.EnvVar{
		secretEnv("POSTGRES_PASSWORD", n.credentials(), keyPostgresPassword),
		{Name: "ISOMER_ROLE", Value: role},
	}
	if role == roleCoordinator {
		env = append(env,
			corev1.EnvVar{Name: "ISOMER_COORDINATOR_HOST", Value: n.coordinatorHost()},
			secretEnv("ISOMER_MONGO_USER", n.credentials(), keyMongoUser),
			secretEnv("ISOMER_MONGO_PASSWORD", n.credentials(), keyMongoPassword),
		)
	} else {
		// Citus connects to workers as each client's own role (a MongoDB user's, for example),
		// whose password the coordinator does not hold. Development setting, as in docker-compose:
		// a NetworkPolicy keeps workers reachable from the cluster's own pods only. Production
		// uses certificate authentication (citus.node_conninfo) or pg_dist_authinfo.
		env = append(env, corev1.EnvVar{Name: "POSTGRES_HOST_AUTH_METHOD", Value: "trust"})
	}

	postgres := corev1.Container{
		Name:            "postgres",
		Image:           c.Spec.Image,
		ImagePullPolicy: c.Spec.ImagePullPolicy,
		Env:             env,
		Ports:           []corev1.ContainerPort{{Name: "postgres", ContainerPort: 5432}},
		Resources:       c.Spec.Resources,
		VolumeMounts: []corev1.VolumeMount{
			{Name: "data", MountPath: "/var/lib/postgresql"},
			{Name: "socket", MountPath: "/var/run/postgresql"},
			{Name: "dshm", MountPath: "/dev/shm"},
		},
		// First boot (initdb, extensions, DocumentDB setup) takes a minute or two.
		StartupProbe: &corev1.Probe{
			ProbeHandler:     corev1.ProbeHandler{Exec: &corev1.ExecAction{Command: []string{"pg_isready", "-U", "postgres", "-h", "127.0.0.1", "-q"}}},
			PeriodSeconds:    5,
			FailureThreshold: 120,
		},
		LivenessProbe: &corev1.Probe{
			ProbeHandler:     corev1.ProbeHandler{Exec: &corev1.ExecAction{Command: []string{"pg_isready", "-U", "postgres", "-h", "/var/run/postgresql", "-q"}}},
			PeriodSeconds:    10,
			TimeoutSeconds:   5,
			FailureThreshold: 6,
		},
		ReadinessProbe: &corev1.Probe{
			ProbeHandler:   corev1.ProbeHandler{Exec: &corev1.ExecAction{Command: isomerReady}},
			PeriodSeconds:  5,
			TimeoutSeconds: 5,
		},
	}
	containers := []corev1.Container{postgres}
	if role == roleCoordinator && c.Spec.Gateway.Enabled {
		// The gateway only connects to Postgres over its Unix socket, so it shares the pod.
		containers = append(containers, corev1.Container{
			Name:            "gateway",
			Image:           c.Spec.Image,
			ImagePullPolicy: c.Spec.ImagePullPolicy,
			Command:         []string{"documentdb_gateway", "/etc/isomerdb/gateway.json"},
			Ports:           []corev1.ContainerPort{{Name: "mongodb", ContainerPort: 10260}},
			VolumeMounts:    []corev1.VolumeMount{{Name: "socket", MountPath: "/var/run/postgresql"}},
			ReadinessProbe: &corev1.Probe{
				ProbeHandler:  corev1.ProbeHandler{TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromInt32(10260)}},
				PeriodSeconds: 5,
			},
		})
	}

	storage := c.Spec.Storage
	if storage.IsZero() {
		storage = resource.MustParse("10Gi")
	}
	retention := appsv1.RetainPersistentVolumeClaimRetentionPolicyType
	if c.Spec.DeleteVolumes {
		retention = appsv1.DeletePersistentVolumeClaimRetentionPolicyType
	}
	fsGroup := int64(postgresUID)
	shm := resource.MustParse("1Gi")
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: c.Namespace, Labels: labels(c, role)},
		Spec: appsv1.StatefulSetSpec{
			Replicas:            &replicas,
			ServiceName:         name,
			PodManagementPolicy: appsv1.ParallelPodManagement,
			Selector:            &metav1.LabelSelector{MatchLabels: selector(c, role)},
			// A worker is only scaled away after its shards are drained, so its volume holds no
			// data the cluster needs; a node added later starts empty.
			PersistentVolumeClaimRetentionPolicy: &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{
				WhenDeleted: retention,
				WhenScaled:  appsv1.DeletePersistentVolumeClaimRetentionPolicyType,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels(c, role)},
				Spec: corev1.PodSpec{
					SecurityContext:               &corev1.PodSecurityContext{FSGroup: &fsGroup},
					TerminationGracePeriodSeconds: ptr(int64(60)),
					Containers:                    containers,
					Volumes: []corev1.Volume{
						{Name: "socket", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
						{Name: "dshm", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{Medium: corev1.StorageMediumMemory, SizeLimit: &shm}}},
					},
				},
			},
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{{
				ObjectMeta: metav1.ObjectMeta{Name: "data", Labels: labels(c, role)},
				Spec: corev1.PersistentVolumeClaimSpec{
					AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
					StorageClassName: c.Spec.StorageClassName,
					Resources:        corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: storage}},
				},
			}},
		},
	}
}

// ── stateless components ────────────────────────────────────────────────────

func serviceDeployment(c *isomerv1.IsomerCluster, role string, spec isomerv1.ComponentSpec) *appsv1.Deployment {
	n := names{c}
	name := n.api()
	if role == roleEmbedder {
		name = n.embedder()
	}
	replicas := int32(1)
	if spec.Replicas != nil {
		replicas = *spec.Replicas
	}
	env := []corev1.EnvVar{
		secretEnv("ISOMER_PG_PASSWORD", n.credentials(), keyPostgresPassword),
		// $(VAR) expands from the variable above.
		{Name: "ISOMER_DATABASE_URL", Value: fmt.Sprintf("host=%s port=5432 user=postgres password=$(ISOMER_PG_PASSWORD) dbname=postgres", n.clientHost())},
	}
	env = append(env, spec.Env...)
	container := corev1.Container{
		Name:            role,
		Image:           spec.Image,
		ImagePullPolicy: c.Spec.ImagePullPolicy,
		Env:             env,
	}
	if role == roleAPI {
		container.Ports = []corev1.ContainerPort{{Name: "http", ContainerPort: 8080}}
		container.ReadinessProbe = &corev1.Probe{
			ProbeHandler:  corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/healthz", Port: intstr.FromInt32(8080)}},
			PeriodSeconds: 5,
		}
	}
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: c.Namespace, Labels: labels(c, role)},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: selector(c, role)},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels(c, role)},
				Spec:       corev1.PodSpec{Containers: []corev1.Container{container}},
			},
		},
	}
}

func ptr[T any](v T) *T { return &v }

// workerNetworkPolicy admits connections to workers only from this cluster's own database pods
// (the coordinator and the other workers), since workers trust connections (see above).
func workerNetworkPolicy(c *isomerv1.IsomerCluster) *networkingv1.NetworkPolicy {
	return &networkingv1.NetworkPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: names{c}.worker(), Namespace: c.Namespace, Labels: labels(c, roleWorker)},
		Spec: networkingv1.NetworkPolicySpec{
			PodSelector: metav1.LabelSelector{MatchLabels: selector(c, roleWorker)},
			PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeIngress},
			Ingress: []networkingv1.NetworkPolicyIngressRule{{
				From: []networkingv1.NetworkPolicyPeer{
					{PodSelector: &metav1.LabelSelector{MatchLabels: selector(c, roleCoordinator)}},
					{PodSelector: &metav1.LabelSelector{MatchLabels: selector(c, roleWorker)}},
				},
			}},
		},
	}
}
