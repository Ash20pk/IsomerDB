package controller

import (
	"fmt"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	isomerv1 "github.com/Ash20pk/IsomerDB/operator/api/v1alpha1"
)

const (
	roleCoordinator        = "coordinator"
	roleWorker             = "worker"
	roleCoordinatorReplica = "coordinator-replica"
	roleWorkerReplica      = "worker-replica"
	roleAPI                = "api"
	roleEmbedder           = "embedder"

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
func (n names) auth() string        { return n.c.Name + "-auth" }
func (n names) topology() string    { return n.c.Name + "-topology" }

// Standbys (spec.ha) run in StatefulSets beside the primaries: pod i of <name>-worker-replica
// replicates pod i of <name>-worker, and either may be the group's primary after a failover.
func (n names) coordinatorReplica() string { return n.c.Name + "-coordinator-replica" }
func (n names) workerReplica() string      { return n.c.Name + "-worker-replica" }

// coordinatorHost is the coordinator pod's stable DNS name, which workers use to reach it.
func (n names) coordinatorHost() string {
	return fmt.Sprintf("%s-0.%s.%s.svc.cluster.local", n.coordinator(), n.coordinator(), n.c.Namespace)
}

// workerHost is worker i's stable DNS name, as registered with Citus.
func (n names) workerHost(i int) string {
	return fmt.Sprintf("%s-%d.%s.%s.svc.cluster.local", n.worker(), i, n.worker(), n.c.Namespace)
}

// podHost is a database pod's stable DNS name: <pod>.<its StatefulSet>.<namespace>.svc.cluster.local.
func (n names) podHost(pod string) string {
	set := pod[:strings.LastIndex(pod, "-")]
	return fmt.Sprintf("%s.%s.%s.svc.cluster.local", pod, set, n.c.Namespace)
}

// setFor is the StatefulSet (and headless Service) name for a database role.
func (n names) setFor(role string) string { return n.c.Name + "-" + role }

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

const (
	labelNode    = "isomerdb.io/node"    // coordinator | worker, on primaries and standbys alike
	labelPrimary = "isomerdb.io/primary" // "true" on each group's primary pod (set by the operator)
)

// nodeKind is the kind of node a database role runs: a coordinator or a worker.
func nodeKind(role string) string { return strings.TrimSuffix(role, "-replica") }

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
		// Whichever coordinator pod is the primary (the operator labels it).
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{"app.kubernetes.io/instance": c.Name, labelNode: roleCoordinator, labelPrimary: "true"},
			Ports:    ports,
		},
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

// ── authentication ──────────────────────────────────────────────────────────

// Node-to-node connections (Citus, IsomerDB's own, replication) use TLS and the node client
// certificate, which maps to any role. Applications reach the coordinator with a password; workers
// accept nothing but the node certificate. Inside the pod (socket, loopback) trust is kept: the
// init scripts and the MongoDB gateway connect that way.
const hbaCommon = `# Managed by the IsomerDB operator.
local    all          all                  trust
host     all          all   127.0.0.1/32   trust
host     all          all   ::1/128        trust
hostssl  replication  all   all            cert map=isomer-nodes
`

func authConfigMap(c *isomerv1.IsomerCluster) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: names{c}.auth(), Namespace: c.Namespace, Labels: labels(c, roleCoordinator)},
		Data: map[string]string{
			"pg_hba-coordinator.conf": hbaCommon + "host     all          all   all            scram-sha-256\n",
			"pg_hba-worker.conf":      hbaCommon + "hostssl  all          all   all            cert map=isomer-nodes\n",
			"pg_ident.conf":           "# Managed by the IsomerDB operator: the node certificate may act as any role.\nisomer-nodes  " + nodeCertCN + "  all\n",
		},
	}
}

// postgresArgs points the server at the operator's TLS files and authentication rules, and makes
// every connection Citus opens to another node verify it and present the node certificate.
//
// Replication settings (max_slot_wal_keep_size, synchronous_standby_names) are not here: the
// operator sets them on each primary with ALTER SYSTEM (replicationSettings), so turning standbys
// on or off never restarts a primary.
func postgresArgs(c *isomerv1.IsomerCluster, role string) []string {
	args := []string{
		"postgres",
		"-c", "ssl=on",
		"-c", "ssl_cert_file=" + tlsDir + "/tls.crt",
		"-c", "ssl_key_file=" + tlsDir + "/tls.key",
		"-c", "ssl_ca_file=" + tlsDir + "/ca.crt",
		"-c", "hba_file=/etc/isomerdb/auth/pg_hba-" + role + ".conf",
		"-c", "ident_file=/etc/isomerdb/auth/pg_ident.conf",
		"-c", "citus.node_conninfo=" + nodeConninfo,
	}
	if c.Spec.Backup != nil {
		// Primaries archive every WAL segment to the backup store (standbys do not, until promoted).
		args = append(args,
			"-c", "archive_mode=on",
			"-c", "archive_command=isomer-walg wal-push %p",
			"-c", "archive_timeout=60")
	}
	return args
}

const nodeConninfo = "sslmode=verify-full sslrootcert=" + tlsDir + "/ca.crt sslcert=" + tlsDir + "/client.crt sslkey=" + tlsDir + "/client.key"

// ── backups ─────────────────────────────────────────────────────────────────

// s3URL is the s3:// location of a cluster's backups (prefix defaults to <namespace>/<name>).
func s3URL(c *isomerv1.IsomerCluster, s isomerv1.S3Spec) string {
	prefix := s.Prefix
	if prefix == "" {
		prefix = c.Namespace + "/" + c.Name
	}
	return "s3://" + s.Bucket + "/" + strings.Trim(prefix, "/")
}

// s3Env is what WAL-G needs to reach a store; envPrefix names the variables (for the restore
// source: ISOMER_RESTORE_AWS_...).
func s3Env(s isomerv1.S3Spec, envPrefix string) []corev1.EnvVar {
	region := s.Region
	if region == "" {
		region = "us-east-1"
	}
	env := []corev1.EnvVar{
		secretEnv(envPrefix+"AWS_ACCESS_KEY_ID", s.CredentialsSecret, "access-key-id"),
		secretEnv(envPrefix+"AWS_SECRET_ACCESS_KEY", s.CredentialsSecret, "secret-access-key"),
		{Name: envPrefix + "AWS_REGION", Value: region},
	}
	if s.Endpoint != "" {
		env = append(env, corev1.EnvVar{Name: envPrefix + "AWS_ENDPOINT", Value: s.Endpoint})
	}
	if s.ForcePathStyle {
		env = append(env, corev1.EnvVar{Name: envPrefix + "AWS_S3_FORCE_PATH_STYLE", Value: "true"})
	}
	return env
}

// backupEnv: the variables isomer-walg needs, for this cluster's store and a restore's source.
func backupEnv(c *isomerv1.IsomerCluster) []corev1.EnvVar {
	env := []corev1.EnvVar{{Name: "ISOMER_CLUSTER", Value: c.Name}}
	if b := c.Spec.Backup; b != nil {
		env = append(env, corev1.EnvVar{Name: "ISOMER_BACKUP_PREFIX", Value: s3URL(c, b.S3)})
		env = append(env, s3Env(b.S3, "")...)
	}
	if r := c.Spec.Restore; r != nil {
		env = append(env,
			corev1.EnvVar{Name: "ISOMER_RESTORE_PREFIX", Value: s3URL(c, r.From)},
			corev1.EnvVar{Name: "ISOMER_RESTORE_POINT", Value: r.RestorePoint})
		env = append(env, s3Env(r.From, "ISOMER_RESTORE_")...)
	}
	return env
}

// ── database nodes ──────────────────────────────────────────────────────────

// isomerReady passes once the server accepts TCP connections and the isomer extension exists. The
// image's first-boot setup runs against a socket-only server, so this stays false until it is done.
var isomerReady = []string{"sh", "-c",
	`pg_isready -U postgres -h 127.0.0.1 -q && psql -U postgres -h /var/run/postgresql -tAc "SELECT 1 FROM pg_extension WHERE extname = 'isomer'" | grep -q 1`}

func databaseStatefulSet(c *isomerv1.IsomerCluster, role string, replicas int32) *appsv1.StatefulSet {
	n := names{c}
	name := n.setFor(role)
	kind := nodeKind(role)
	env := []corev1.EnvVar{
		secretEnv("POSTGRES_PASSWORD", n.credentials(), keyPostgresPassword),
		{Name: "ISOMER_ROLE", Value: kind},
	}
	if kind == roleCoordinator {
		env = append(env,
			corev1.EnvVar{Name: "ISOMER_COORDINATOR_HOST", Value: n.coordinatorHost()},
			secretEnv("ISOMER_MONGO_USER", n.credentials(), keyMongoUser),
			secretEnv("ISOMER_MONGO_PASSWORD", n.credentials(), keyMongoPassword),
		)
	}

	env = append(env, backupEnv(c)...)
	postgres := corev1.Container{
		Name:            "postgres",
		Image:           c.Spec.Image,
		ImagePullPolicy: c.Spec.ImagePullPolicy,
		Args:            postgresArgs(c, kind),
		Env:             env,
		Ports:           []corev1.ContainerPort{{Name: "postgres", ContainerPort: 5432}},
		Resources:       c.Spec.Resources,
		VolumeMounts: []corev1.VolumeMount{
			{Name: "data", MountPath: "/var/lib/postgresql"},
			{Name: "socket", MountPath: "/var/run/postgresql"},
			{Name: "dshm", MountPath: "/dev/shm"},
			{Name: "tls", MountPath: tlsDir, ReadOnly: true},
			{Name: "auth", MountPath: "/etc/isomerdb/auth", ReadOnly: true},
		},
		// Kubernetes stops containers with SIGTERM, which is Postgres's smart shutdown: it waits for
		// every client to leave, and Citus keeps connections cached, so it would run into the
		// grace period and be killed. A fast shutdown (SIGINT) finishes cleanly in well under a
		// second, after sending a standby all of its WAL. The hook only sends the signal: a hook
		// that waits for PID 1 to exit is itself left hanging until the grace period ends.
		Lifecycle: &corev1.Lifecycle{PreStop: &corev1.LifecycleHandler{Exec: &corev1.ExecAction{
			Command: []string{"sh", "-c", "kill -INT 1"},
		}}},
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
	if kind == roleCoordinator && c.Spec.Gateway.Enabled {
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
	// Before Postgres starts: keep a primary's data, clone or rewind a standby (isomer-node-init).
	initNode := corev1.Container{
		Name:            "node-init",
		Image:           c.Spec.Image,
		ImagePullPolicy: c.Spec.ImagePullPolicy,
		Command:         []string{"isomer-node-init"},
		SecurityContext: &corev1.SecurityContext{RunAsUser: ptr(int64(postgresUID)), RunAsGroup: ptr(int64(postgresUID))},
		Env: []corev1.EnvVar{
			{Name: "ISOMER_CLUSTER", Value: c.Name},
			{Name: "POD_NAMESPACE", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.namespace"}}},
			secretEnv("PGPASSWORD", n.credentials(), keyPostgresPassword),
		},
		VolumeMounts: []corev1.VolumeMount{
			{Name: "data", MountPath: "/var/lib/postgresql"},
			{Name: "tls", MountPath: tlsDir, ReadOnly: true},
			{Name: "topology", MountPath: "/etc/isomerdb/topology", ReadOnly: true},
		},
	}
	initNode.Env = append(initNode.Env, backupEnv(c)[1:]...) // ISOMER_CLUSTER is already set
	if b := c.Spec.Backup; b != nil {
		full := b.FullEvery.Duration
		if full <= 0 {
			full = 24 * time.Hour
		}
		retain := b.Retain
		if retain <= 0 {
			retain = 7
		}
		agentEnv := append(backupEnv(c),
			corev1.EnvVar{Name: "ISOMER_BACKUP_FULL_EVERY", Value: fmt.Sprint(int64(full.Seconds()))},
			corev1.EnvVar{Name: "ISOMER_BACKUP_RETAIN", Value: fmt.Sprint(retain)})
		containers = append(containers, corev1.Container{
			Name:            "backup",
			Image:           c.Spec.Image,
			ImagePullPolicy: c.Spec.ImagePullPolicy,
			Command:         []string{"isomer-backup-agent"},
			SecurityContext: &corev1.SecurityContext{RunAsUser: ptr(int64(postgresUID)), RunAsGroup: ptr(int64(postgresUID))},
			Env:             agentEnv,
			VolumeMounts: []corev1.VolumeMount{
				{Name: "data", MountPath: "/var/lib/postgresql"},
				{Name: "socket", MountPath: "/var/run/postgresql"},
			},
		})
	}
	podLabels := labels(c, role)
	podLabels[labelNode] = kind
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
				ObjectMeta: metav1.ObjectMeta{Labels: podLabels},
				Spec: corev1.PodSpec{
					SecurityContext:               &corev1.PodSecurityContext{FSGroup: &fsGroup},
					TerminationGracePeriodSeconds: ptr(int64(60)),
					InitContainers:                []corev1.Container{initNode},
					Containers:                    containers,
					Volumes: []corev1.Volume{
						// Keys are root-owned and group-readable (0640, group postgres via fsGroup),
						// which both the server and libpq accept.
						{Name: "tls", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: n.tlsSecret(), DefaultMode: ptr(int32(0o640))}}},
						{Name: "auth", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: n.auth()}}}},
						{Name: "topology", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: n.topology()}, Optional: ptr(true)}}},
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

// workerNetworkPolicy admits connections to workers (primaries and standbys) only from this
// cluster's own database pods and the operator: defence in depth on top of certificate auth.
func workerNetworkPolicy(c *isomerv1.IsomerCluster) *networkingv1.NetworkPolicy {
	database := &metav1.LabelSelector{
		MatchLabels:      map[string]string{"app.kubernetes.io/instance": c.Name},
		MatchExpressions: []metav1.LabelSelectorRequirement{{Key: labelNode, Operator: metav1.LabelSelectorOpExists}},
	}
	return &networkingv1.NetworkPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: names{c}.worker(), Namespace: c.Namespace, Labels: labels(c, roleWorker)},
		Spec: networkingv1.NetworkPolicySpec{
			PodSelector: metav1.LabelSelector{MatchLabels: map[string]string{"app.kubernetes.io/instance": c.Name, labelNode: roleWorker}},
			PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeIngress},
			Ingress: []networkingv1.NetworkPolicyIngressRule{{
				From: []networkingv1.NetworkPolicyPeer{
					{PodSelector: database},
					{
						NamespaceSelector: &metav1.LabelSelector{},
						PodSelector:       &metav1.LabelSelector{MatchLabels: map[string]string{"app.kubernetes.io/name": "isomerdb-operator"}},
					},
				},
			}},
		},
	}
}
