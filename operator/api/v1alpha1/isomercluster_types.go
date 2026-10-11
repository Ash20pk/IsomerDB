package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// IsomerClusterSpec is what an IsomerDB cluster should look like.
type IsomerClusterSpec struct {
	// Image of the database (Postgres + Citus + DocumentDB + isomer), also used for the MongoDB gateway.
	// +kubebuilder:default="isomerdb:dev"
	Image string `json:"image,omitempty"`

	// Workers is the number of Citus worker nodes. 0 runs the coordinator alone. Scaling up adds
	// the nodes and rebalances online; scaling down drains a node's shards before removing it.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:default=0
	Workers int32 `json:"workers"`

	// Storage for each node's data volume.
	// +kubebuilder:default="10Gi"
	Storage resource.Quantity `json:"storage,omitempty"`

	// StorageClassName for the data volumes; empty uses the cluster default.
	// +optional
	StorageClassName *string `json:"storageClassName,omitempty"`

	// Resources for each database container.
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`

	// Gateway runs the MongoDB wire-protocol gateway next to the coordinator.
	// +kubebuilder:default={enabled:true}
	Gateway ComponentSpec `json:"gateway,omitempty"`

	// API runs the HTTP API (projections by key, time travel, change feeds, live streams).
	// +kubebuilder:default={enabled:true,image:"isomer-api:dev"}
	API ComponentSpec `json:"api,omitempty"`

	// Embedder runs the service that computes embeddings for vector projections.
	// +kubebuilder:default={enabled:false,image:"isomer-embedder:dev"}
	Embedder ComponentSpec `json:"embedder,omitempty"`

	// ImagePullPolicy for every container.
	// +kubebuilder:default=IfNotPresent
	ImagePullPolicy corev1.PullPolicy `json:"imagePullPolicy,omitempty"`

	// HA runs a streaming standby beside the coordinator and every worker, and fails over to it.
	// +optional
	HA HASpec `json:"ha,omitempty"`

	// DeleteVolumes removes the data volumes when the IsomerCluster is deleted. Off by default:
	// deleting the resource keeps the data.
	// +optional
	DeleteVolumes bool `json:"deleteVolumes,omitempty"`
}

// HASpec configures standbys and failover.
type HASpec struct {
	// Standbys per node: 0 (no standbys) or 1. Each standby streams from its node's primary and
	// takes over when the primary stays unready for FailoverAfter.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=1
	// +optional
	Standbys int32 `json:"standbys,omitempty"`

	// FailoverAfter is how long a primary may be unready before its standby is promoted.
	// +kubebuilder:default="30s"
	// +optional
	FailoverAfter metav1.Duration `json:"failoverAfter,omitempty"`

	// Synchronous makes every commit wait until its node's standby has the WAL, so a failover
	// loses nothing. Writes then stop while a standby is down. Off by default (asynchronous: a
	// failover can lose the last moments of commits).
	// +optional
	Synchronous bool `json:"synchronous,omitempty"`

	// MaxRetainedWAL caps the WAL a primary keeps for a disconnected standby. Past it, the
	// standby's slot is invalidated and the operator re-clones the standby.
	// +kubebuilder:default="4GB"
	// +optional
	MaxRetainedWAL string `json:"maxRetainedWAL,omitempty"`
}

// ComponentSpec turns a stateless component on or off.
type ComponentSpec struct {
	Enabled bool `json:"enabled"`
	// +optional
	Image string `json:"image,omitempty"`
	// +optional
	Replicas *int32 `json:"replicas,omitempty"`
	// Env adds variables to the component's container (e.g. provider API keys for the embedder).
	// +optional
	Env []corev1.EnvVar `json:"env,omitempty"`
}

// WorkerStatus is one worker node as the cluster sees it.
type WorkerStatus struct {
	Name string `json:"name"`
	// Ready: the pod passes its readiness check.
	Ready bool `json:"ready"`
	// Registered: the node is in Citus's metadata.
	Registered bool `json:"registered"`
	// Draining: shards are being moved off before the node is removed.
	Draining bool  `json:"draining,omitempty"`
	Shards   int64 `json:"shards"`
}

// GroupStatus is one node group: a primary and, with spec.ha, its standby.
type GroupStatus struct {
	// Name: coordinator-0, worker-0, worker-1, ...
	Name string `json:"name"`
	// Primary is the pod that is the group's primary.
	Primary string `json:"primary"`
	// Standby is the pod that follows it, if any.
	Standby string `json:"standby,omitempty"`
	// StandbyStreaming: the standby is connected and replaying the primary's WAL.
	StandbyStreaming bool `json:"standbyStreaming,omitempty"`
	// ReplayLagBytes is how far the standby's replay is behind the primary.
	ReplayLagBytes int64 `json:"replayLagBytes,omitempty"`
}

// Failover is the last time a standby was promoted.
type Failover struct {
	Group  string      `json:"group"`
	From   string      `json:"from"`
	To     string      `json:"to"`
	At     metav1.Time `json:"at"`
	Reason string      `json:"reason"`
}

// IsomerClusterStatus is what the operator last observed.
type IsomerClusterStatus struct {
	// Phase: Provisioning, Ready, Scaling, or Degraded.
	Phase string `json:"phase,omitempty"`
	// Workers that are registered with Citus and ready.
	ReadyWorkers int32          `json:"readyWorkers"`
	Workers      []WorkerStatus `json:"workers,omitempty"`
	// Rebalance is the state of the last Citus rebalance job, if any.
	Rebalance string `json:"rebalance,omitempty"`
	// LastRepairedJob is the last rebalance job after which capture triggers were re-checked.
	LastRepairedJob int64 `json:"lastRepairedJob,omitempty"`
	// Groups: each node group's primary and standby.
	Groups []GroupStatus `json:"groups,omitempty"`
	// LastFailover is the last standby promotion, planned or not.
	LastFailover *Failover `json:"lastFailover,omitempty"`
	// IsomerVersion is the isomer extension version on the coordinator.
	IsomerVersion string `json:"isomerVersion,omitempty"`
	// CertificatesIssuedAt is when the node certificates were last issued. Nodes reload their
	// configuration for a few minutes afterwards, as the new files reach their volumes.
	CertificatesIssuedAt *metav1.Time `json:"certificatesIssuedAt,omitempty"`
	// Endpoints clients connect to.
	Endpoints          Endpoints          `json:"endpoints,omitempty"`
	Conditions         []metav1.Condition `json:"conditions,omitempty"`
	ObservedGeneration int64              `json:"observedGeneration,omitempty"`
}

type Endpoints struct {
	Postgres string `json:"postgres,omitempty"`
	MongoDB  string `json:"mongodb,omitempty"`
	API      string `json:"api,omitempty"`
	// CredentialsSecret holds the postgres and MongoDB passwords.
	CredentialsSecret string `json:"credentialsSecret,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:subresource:scale:specpath=.spec.workers,statuspath=.status.readyWorkers
// +kubebuilder:resource:shortName=isomer
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Workers",type=integer,JSONPath=`.spec.workers`
// +kubebuilder:printcolumn:name="Ready",type=integer,JSONPath=`.status.readyWorkers`
// +kubebuilder:printcolumn:name="Rebalance",type=string,JSONPath=`.status.rebalance`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// IsomerCluster is an IsomerDB cluster: a coordinator, Citus workers, the MongoDB gateway, and the
// HTTP API.
type IsomerCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   IsomerClusterSpec   `json:"spec,omitempty"`
	Status IsomerClusterStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// IsomerClusterList is a list of IsomerClusters.
type IsomerClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []IsomerCluster `json:"items"`
}

func init() {
	SchemeBuilder.Register(&IsomerCluster{}, &IsomerClusterList{})
}
