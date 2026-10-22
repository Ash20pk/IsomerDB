package main

import (
	"context"
	"fmt"
	"regexp"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"

	isomerv1 "github.com/Ash20pk/IsomerDB/operator/api/v1alpha1"
)

const (
	labelManaged = "isomerdb.io/managed-by"
	labelOrg     = "isomerdb.io/org"
)

// regions maps each region's name to a client for its Kubernetes cluster.
type regions map[string]client.Client

func connectRegions(rs []Region) (regions, error) {
	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(isomerv1.AddToScheme(scheme))
	out := regions{}
	for _, r := range rs {
		var cfg *rest.Config
		var err error
		if r.Kubeconfig == "" {
			cfg, err = rest.InClusterConfig()
			if err != nil {
				// Outside Kubernetes (development): the usual kubeconfig.
				cfg, err = clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
					clientcmd.NewDefaultClientConfigLoadingRules(), nil).ClientConfig()
			}
		} else {
			cfg, err = clientcmd.BuildConfigFromFlags("", r.Kubeconfig)
		}
		if err != nil {
			return nil, fmt.Errorf("region %s: %w", r.Name, err)
		}
		c, err := client.New(cfg, client.Options{Scheme: scheme})
		if err != nil {
			return nil, fmt.Errorf("region %s: %w", r.Name, err)
		}
		out[r.Name] = c
	}
	return out, nil
}

// Cluster names become Kubernetes object names (with suffixes such as -coordinator-replica).
var validName = regexp.MustCompile(`^[a-z]([a-z0-9-]{0,22}[a-z0-9])?$`)

func namespaceFor(orgID int64) string { return fmt.Sprintf("isomer-org-%d", orgID) }

// provision creates the cluster's namespace (once per organisation) and its IsomerCluster.
func provision(ctx context.Context, c client.Client, cfg Config, region Region, plan Plan, orgID int64, ns, name string) error {
	namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
		Name:   ns,
		Labels: map[string]string{labelManaged: "isomerdb-cloud", labelOrg: fmt.Sprint(orgID)},
	}}
	if err := c.Create(ctx, namespace); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("creating namespace %s: %w", ns, err)
	}
	ic := &isomerv1.IsomerCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: ns,
			Labels: map[string]string{labelManaged: "isomerdb-cloud", labelOrg: fmt.Sprint(orgID)},
		},
		Spec: isomerv1.IsomerClusterSpec{
			Image:   cfg.Image,
			Workers: plan.Workers,
			Storage: resource.MustParse(plan.Storage),
			Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse(plan.CPU),
				corev1.ResourceMemory: resource.MustParse(plan.Memory),
			}},
			Gateway:         isomerv1.ComponentSpec{Enabled: true},
			API:             isomerv1.ComponentSpec{Enabled: true, Image: cfg.APIImage},
			ImagePullPolicy: corev1.PullIfNotPresent,
			HA:              isomerv1.HASpec{Standbys: plan.Standbys},
			// A deleted managed cluster takes its data with it.
			DeleteVolumes: true,
		},
	}
	if region.StorageClassName != "" {
		sc := region.StorageClassName
		ic.Spec.StorageClassName = &sc
	}
	if err := c.Create(ctx, ic); err != nil {
		return fmt.Errorf("creating the cluster: %w", err)
	}
	return nil
}

func getCluster(ctx context.Context, c client.Client, ns, name string) (*isomerv1.IsomerCluster, error) {
	ic := &isomerv1.IsomerCluster{}
	if err := c.Get(ctx, client.ObjectKey{Namespace: ns, Name: name}, ic); err != nil {
		return nil, err
	}
	return ic, nil
}

func scaleCluster(ctx context.Context, c client.Client, ns, name string, workers int32) error {
	ic, err := getCluster(ctx, c, ns, name)
	if err != nil {
		return err
	}
	patch := client.MergeFrom(ic.DeepCopy())
	ic.Spec.Workers = workers
	return c.Patch(ctx, ic, patch)
}

func deleteCluster(ctx context.Context, c client.Client, ns, name string) error {
	ic := &isomerv1.IsomerCluster{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name}}
	if err := c.Delete(ctx, ic); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return nil
}

// password reads the cluster's postgres password from the Secret the operator generated.
func password(ctx context.Context, c client.Client, ic *isomerv1.IsomerCluster) (string, error) {
	name := ic.Status.Endpoints.CredentialsSecret
	if name == "" {
		name = ic.Name + "-credentials"
	}
	s := &corev1.Secret{}
	if err := c.Get(ctx, client.ObjectKey{Namespace: ic.Namespace, Name: name}, s); err != nil {
		return "", err
	}
	return string(s.Data["postgres-password"]), nil
}

func mongoCredentials(ctx context.Context, c client.Client, ic *isomerv1.IsomerCluster) (user, pass string, err error) {
	name := ic.Status.Endpoints.CredentialsSecret
	if name == "" {
		name = ic.Name + "-credentials"
	}
	s := &corev1.Secret{}
	if err = c.Get(ctx, client.ObjectKey{Namespace: ic.Namespace, Name: name}, s); err != nil {
		return "", "", err
	}
	return string(s.Data["mongo-user"]), string(s.Data["mongo-password"]), nil
}
