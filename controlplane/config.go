package main

import (
	"fmt"

	"k8s.io/apimachinery/pkg/api/resource"
)

// Config is what the control plane offers: where clusters run, and the plans they run on.
type Config struct {
	// Image of the database, and of the HTTP API, for new clusters.
	Image    string   `json:"image"`
	APIImage string   `json:"apiImage"`
	Regions  []Region `json:"regions"`
	Plans    []Plan   `json:"plans"`
}

// Region is a Kubernetes cluster that clusters are created in.
type Region struct {
	Name    string `json:"name"`
	Display string `json:"display"`
	// Kubeconfig is a kubeconfig file for the region's Kubernetes cluster; empty means the one
	// the control plane runs in.
	Kubeconfig string `json:"kubeconfig,omitempty"`
	// StorageClassName for data volumes; empty uses the region's default.
	StorageClassName string `json:"storageClassName,omitempty"`
}

// Plan is a size and a price. Prices follow the PRD: compute by the node-hour and storage by the
// GB-month, the same units as the database itself (decision 6); embeddings on bundled models are
// passed through at what the cluster's own metering bills.
type Plan struct {
	Name       string `json:"name"`
	Workers    int32  `json:"workers"`    // worker nodes a new cluster starts with
	MaxWorkers int32  `json:"maxWorkers"` // the most it can scale to
	Standbys   int32  `json:"standbys"`   // 1: a standby beside every node, with failover
	Storage    string `json:"storage"`    // per node
	CPU        string `json:"cpu"`        // requested per node
	Memory     string `json:"memory"`     // requested per node
	// Prices, in USD.
	NodeHourUSD       float64 `json:"nodeHourUSD"`
	StorageGBMonthUSD float64 `json:"storageGBMonthUSD"`
}

func (p Plan) validate() error {
	for _, q := range []string{p.Storage, p.CPU, p.Memory} {
		if _, err := resource.ParseQuantity(q); err != nil {
			return fmt.Errorf("plan %s: %q: %w", p.Name, q, err)
		}
	}
	if p.Workers > p.MaxWorkers || p.Standbys < 0 || p.Standbys > 1 {
		return fmt.Errorf("plan %s: workers must be at most maxWorkers, and standbys 0 or 1", p.Name)
	}
	return nil
}

func defaultConfig() Config {
	return Config{
		Image:    "isomerdb:dev",
		APIImage: "isomer-api:dev",
		Regions:  []Region{{Name: "local-1", Display: "This Kubernetes cluster"}},
		Plans: []Plan{
			{Name: "dev", Workers: 0, MaxWorkers: 2, Standbys: 0, Storage: "2Gi", CPU: "200m", Memory: "512Mi",
				NodeHourUSD: 0.05, StorageGBMonthUSD: 0.15},
			{Name: "standard", Workers: 2, MaxWorkers: 16, Standbys: 1, Storage: "20Gi", CPU: "1", Memory: "4Gi",
				NodeHourUSD: 0.20, StorageGBMonthUSD: 0.15},
		},
	}
}

func (c Config) plan(name string) (Plan, bool) {
	for _, p := range c.Plans {
		if p.Name == name {
			return p, true
		}
	}
	return Plan{}, false
}

func (c Config) region(name string) (Region, bool) {
	for _, r := range c.Regions {
		if r.Name == name {
			return r, true
		}
	}
	return Region{}, false
}
