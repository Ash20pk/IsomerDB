package main

import (
	"testing"
	"time"
)

func TestInvoicePricesUsageByPlan(t *testing.T) {
	cfg := defaultConfig()
	hour := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	usage := map[string][]UsageRow{
		"app": {
			{Hour: hour, Metric: "node_hours", Dimension: "coordinator", Value: 10},
			{Hour: hour, Metric: "node_hours", Dimension: "worker", Value: 20},
			{Hour: hour, Metric: "storage_bytes", Value: 73e9},                // 73 GB for one hour
			{Hour: hour.Add(time.Hour), Metric: "storage_bytes", Value: 73e9}, // and another
			{Hour: hour, Metric: "embedding_billed_usd", Dimension: "bundled-small", Value: 1.5},
			{Hour: hour, Metric: "sql_statements", Value: 1e6}, // not billed
		},
		"tiny": {{Hour: hour, Metric: "node_hours", Dimension: "coordinator", Value: 4}},
	}
	inv := invoice("2026-10", usage, map[string]string{"app": "standard", "tiny": "dev"}, cfg)

	want := map[string]float64{
		"app/compute":    30 * 0.20,          // 6.00
		"app/standbys":   30 * 1 * 0.20,      // one standby per node: 6.00
		"app/storage":    146.0 / 730 * 0.15, // 0.03
		"app/embeddings": 1.5,                // passed through
		"tiny/compute":   4 * 0.05,           // 0.20
	}
	if len(inv.Lines) != len(want) {
		t.Fatalf("lines: got %d, want %d: %+v", len(inv.Lines), len(want), inv.Lines)
	}
	total := 0.0
	for _, l := range inv.Lines {
		w, ok := want[l.Cluster+"/"+l.Item]
		if !ok {
			t.Errorf("unexpected line %+v", l)
			continue
		}
		if l.AmountUSD != cents(w) {
			t.Errorf("%s/%s: got %.2f, want %.2f", l.Cluster, l.Item, l.AmountUSD, cents(w))
		}
		total += cents(w)
	}
	if inv.TotalUSD != cents(total) {
		t.Errorf("total: got %.2f, want %.2f", inv.TotalUSD, cents(total))
	}
}

func TestClusterNames(t *testing.T) {
	for name, ok := range map[string]bool{
		"app": true, "a": true, "my-app-2": true, "a-very-long-name-1234567": true,
		"App": false, "-app": false, "app-": false, "1app": false, "a-very-long-name-12345678": false, "": false,
	} {
		if validName.MatchString(name) != ok {
			t.Errorf("%q: want valid=%v", name, ok)
		}
	}
}

func TestDefaultPlansAreValid(t *testing.T) {
	for _, p := range defaultConfig().Plans {
		if err := p.validate(); err != nil {
			t.Error(err)
		}
	}
}
