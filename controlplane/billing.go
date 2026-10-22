package main

import (
	"math"
	"sort"
)

// InvoiceLine is one priced item: a quantity of a unit, for one cluster.
type InvoiceLine struct {
	Cluster     string  `json:"cluster"`
	Plan        string  `json:"plan"`
	Item        string  `json:"item"`
	Quantity    float64 `json:"quantity"`
	Unit        string  `json:"unit"`
	UnitPrice   float64 `json:"unit_price_usd"`
	AmountUSD   float64 `json:"amount_usd"`
	Description string  `json:"description,omitempty"`
}

type Invoice struct {
	Month    string        `json:"month"`
	Lines    []InvoiceLine `json:"lines"`
	TotalUSD float64       `json:"total_usd"`
}

const hoursPerMonth = 730.0

func cents(x float64) float64 { return math.Round(x*100) / 100 }

// invoice prices an organisation's hourly usage by each cluster's plan:
//   - node-hours of the coordinator and workers (as each cluster meters them), and of their
//     standbys, which the database can't see: one per node with the plan's standbys;
//   - storage, from each hour's peak, as GB-months;
//   - embeddings on bundled models, at what the cluster's metering bills (list price plus
//     markup, past the free allowance). Embeddings on the customer's own keys bill nothing here.
func invoice(month string, usage map[string][]UsageRow, plans map[string]string, catalog Config) Invoice {
	inv := Invoice{Month: month, Lines: []InvoiceLine{}}
	names := make([]string, 0, len(usage))
	for name := range usage {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		plan, _ := catalog.plan(plans[name])
		var nodeHours, gbHours, billed float64
		for _, u := range usage[name] {
			switch u.Metric {
			case "node_hours":
				nodeHours += u.Value
			case "storage_bytes":
				gbHours += u.Value / 1e9
			case "embedding_billed_usd":
				billed += u.Value
			}
		}
		add := func(item string, qty float64, unit string, price float64, desc string) {
			if qty <= 0 {
				return
			}
			inv.Lines = append(inv.Lines, InvoiceLine{Cluster: name, Plan: plan.Name, Item: item, Quantity: math.Round(qty*1000) / 1000,
				Unit: unit, UnitPrice: price, AmountUSD: cents(qty * price), Description: desc})
		}
		add("compute", nodeHours, "node-hours", plan.NodeHourUSD, "coordinator and worker nodes")
		add("standbys", nodeHours*float64(plan.Standbys), "node-hours", plan.NodeHourUSD, "a standby beside every node")
		add("storage", gbHours/hoursPerMonth, "GB-months", plan.StorageGBMonthUSD, "data across every node, hourly peak")
		add("embeddings", billed, "USD", 1, "bundled embedding models")
	}
	for _, l := range inv.Lines {
		inv.TotalUSD += l.AmountUSD
	}
	inv.TotalUSD = cents(inv.TotalUSD)
	return inv
}
