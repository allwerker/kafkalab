// Package domain creates realistic synthetic orders: a product catalogue with popular and
// niche items, returning customers, regions and payment methods.
package domain

import (
	"fmt"
	"math/rand/v2"
)

// Product is one catalogue entry.
type Product struct {
	SKU        string
	PriceCents int64
	Weight     float64 // relative popularity
}

// Line is one order line.
type Line struct {
	SKU        string
	Quantity   int
	PriceCents int64
}

// Order is a generated order before it becomes an event.
type Order struct {
	CustomerID    string
	Region        string
	PaymentMethod string
	Lines         []Line
	TotalCents    int64
}

// Catalogue is the fixed product range (40 SKUs, Zipf-like popularity).
func Catalogue() []Product {
	ps := make([]Product, 40)
	for i := range ps {
		ps[i] = Product{
			SKU:        fmt.Sprintf("sku-%03d", i+1),
			PriceCents: int64(499 + (i*7919)%24000),
			Weight:     1 / float64(i+1),
		}
	}
	return ps
}

// Generator draws orders from the catalogue.
type Generator struct {
	products []Product
	total    float64
	rnd      *rand.Rand
}

// NewGenerator returns a generator seeded for reproducible runs.
func NewGenerator(seed uint64) *Generator {
	g := &Generator{products: Catalogue(), rnd: rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))}
	for _, p := range g.products {
		g.total += p.Weight
	}
	return g
}

var (
	regions = []string{"NORTH", "SOUTH", "EAST", "WEST"}
	// Payment method shares: card 55%, PayPal 25%, invoice 12%, wallet 8%.
	methods = []struct {
		name  string
		share float64
	}{{"CARD", 0.55}, {"PAYPAL", 0.25}, {"INVOICE", 0.12}, {"WALLET", 0.08}}
)

// Next creates an order. popularSKU (if not empty) is chosen for the first line with
// probability popularProbability ("Flash Sale").
func (g *Generator) Next(popularSKU string, popularProbability float64) Order {
	o := Order{
		CustomerID:    fmt.Sprintf("cust-%05d", g.rnd.IntN(20_000)),
		Region:        regions[g.rnd.IntN(len(regions))],
		PaymentMethod: g.method(),
	}
	n := 1 + g.rnd.IntN(3)
	for i := 0; i < n; i++ {
		p := g.product()
		if i == 0 && popularSKU != "" && g.rnd.Float64() < popularProbability {
			p = g.find(popularSKU)
		}
		q := 1 + g.rnd.IntN(2)
		o.Lines = append(o.Lines, Line{SKU: p.SKU, Quantity: q, PriceCents: p.PriceCents})
		o.TotalCents += int64(q) * p.PriceCents
	}
	return o
}

func (g *Generator) method() string {
	r := g.rnd.Float64()
	for _, m := range methods {
		if r < m.share {
			return m.name
		}
		r -= m.share
	}
	return methods[len(methods)-1].name
}

func (g *Generator) product() Product {
	r := g.rnd.Float64() * g.total
	for _, p := range g.products {
		if r < p.Weight {
			return p
		}
		r -= p.Weight
	}
	return g.products[len(g.products)-1]
}

func (g *Generator) find(sku string) Product {
	for _, p := range g.products {
		if p.SKU == sku {
			return p
		}
	}
	return Product{SKU: sku, PriceCents: 1999}
}

// Float64 exposes the generator's random source for scenario decisions.
func (g *Generator) Float64() float64 { return g.rnd.Float64() }
