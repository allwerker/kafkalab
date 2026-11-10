package domain

import "testing"

func Test_shouldOfferFortyProductsWithPositivePricesWhenTheCatalogueIsLoaded(t *testing.T) {
	// GIVEN / WHEN: the catalogue
	ps := Catalogue()

	// THEN: it has 40 priced products, the first being the most popular
	if len(ps) != 40 || ps[0].Weight <= ps[39].Weight {
		t.Fatalf("unexpected catalogue")
	}
	for _, p := range ps {
		if p.PriceCents <= 0 {
			t.Fatalf("%s has no price", p.SKU)
		}
	}
}

func Test_shouldProduceConsistentTotalsWhenOrdersAreGenerated(t *testing.T) {
	// GIVEN: a seeded generator
	g := NewGenerator(42)

	// WHEN: a thousand orders are generated
	methods := map[string]int{}
	for i := 0; i < 1000; i++ {
		o := g.Next("", 0)
		var sum int64
		for _, l := range o.Lines {
			sum += int64(l.Quantity) * l.PriceCents
		}

		// THEN: each total matches its lines and has one to three lines
		if sum != o.TotalCents || len(o.Lines) < 1 || len(o.Lines) > 3 || o.CustomerID == "" || o.Region == "" {
			t.Fatalf("inconsistent order %+v", o)
		}
		methods[o.PaymentMethod]++
	}
	// AND: card is the most common payment method and all four occur
	if len(methods) != 4 || methods["CARD"] < methods["PAYPAL"] {
		t.Fatalf("unexpected method mix %v", methods)
	}
}

func Test_shouldReproduceTheSameOrdersWhenTheSameSeedIsUsed(t *testing.T) {
	// GIVEN: two generators with the same seed
	a, b := NewGenerator(7), NewGenerator(7)

	// WHEN / THEN: they generate identical orders, so baseline runs are comparable
	for i := 0; i < 50; i++ {
		if a.Next("", 0).TotalCents != b.Next("", 0).TotalCents {
			t.Fatal("seeded generators diverged")
		}
	}
}

func Test_shouldConcentrateDemandOnOneProductWhenAFlashSaleRuns(t *testing.T) {
	// GIVEN: a flash sale on a niche product with 70% probability
	g := NewGenerator(1)

	// WHEN: 1,000 orders are generated
	hits := 0
	for i := 0; i < 1000; i++ {
		if g.Next("sku-040", 0.7).Lines[0].SKU == "sku-040" {
			hits++
		}
	}

	// THEN: roughly 70% of orders start with that product
	if hits < 640 || hits > 760 {
		t.Fatalf("expected ~700 flash sale orders, got %d", hits)
	}
}

func Test_shouldUseAFallbackPriceWhenTheFlashSaleProductIsUnknown(t *testing.T) {
	// GIVEN: a flash sale on a SKU that is not in the catalogue
	g := NewGenerator(3)

	// WHEN: an order is generated with certainty for that SKU
	o := g.Next("sku-999", 1)

	// THEN: the order still has a valid price
	if o.Lines[0].SKU != "sku-999" || o.Lines[0].PriceCents != 1999 {
		t.Fatalf("unexpected %+v", o.Lines[0])
	}
}
