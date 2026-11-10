package dash

import g "kafkalab/monitoring/dashgen/internal/grafana"

// Business is the commerce business dashboard (uid lab-business).
func Business() *g.Dashboard {
	d := demo("lab-business", "Kafka Lab · Commerce Business",
		"The shop as a business sees it: the order funnel from creation to notification, revenue, payment and reservation success, end-to-end order completion time and dead-lettered records. Correlate dips with the orange chaos markers.")
	d.Rows = []*g.Row{
		row("", false,
			g.Stat("Orders / min", "Orders created per minute by the generator and accepted by Kafka. The heartbeat of the shop: compare with the target rate on Service & Client Runtime.", "short",
				q(`sum(rate(lab_orders_created_total[$__rate_interval])) * 60`, "")).Hue(g.Blue).Neutral(),
			g.Stat("Revenue / min", "Value of shipped orders per minute (lab_revenue_cents_total).", "currencyEUR",
				q(`sum(rate(lab_revenue_cents_total[$__rate_interval])) * 60 / 100`, "")).Hue(g.Green).Neutral(),
			g.Stat("Revenue vs 1 h ago", "Revenue rate of the last 15 minutes divided by the same window one hour earlier. Below 80 % for a while fires LabRevenueDrop.", "percentunit",
				q(`sum(rate(lab_revenue_cents_total[15m])) / sum(rate(lab_revenue_cents_total[15m] offset 1h))`, "")).
				Thresholds(g.Red, g.Step{Color: g.Yellow, Value: 0.8}, g.Step{Color: g.Green, Value: 0.95}).Empty("needs 1 h of data"),
			g.Stat("Payment success", "Completed ÷ all payments, last 5 minutes. The simulated payment provider fails some payments on purpose; chaos can make it worse.", "percentunit",
				q(`sum(rate(lab_payments_total{result="completed"}[5m])) / sum(rate(lab_payments_total[5m]))`, "")).
				Thresholds(g.Red, g.Step{Color: g.Yellow, Value: 0.8}, g.Step{Color: g.Green, Value: 0.9}).SetMax(1),
			g.Stat("Orders in flight", "Created minus shipped minus cancelled orders (counters since service start): orders still being processed somewhere.", "short",
				q(`sum(lab_orders_created_total) - sum(lab_orders_shipped_total) - sum(lab_orders_cancelled_total)`, "")).Hue(g.Purple).Neutral(),
			g.Stat("Completion p95", "95th percentile time from order creation to shipped/cancelled.", "s",
				g.Target{Expr: `histogram_quantile(0.95, sum by (vmrange) (rate(lab_order_completion_seconds_bucket[5m])))`}).
				Using(g.VMNative).Thresholds(g.Green, g.Step{Color: g.Yellow, Value: 10}, g.Step{Color: g.Red, Value: 30}),
		),
		row("Order funnel", false,
			g.BarGauge("Funnel (selected time range)",
				"How many orders reached each step within the time range: created → paid → stock reserved → shipped, plus cancelled and notifications sent. Big drops between steps show where orders get stuck or fail.",
				"short",
				q(`sum(increase(lab_orders_created_total[$__range]))`, "1 created"),
				q(`sum(increase(lab_payments_total{result="completed"}[$__range]))`, "2 paid"),
				q(`sum(increase(lab_reservations_total{result="reserved"}[$__range]))`, "3 reserved"),
				q(`sum(increase(lab_orders_shipped_total[$__range]))`, "4 shipped"),
				q(`sum(increase(lab_orders_cancelled_total[$__range]))`, "4 cancelled"),
				q(`sum(increase(lab_notifications_total[$__range]))`, "5 notified")).
				Hue(g.Blue).Neutral().Opt("reduceOptions", map[string]any{"calcs": []string{"lastNotNull"}, "fields": "", "values": false}).
				Opt("displayMode", "basic").SeriesColor("4 cancelled", g.Orange).Size(8, 9),
			g.TimeSeries("Funnel steps per minute",
				"Throughput of each funnel step per minute. In a healthy shop the lines run in parallel with a small delay; a step that falls behind is the bottleneck (check its service on Service & Client Runtime).",
				"short",
				q(`sum(rate(lab_orders_created_total[$__rate_interval])) * 60`, "created"),
				q(`sum(rate(lab_payments_total{result="completed"}[$__rate_interval])) * 60`, "paid"),
				q(`sum(rate(lab_reservations_total{result="reserved"}[$__rate_interval])) * 60`, "reserved"),
				q(`sum(rate(lab_orders_shipped_total[$__rate_interval])) * 60`, "shipped"),
				q(`sum(rate(lab_orders_cancelled_total[$__rate_interval])) * 60`, "cancelled"),
				q(`sum(rate(lab_notifications_total[$__rate_interval])) * 60`, "notified")).
				SeriesColor("cancelled", g.Orange).Hue(g.Blue).Size(16, 9),
		),
		row("Money", false,
			g.TimeSeries("Revenue per minute by region",
				"Value of shipped orders per minute, stacked by region, and the total one hour earlier (dashed) for comparison.",
				"currencyEUR",
				q(`sum by (region) (rate(lab_revenue_cents_total[$__rate_interval])) * 60 / 100`, "{{region}}"),
				q(`sum(rate(lab_revenue_cents_total[$__rate_interval] offset 1h)) * 60 / 100`, "total 1 h ago")).
				Stack().Over("byName", "total 1 h ago", map[string]any{"custom.stacking": map[string]any{"mode": "none"}, "custom.fillOpacity": 0, "custom.lineStyle": map[string]any{"fill": "dash", "dash": []int{10, 10}}, "color": map[string]any{"mode": "fixed", "fixedColor": g.Text}}).
				Hue(g.Green).Size(12, 8),
			g.TimeSeries("Payments by result",
				"Completed and failed payments per minute; failures broken down by reason. A jump in one reason after a chaos experiment shows what the experiment did.",
				"short",
				q(`sum(rate(lab_payments_total{result="completed"}[$__rate_interval])) * 60`, "completed"),
				q(`sum by (reason) (rate(lab_payments_total{result="failed"}[$__rate_interval])) * 60`, "failed · {{reason}}")).
				SeriesColor("completed", g.Green).Over("byRegexp", "failed.*", map[string]any{"color": map[string]any{"mode": "shades", "fixedColor": g.Red}}).
				Stack().Size(12, 8),
			g.TimeSeries("Stock reservations",
				"Reserved vs rejected stock reservations per minute (rejected = out of stock or invalid), and releases after cancellations.",
				"short",
				q(`sum(rate(lab_reservations_total{result="reserved"}[$__rate_interval])) * 60`, "reserved"),
				q(`sum by (reason) (rate(lab_reservations_total{result="rejected"}[$__rate_interval])) * 60`, "rejected · {{reason}}"),
				q(`sum(rate(lab_releases_total[$__rate_interval])) * 60`, "released")).
				SeriesColor("reserved", g.Green).SeriesColor("released", g.Purple).Over("byRegexp", "rejected.*", map[string]any{"color": map[string]any{"mode": "fixed", "fixedColor": g.Orange}}).
				Size(12, 8),
			g.TimeSeries("Refunds",
				"Refunds per minute and refunded value. Refunds happen when an order is cancelled after its payment completed.",
				"short",
				q(`sum(rate(lab_refunds_total[$__rate_interval])) * 60`, "refunds / min")).
				Hue(g.Orange).Size(12, 8),
		),
		row("Order completion time", false,
			g.Heatmap("Order completion time distribution",
				"Time from order creation to shipped or cancelled as a heatmap of VictoriaMetrics histogram buckets (vmrange, converted with prometheus_buckets, top 20 buckets). Healthy: one tight band of a few seconds. A second band higher up = some orders take a slow path (retries, lag).",
				"s",
				g.Target{Expr: `buckets_limit(20, prometheus_buckets(sum by (vmrange) (rate(lab_order_completion_seconds_bucket[$__rate_interval]))))`, Legend: "{{le}}", Format: "heatmap"}).
				Using(g.VMNative).Opt("yAxis", map[string]any{"axisPlacement": "left", "unit": "s"}).Size(14, 9),
			g.TimeSeries("Completion p50 / p95 / p99 by outcome",
				"Order completion percentiles per outcome (shipped vs cancelled), MetricsQL histogram_quantile on vmrange buckets.",
				"s",
				g.Target{Expr: `histogram_quantile(0.5, sum by (vmrange, outcome) (rate(lab_order_completion_seconds_bucket[5m])))`, Legend: "p50 · {{outcome}}"},
				g.Target{Expr: `histogram_quantile(0.95, sum by (vmrange, outcome) (rate(lab_order_completion_seconds_bucket[5m])))`, Legend: "p95 · {{outcome}}"},
				g.Target{Expr: `histogram_quantile(0.99, sum by (vmrange, outcome) (rate(lab_order_completion_seconds_bucket[5m])))`, Legend: "p99 · {{outcome}}"}).
				Using(g.VMNative).Thresholds(g.Green, g.Step{Color: g.Yellow, Value: 10}, g.Step{Color: g.Red, Value: 30}).Lines("dashed").
				Hue(g.Purple).Size(10, 9),
		),
		row("Dead letters & duplicates", false,
			g.TimeSeries("Dead-lettered records",
				"Records the services gave up on and wrote to a dead-letter topic, by source topic and by reason. Poison messages from the generator end up here by design; anything else is a bug or a chaos effect.",
				"short",
				q(`sum by (source_topic) (increase(lab_dlq_total[$__rate_interval]))`, "analytics · {{source_topic}}"),
				q(`sum by (topic, reason) (increase(lab_dlq_records_total[$__rate_interval]))`, "{{topic}} · {{reason}}")).
				Hue(g.Red).Bars().Size(12, 8),
			g.TimeSeries("Duplicates detected",
				"Duplicate events the idempotent consumers recognised and skipped (generator sends some on purpose; retries after rebalances add more).",
				"short",
				q(`sum by (topic) (increase(lab_duplicates_total[$__rate_interval]))`, "{{topic}}")).
				Hue(g.Yellow).Bars().Size(12, 8),
			g.TimeSeries("Notifications per channel",
				"Notifications sent per minute by channel. If shipped/cancelled orders keep coming but notifications fall behind, the notification service is lagging (LabNotificationBacklog).",
				"short",
				q(`sum by (channel) (rate(lab_notifications_total[$__rate_interval])) * 60`, "{{channel}}")).
				Hue(g.Blue).Size(12, 8),
			g.BarGauge("Stock of the top SKUs",
				"Units in stock for the 20 most popular SKUs (inventory service). Low stock leads to rejected reservations.",
				"short",
				iq(`sort(lab_stock_units)`, "{{sku}}")).
				Thresholds(g.Red, g.Step{Color: g.Yellow, Value: 10}, g.Step{Color: g.Green, Value: 50}).Size(12, 8),
		),
	}
	return d
}
