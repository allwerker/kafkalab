package experiments

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"kafkalab/controlboard/backend/internal/api"
	"kafkalab/controlboard/backend/internal/k8s"
	"kafkalab/pkg/contracts"
)

// The scenario library (draft-for-load-generators.md §4). Ids and parameter keys match the
// controlboard UI (controlboard/api/fixtures/scenarios.json).

const (
	gen          = contracts.ServiceOrderGenerator
	baseRate     = 50.0 // order-generator default orders/s
	baseLatency  = 5.0  // inventory's normal processing time per record (ms)
	defaultSku   = "sku-001"
	tsunamiStep  = 10 * time.Second
	kubeSelector = "app.kubernetes.io/name="
)

var consumers = []string{contracts.ServicePayment, contracts.ServiceInventory, contracts.ServiceShipping,
	contracts.ServiceNotification, contracts.ServiceAnalytics}

var regions = map[string][]string{
	"region-a (payment, inventory)":     {contracts.ServicePayment, contracts.ServiceInventory},
	"region-b (shipping, notification)": {contracts.ServiceShipping, contracts.ServiceNotification},
}

func dashboards(cfg *Config, uids ...string) []api.DashboardLink {
	titles := map[string]string{
		"kafka-lag": "Consumer lag", "lab-business": "Business KPIs", "lab-services": "Services",
		"kafka-broker": "Kafka brokers", "kafka-topics": "Topics", "lab-postgres": "PostgreSQL",
		"lab-chaos": "Chaos experiments", "kafka-overview": "Kafka overview", "kafka-e2e": "End-to-end latency",
		"kafka-kraft": "KRaft controller", "kafka-pods": "Kafka pods",
	}
	out := make([]api.DashboardLink, 0, len(uids))
	for _, uid := range uids {
		out = append(out, api.DashboardLink{Title: titles[uid], URL: strings.TrimRight(cfg.GrafanaURL, "/") + "/d/" + uid})
	}
	return out
}

func overlay(svc string, v api.Values) map[string]api.Values { return map[string]api.Values{svc: v} }

func catalog(cfg *Config) []*Scenario {
	return []*Scenario{
		// ---- traffic
		{Drill: "easy", Def: api.ScenarioDef{ID: "black-friday", Name: "Black Friday", Category: "traffic", Severity: "high",
			Description:        "Orders jump from 50/s to the configured rate; payment and inventory get slower under load.",
			DefaultDurationSec: 180,
			Params: []api.ScenarioParam{numParam("ordersPerSecond", "Orders per second", 100, 5000, 50, "orders/s", 1000),
				numParam("paymentLatencyMs", "Payment latency", 0, 1000, 10, "ms", 150),
				numParam("inventoryLatencyMs", "Inventory latency", 0, 1000, 10, "ms", 100), randomizeParam()},
			ExpectedSymptoms: []string{"Throughput spikes", "Consumer lag grows on payment and inventory", "End-to-end latency rises"},
			Dashboards:       dashboards(cfg, "lab-business", "kafka-lag", "kafka-topics")},
			Plan: func(in PlanInput) (Plan, error) {
				rate := pNum(in.Params, "ordersPerSecond")
				o := overlay(gen, api.Values{"ordersPerSecond": rate})
				if l := pInt(in.Params, "paymentLatencyMs"); l > 0 {
					o[contracts.ServicePayment] = api.Values{"processingLatencyMs": l}
				}
				if l := pInt(in.Params, "inventoryLatencyMs"); l > 0 {
					o[contracts.ServiceInventory] = api.Values{"processingLatencyMs": l}
				}
				return Plan{Affected: sortedKeys(o), Overlay: static(o), Intensity: constant(rate / 5000),
					Checks: []Check{rateCheck(rate), lagGrowingCheck()}}, nil
			}},
		{Drill: "medium", Def: api.ScenarioDef{ID: "flash-sale", Name: "Flash Sale", Category: "traffic", Severity: "medium",
			Description:        "A share of all orders goes to one popular product: stock contention and stockouts.",
			DefaultDurationSec: 120,
			Params: []api.ScenarioParam{selectParam("sku", "Product", []string{"sku-001", "sku-002", "sku-003", "sku-010"}, defaultSku),
				numParam("probability", "Share of orders", 0.05, 1, 0.05, "", 0.7),
				numParam("ordersPerSecond", "Orders per second (0 = unchanged)", 0, 2000, 10, "orders/s", 200), randomizeParam()},
			ExpectedSymptoms: []string{"Reservations rejected (out of stock)", "Cancelled orders and refunds", "Restock job activity"},
			Dashboards:       dashboards(cfg, "lab-business", "lab-services")},
			Plan: func(in PlanInput) (Plan, error) {
				v := api.Values{"popularSku": pStr(in.Params, "sku"), "popularSkuProbability": pNum(in.Params, "probability")}
				if r := pNum(in.Params, "ordersPerSecond"); r > 0 {
					v["ordersPerSecond"] = r
				}
				return Plan{Affected: []string{gen}, Overlay: static(overlay(gen, v)), Intensity: constant(pNum(in.Params, "probability")),
					Checks: []Check{kpiCheck("Stockouts", "reservation success rate below 95 %", func(k api.Kpis) float64 { return k.ReservationSuccessRate * 100 },
						"%.1f %% reserved", func(now, _ float64) bool { return now < 95 })}}, nil
			}},
		{Drill: "hard", Def: api.ScenarioDef{ID: "traffic-tsunami", Name: "Traffic Tsunami", Category: "traffic", Severity: "high",
			Description:        "Traffic ramps up step by step (every 10 s) from 50/s to the peak until it is beyond what the consumers can process.",
			DefaultDurationSec: 240,
			Params:             []api.ScenarioParam{numParam("peakOrdersPerSecond", "Peak orders per second", 100, 5000, 50, "orders/s", 1500), randomizeParam()},
			ExpectedSymptoms:   []string{"Sustained, growing backlog", "Lag never catches up during the ramp", "Rising end-to-end latency"},
			Dashboards:         dashboards(cfg, "kafka-lag", "lab-business", "kafka-e2e")},
			Plan: func(in PlanInput) (Plan, error) {
				peak := pNum(in.Params, "peakOrdersPerSecond")
				rate := func(elapsed time.Duration) float64 {
					steps := float64(int64(elapsed / tsunamiStep))
					total := math.Max(1, float64(int64(in.Duration/tsunamiStep)))
					return math.Round(baseRate + (peak-baseRate)*math.Min(1, steps/total))
				}
				return Plan{Affected: []string{gen}, StepEvery: tsunamiStep,
					Overlay: func(el time.Duration) map[string]api.Values {
						return overlay(gen, api.Values{"ordersPerSecond": rate(el)})
					},
					Intensity: func(el time.Duration) float64 { return rate(el) / 5000 },
					Checks:    []Check{lagGrowingCheck()}}, nil
			}},
		{Drill: "hard", Def: api.ScenarioDef{ID: "hot-partition", Name: "Hot Partition", Category: "traffic", Severity: "medium",
			Description:        "A share of all orders is keyed onto one partition of orders.created: one consumer pod gets all the work.",
			DefaultDurationSec: 150,
			Params: []api.ScenarioParam{numParam("partition", "Partition", 0, 5, 1, "", 0),
				numParam("ratio", "Share of orders", 0.1, 1, 0.05, "", 0.6)},
			ExpectedSymptoms: []string{"Lag on a single partition", "Uneven consumer load", "Higher latency for that partition"},
			Dashboards:       dashboards(cfg, "kafka-lag", "kafka-topics")},
			Plan: func(in PlanInput) (Plan, error) {
				v := api.Values{"hotPartition": pInt(in.Params, "partition"), "hotPartitionRatio": pNum(in.Params, "ratio")}
				return Plan{Affected: []string{gen}, Overlay: static(overlay(gen, v)), Intensity: constant(pNum(in.Params, "ratio"))}, nil
			}},
		{Drill: "hard", Def: api.ScenarioDef{ID: "fat-messages", Name: "Fat Messages", Category: "traffic", Severity: "medium",
			Description:        "Every order carries a large payload filler: bytes in/out, disk and fetch sizes explode.",
			DefaultDurationSec: 120,
			Params:             []api.ScenarioParam{numParam("paddingBytes", "Padding per order", 1000, 900000, 1000, "bytes", 50000)},
			ExpectedSymptoms:   []string{"Broker bytes in/out up", "Fewer records per fetch", "Disk usage grows faster"},
			Dashboards:         dashboards(cfg, "kafka-broker", "kafka-topics")},
			Plan: func(in PlanInput) (Plan, error) {
				p := pInt(in.Params, "paddingBytes")
				return Plan{Affected: []string{gen}, Overlay: static(overlay(gen, api.Values{"paddingBytes": p})), Intensity: constant(float64(p) / 900000)}, nil
			}},
		// ---- services
		{Drill: "easy", Def: api.ScenarioDef{ID: "payment-provider-down", Name: "Payment Provider Down", Category: "service", Severity: "high",
			Description:        "The payment provider rejects every authorisation (PROVIDER_UNAVAILABLE).",
			DefaultDurationSec: 90,
			Params:             []api.ScenarioParam{},
			ExpectedSymptoms:   []string{"Failed payments", "Cancelled orders and stock releases", "Revenue drops"},
			Dashboards:         dashboards(cfg, "lab-business", "lab-services")},
			Plan: func(in PlanInput) (Plan, error) {
				return Plan{Affected: []string{contracts.ServicePayment}, Overlay: static(overlay(contracts.ServicePayment, api.Values{"providerDown": true})),
					Checks: []Check{kpiCheck("Payments failing", "payment success rate below 50 %", func(k api.Kpis) float64 { return k.PaymentSuccessRate * 100 },
						"%.1f %% successful", func(now, _ float64) bool { return now < 50 })}}, nil
			}},
		{Drill: "medium", Def: api.ScenarioDef{ID: "warehouse-overload", Name: "Warehouse Overload", Category: "service", Severity: "medium",
			Description:        "Inventory processing becomes slower by the given factor (normal: 5 ms per record).",
			DefaultDurationSec: 120,
			Params:             []api.ScenarioParam{numParam("factor", "Slowdown factor", 2, 100, 1, "×", 10), randomizeParam()},
			ExpectedSymptoms:   []string{"Inventory consumer lag", "Shipping waits for reservations", "End-to-end latency up"},
			Dashboards:         dashboards(cfg, "kafka-lag", "lab-services")},
			Plan: func(in PlanInput) (Plan, error) {
				f := pNum(in.Params, "factor")
				o := overlay(contracts.ServiceInventory, api.Values{"processingLatencyMs": int(baseLatency * f)})
				return Plan{Affected: []string{contracts.ServiceInventory}, Overlay: static(o), Intensity: constant(f / 100), Checks: []Check{lagGrowingCheck()}}, nil
			}},
		{Drill: "easy", Def: api.ScenarioDef{ID: "notification-meltdown", Name: "Notification Meltdown", Category: "service", Severity: "medium",
			Description:        "Notification processes only the given number of events per second and pod.",
			DefaultDurationSec: 120,
			Params:             []api.ScenarioParam{numParam("eventsPerSecond", "Events per second", 0.5, 50, 0.5, "events/s", 1), randomizeParam()},
			ExpectedSymptoms:   []string{"Rapidly increasing notification lag", "Other services unaffected"},
			Dashboards:         dashboards(cfg, "kafka-lag", "lab-services")},
			Plan: func(in PlanInput) (Plan, error) {
				eps := pNum(in.Params, "eventsPerSecond")
				o := overlay(contracts.ServiceNotification, api.Values{"processingLatencyMs": int(math.Round(1000 / eps))})
				return Plan{Affected: []string{contracts.ServiceNotification}, Overlay: static(o), Intensity: constant(1 / eps), Checks: []Check{lagGrowingCheck()}}, nil
			}},
		{Drill: "medium", Def: api.ScenarioDef{ID: "silent-failure", Name: "Silent Failure", Category: "service", Severity: "high",
			Description:        "One service stops making progress without crashing: healthy pods, growing lag.",
			DefaultDurationSec: 120,
			Params:             []api.ScenarioParam{serviceParam("service", "Service", consumers, contracts.ServiceShipping)},
			ExpectedSymptoms:   []string{"Growing lag for one consumer group", "Pods stay ready", "No errors in the logs"},
			Dashboards:         dashboards(cfg, "kafka-lag", "lab-services")},
			Plan: func(in PlanInput) (Plan, error) {
				svc := pStr(in.Params, "service")
				return Plan{Affected: []string{svc}, Overlay: static(overlay(svc, api.Values{"paused": true})),
					Checks: []Check{lagGrowingCheck(), replicasCheck(svc, func(n int) bool { return n > 0 }, "pods still ready")}}, nil
			}},
		{Drill: "medium", Def: api.ScenarioDef{ID: "database-slowdown", Name: "Database Slowdown", Category: "service", Severity: "medium",
			Description:        "Every database transaction of the chosen service(s) takes longer (locks are held meanwhile).",
			DefaultDurationSec: 120,
			Params: []api.ScenarioParam{numParam("dbLatencyMs", "Extra latency per transaction", 10, 5000, 10, "ms", 80),
				selectParam("service", "Service", append([]string{"all"}, consumers...), "all"), randomizeParam()},
			ExpectedSymptoms: []string{"Higher transaction time", "End-to-end latency up", "Lag for heavy consumers"},
			Dashboards:       dashboards(cfg, "lab-postgres", "lab-services", "kafka-e2e")},
			Plan: func(in PlanInput) (Plan, error) {
				targets := consumers
				if s := pStr(in.Params, "service"); s != "all" {
					targets = []string{s}
				}
				o := map[string]api.Values{}
				for _, s := range targets {
					o[s] = api.Values{"dbLatencyMs": pInt(in.Params, "dbLatencyMs")}
				}
				return Plan{Affected: sortedKeys(o), Overlay: static(o), Intensity: constant(pNum(in.Params, "dbLatencyMs") / 5000)}, nil
			}},
		{Drill: "medium", Def: api.ScenarioDef{ID: "recovery-storm", Name: "Recovery Storm", Category: "service", Severity: "high",
			Description:        "A share of the consumers is paused for the duration, then all resume at the same moment.",
			DefaultDurationSec: 120,
			Params:             []api.ScenarioParam{numParam("pauseShare", "Share of consumers paused", 0.2, 1, 0.2, "", 0.6)},
			ExpectedSymptoms:   []string{"Lag builds up on the paused groups", "Catch-up traffic spike at the end", "CPU and broker fetch spikes"},
			Dashboards:         dashboards(cfg, "kafka-lag", "kafka-broker")},
			Plan: func(in PlanInput) (Plan, error) {
				n := int(math.Ceil(pNum(in.Params, "pauseShare")*float64(len(consumers)) - 1e-9))
				o := map[string]api.Values{}
				for _, s := range consumers[:n] {
					o[s] = api.Values{"paused": true}
				}
				return Plan{Affected: sortedKeys(o), Overlay: static(o), Intensity: constant(pNum(in.Params, "pauseShare")), Checks: []Check{lagGrowingCheck()}}, nil
			}},
		// ---- kubernetes
		{Drill: "hard", Kube: true, Def: api.ScenarioDef{ID: "consumer-crash-loop", Name: "Consumer Crash Loop", Category: "kubernetes", Severity: "high",
			Description:        "Every pod of the service is killed repeatedly; Kubernetes restarts them each time.",
			DefaultDurationSec: 120,
			Params: []api.ScenarioParam{serviceParam("service", "Service", consumers, contracts.ServiceInventory),
				numParam("intervalSec", "Kill every", 5, 120, 1, "s", 12)},
			ExpectedSymptoms: []string{"Rebalances", "Lag fluctuates", "Pod restarts"},
			Dashboards:       dashboards(cfg, "kafka-lag", "lab-services")},
			Plan: func(in PlanInput) (Plan, error) {
				svc := pStr(in.Params, "service")
				a := &killPodsAction{Namespace: in.Cfg.CommerceNamespace, Selector: kubeSelector + svc, Every: time.Duration(pInt(in.Params, "intervalSec")) * time.Second}
				return Plan{Affected: []string{svc}, Actions: []Action{a}, Intensity: constant(5 / pNum(in.Params, "intervalSec")),
					Checks: []Check{{Label: svc + " pods killed", Requested: "every " + strconv.Itoa(pInt(in.Params, "intervalSec")) + " s",
						Eval: func(*CheckContext) (string, bool) { return fmt.Sprintf("%d pods killed", a.Kills), a.Kills > 0 }}}}, nil
			}},
		{Kube: true, Def: api.ScenarioDef{ID: "regional-outage", Name: "Regional Outage", Category: "kubernetes", Severity: "critical", Destructive: true,
			Description:        "A whole group of services is scaled to zero and restored to its replicas afterwards.",
			DefaultDurationSec: 90,
			Params:             []api.ScenarioParam{selectParam("region", "Region", sortedKeys(regions), "region-a (payment, inventory)")},
			ExpectedSymptoms:   []string{"Partial outage: some groups stop consuming", "Orders stall waiting for the missing services", "Catch-up after recovery"},
			Dashboards:         dashboards(cfg, "lab-services", "kafka-lag", "lab-business")},
			Plan: func(in PlanInput) (Plan, error) {
				svcs := regions[pStr(in.Params, "region")]
				p := Plan{Affected: svcs, Intensity: constant(float64(len(svcs)) / float64(len(consumers)))}
				for _, s := range svcs {
					p.Actions = append(p.Actions, &scaleAction{Namespace: in.Cfg.CommerceNamespace, Deployment: s})
					p.Checks = append(p.Checks, replicasCheck(s, func(n int) bool { return n == 0 }, "0 ready"))
				}
				return p, nil
			}},
		{Drill: "hard", Kube: true, Def: api.ScenarioDef{ID: "rebalance-storm", Name: "Rebalance Storm", Category: "kubernetes", Severity: "medium",
			Description:        "The service is scaled up and down repeatedly: every change triggers a consumer group rebalance.",
			DefaultDurationSec: 120,
			Params: []api.ScenarioParam{serviceParam("service", "Service", consumers, contracts.ServicePayment),
				numParam("everySec", "Change every", 5, 120, 1, "s", 8), numParam("extraReplicas", "Extra replicas", 1, 3, 1, "", 1)},
			ExpectedSymptoms: []string{"Frequent rebalances", "Processing pauses", "Lag sawtooth"},
			Dashboards:       dashboards(cfg, "kafka-lag", "lab-services")},
			Plan: func(in PlanInput) (Plan, error) {
				svc := pStr(in.Params, "service")
				a := &flipScaleAction{scaleAction: scaleAction{Namespace: in.Cfg.CommerceNamespace, Deployment: svc},
					Extra: pInt(in.Params, "extraReplicas"), Every: time.Duration(pInt(in.Params, "everySec")) * time.Second}
				return Plan{Affected: []string{svc}, Actions: []Action{a}, Intensity: constant(5 / pNum(in.Params, "everySec"))}, nil
			}},
		// ---- brokers (ADR 0003)
		{Kube: true, Def: api.ScenarioDef{ID: "kafka-broker-failure", Name: "Kafka Broker Failure", Category: "broker", Severity: "critical", Destructive: true,
			Description:        "One broker is stopped and kept down for the duration (Strimzi operator paused), then restored.",
			DefaultDurationSec: 90,
			Params:             []api.ScenarioParam{brokerParam(1)},
			ExpectedSymptoms:   []string{"Leader elections", "Under-replicated partitions", "Short produce/consume errors, then normal operation on 2 brokers"},
			Dashboards:         dashboards(cfg, "kafka-broker", "kafka-kraft", "kafka-overview")},
			Plan: func(in PlanInput) (Plan, error) {
				b := pInt(in.Params, "broker")
				zero := int64(0)
				return brokerPlan(b, []Action{
					&scaleAction{Namespace: in.Cfg.KafkaNamespace, Deployment: in.Cfg.OperatorDeployment},
					&brokerDownAction{Pod: in.Cfg.BrokerPodPrefix + strconv.Itoa(b), Grace: &zero},
				}, brokerDownCheck(b)), nil
			}},
		{Kube: true, Def: api.ScenarioDef{ID: "broker-crash", Name: "Broker Crash", Category: "broker", Severity: "high", Destructive: true,
			Description:        "One broker is killed without grace period (like kill -9); Strimzi starts it again right away.",
			DefaultDurationSec: 60,
			Params:             []api.ScenarioParam{brokerParam(1)},
			ExpectedSymptoms:   []string{"Unclean shutdown and log recovery", "Leader elections", "Under-replicated partitions until it caught up"},
			Dashboards:         dashboards(cfg, "kafka-broker", "kafka-kraft")},
			Plan: func(in PlanInput) (Plan, error) {
				b := pInt(in.Params, "broker")
				a := &killPodsAction{Namespace: in.Cfg.KafkaNamespace, Selector: "strimzi.io/pod-name=" + in.Cfg.BrokerPodPrefix + strconv.Itoa(b)}
				return brokerPlan(b, []Action{a}, underReplicatedCheck(fmt.Sprintf("Broker %d recovering", b))), nil
			}},
		{Kube: true, Def: api.ScenarioDef{ID: "broker-rolling-restart", Name: "Broker Rolling Restart", Category: "broker", Severity: "medium", Destructive: true,
			Description:        "All brokers are restarted gracefully one after another, like an upgrade.",
			DefaultDurationSec: 90,
			Params:             []api.ScenarioParam{numParam("pauseSec", "Pause between brokers", 0, 120, 5, "s", 20)},
			ExpectedSymptoms:   []string{"Leadership moves around", "Short client reconnects", "No data loss"},
			Dashboards:         dashboards(cfg, "kafka-broker", "kafka-overview")},
			Plan: func(in PlanInput) (Plan, error) {
				a := &rollingRestartAction{Brokers: []int{0, 1, 2}, Pause: time.Duration(pInt(in.Params, "pauseSec")) * time.Second}
				p := brokerPlan(-1, []Action{a}, Check{Label: "Brokers restarted", Requested: "3 of 3",
					Eval: func(*CheckContext) (string, bool) { return fmt.Sprintf("%d of 3", min(a.next, 3)), a.Done() }})
				p.Affected = []string{"broker-0", "broker-1", "broker-2"}
				return p, nil
			}},
		{Kube: true, Def: api.ScenarioDef{ID: "broker-outage", Name: "Broker Outage (partition)", Category: "broker", Severity: "critical", Destructive: true,
			Description:        "One broker is cut off from clients and the other brokers (iptables in its network namespace); the process keeps running.",
			DefaultDurationSec: 60,
			Params:             []api.ScenarioParam{brokerParam(1)},
			ExpectedSymptoms:   []string{"Broker leaves every ISR", "Under-replicated partitions while its pod looks healthy", "Leader elections"},
			Dashboards:         dashboards(cfg, "kafka-broker", "kafka-overview", "kafka-kraft")},
			Plan: func(in PlanInput) (Plan, error) {
				return netPlan(in, []k8s.EnvVar{{Name: "MODE", Value: "partition"}}, "isolated")
			}},
		{Kube: true, Def: api.ScenarioDef{ID: "broker-network", Name: "Broker Network Degradation", Category: "broker", Severity: "high", Destructive: true,
			Description:        "Latency (tc netem) or packet loss (iptables) on one broker's Kafka ports.",
			DefaultDurationSec: 90,
			Params: []api.ScenarioParam{brokerParam(1), selectParam("mode", "Fault", []string{"latency", "loss"}, "latency"),
				numParam("latencyMs", "Latency", 10, 2000, 10, "ms", 200), numParam("lossPercent", "Packet loss", 1, 100, 1, "%", 30)},
			ExpectedSymptoms: []string{"Slow produce requests (acks=all waits for the slow replica)", "ISR shrinks under heavy loss", "Higher end-to-end latency"},
			Dashboards:       dashboards(cfg, "kafka-broker", "kafka-e2e")},
			Plan: func(in PlanInput) (Plan, error) {
				env := []k8s.EnvVar{{Name: "MODE", Value: pStr(in.Params, "mode")},
					{Name: "LATENCY_MS", Value: strconv.Itoa(pInt(in.Params, "latencyMs"))},
					{Name: "LOSS_PERCENT", Value: strconv.Itoa(pInt(in.Params, "lossPercent"))}}
				return netPlan(in, env, "degraded")
			}},
		// ---- network (client side)
		{Def: api.ScenarioDef{ID: "network-chaos", Name: "Network Chaos", Category: "network", Severity: "high", Destructive: true,
			Description:        "Latency and dropped connections on the service's Kafka client (fault dialer).",
			DefaultDurationSec: 120,
			Params: []api.ScenarioParam{serviceParam("service", "Service", consumers, contracts.ServicePayment),
				numParam("latencyMs", "Latency", 0, 2000, 10, "ms", 200), numParam("dropProbability", "Drop probability", 0, 0.5, 0.01, "", 0.1), randomizeParam()},
			ExpectedSymptoms: []string{"Retries and timeouts", "Reconnects", "Backlog growth"},
			Dashboards:       dashboards(cfg, "lab-services", "kafka-lag")},
			Plan: func(in PlanInput) (Plan, error) {
				svc := pStr(in.Params, "service")
				o := overlay(svc, api.Values{"netLatencyMs": pInt(in.Params, "latencyMs"), "netDropProbability": pNum(in.Params, "dropProbability")})
				return Plan{Affected: []string{svc}, Overlay: static(o), Intensity: constant(math.Max(pNum(in.Params, "latencyMs")/2000, pNum(in.Params, "dropProbability")*2))}, nil
			}},
		// ---- data
		{Drill: "easy", Def: api.ScenarioDef{ID: "poison-message", Name: "Poison Message", Category: "data", Severity: "medium",
			Description:        "The generator publishes a share of undecodable records to orders.created.",
			DefaultDurationSec: 90,
			Params:             []api.ScenarioParam{numParam("probability", "Share of poison records", 0.001, 0.5, 0.001, "", 0.05)},
			ExpectedSymptoms:   []string{"DLQ activity", "Decode errors in the logs", "Consumers keep going"},
			Dashboards:         dashboards(cfg, "lab-services", "kafka-topics")},
			Plan: func(in PlanInput) (Plan, error) {
				return Plan{Affected: []string{gen}, Overlay: static(overlay(gen, api.Values{"poisonProbability": pNum(in.Params, "probability")})),
					Intensity: constant(pNum(in.Params, "probability") * 2),
					Checks: []Check{kpiCheck("Dead letters", "DLQ receives records", func(k api.Kpis) float64 { return k.DlqPerMin },
						"%.0f records/min", func(now, _ float64) bool { return now > 0 })}}, nil
			}},
		{Drill: "medium", Def: api.ScenarioDef{ID: "duplicate-delivery", Name: "Duplicate Delivery", Category: "data", Severity: "low",
			Description:        "The generator publishes a share of orders twice (same event id): idempotency at work.",
			DefaultDurationSec: 90,
			Params:             []api.ScenarioParam{numParam("probability", "Share of duplicates", 0.01, 1, 0.01, "", 0.2)},
			ExpectedSymptoms:   []string{"Duplicates detected and skipped", "Business KPIs unchanged"},
			Dashboards:         dashboards(cfg, "lab-services", "lab-business")},
			Plan: func(in PlanInput) (Plan, error) {
				return Plan{Affected: []string{gen}, Overlay: static(overlay(gen, api.Values{"duplicateProbability": pNum(in.Params, "probability")})),
					Intensity: constant(pNum(in.Params, "probability"))}, nil
			}},
	}
}

func brokerPlan(broker int, actions []Action, check Check) Plan {
	p := Plan{Broker: true, Actions: actions, Checks: []Check{check}, RecoveryChecks: []Check{brokersHealthyCheck()}, Intensity: constant(1)}
	if broker >= 0 {
		p.Affected = []string{"broker-" + strconv.Itoa(broker)}
	}
	return p
}

func netPlan(in PlanInput, env []k8s.EnvVar, what string) (Plan, error) {
	if in.Cfg.ChaosNetImage == "" {
		return Plan{}, fmt.Errorf("%w: the chaos-net image is not configured (CHAOS_NET_IMAGE)", api.ErrUnavailable)
	}
	b := pInt(in.Params, "broker")
	pod := in.Cfg.BrokerPodPrefix + strconv.Itoa(b)
	a := &chaosNetAction{Pod: pod, Env: env, Duration: in.Duration}
	return brokerPlan(b, []Action{a}, underReplicatedCheck(fmt.Sprintf("Broker %d %s", b, what))), nil
}

// rateCheck: the observed order rate reached 80 % of the target.
func rateCheck(target float64) Check {
	return kpiCheck("Order rate", fmt.Sprintf("≥ %.0f orders/s (80 %% of %.0f)", 0.8*target, target),
		func(k api.Kpis) float64 { return k.OrdersPerSec }, "%.0f orders/s", func(now, _ float64) bool { return now >= 0.8*target })
}
