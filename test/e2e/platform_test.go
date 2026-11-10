//go:build e2e

package e2e

import (
	"testing"
	"time"
)

func Test_shouldReachEveryWebInterfaceWhenTheStackIsUp(t *testing.T) {
	// GIVEN: a running stack
	// WHEN: every web UI of the launchpad is opened through the ingress
	for host, path := range map[string]string{
		"grafana": "/api/health", "vm": "/health", "vmagent": "/health", "vmalert": "/health",
		"alertmanager": "/-/healthy", "akhq": "/", "logs": "/health", "board": "/healthz",
	} {
		_, code := do(t, "GET", host, path, nil)

		// THEN: it answers successfully
		if code >= 400 {
			t.Errorf("%s%s answered %d", host, path, code)
		}
	}
}

func Test_shouldScrapeEveryTargetWhenTheStackIsHealthy(t *testing.T) {
	// GIVEN: the monitoring pipeline
	// WHEN: vmagent's scrape targets are inspected
	down := query(t, `up == 0`)

	// THEN: no target is down (Definition of Done 3: metrics appear in Grafana)
	for _, s := range down {
		t.Errorf("target down: %v", s.Metric)
	}
}

func Test_shouldRunThreeBrokersWithOneControllerWhenKafkaIsHealthy(t *testing.T) {
	// GIVEN: the default profile
	// WHEN: broker metrics are queried
	brokers := scalar(t, `count(kafka_server_replicamanager_leadercount)`)
	controllers := scalar(t, `sum(kafka_controller_kafkacontroller_activecontrollercount)`)
	offline := scalar(t, `sum(kafka_controller_kafkacontroller_offlinepartitionscount)`)

	// THEN: three brokers report, exactly one is the active controller and no partition is offline
	if brokers != 3 || controllers != 1 || offline != 0 {
		t.Fatalf("brokers=%v controllers=%v offline=%v", brokers, controllers, offline)
	}
}

func Test_shouldMoveOrdersThroughTheWholeWorkflowWhenTheSimulationRuns(t *testing.T) {
	// GIVEN: the running simulation
	// WHEN: business metrics are observed for a while
	// THEN: orders are created, paid, reserved, shipped and notified (Definition of Done 2)
	for _, q := range []string{
		`sum(rate(lab_orders_created_total[1m]))`,
		`sum(rate(lab_payments_total{result="completed"}[1m]))`,
		`sum(rate(lab_reservations_total{result="reserved"}[1m]))`,
		`sum(rate(lab_orders_shipped_total[1m]))`,
		`sum(rate(lab_notifications_total[1m]))`,
	} {
		q := q
		eventually(t, 3*time.Minute, q+" > 0", func() bool { return scalar(t, q) > 0 })
	}
}

func Test_shouldBringEveryOrderToATerminalStateWhenNothingIsBroken(t *testing.T) {
	// GIVEN: a healthy system at baseline
	// WHEN: orders in flight are compared with the order rate
	inflight := scalar(t, `sum(lab_orders_created_total) - sum(lab_orders_shipped_total) - sum(lab_orders_cancelled_total)`)
	rate := scalar(t, `sum(rate(lab_orders_created_total[5m]))`)

	// THEN: the backlog is at most ~30 s of traffic, so no order is stuck forever
	if rate > 0 && inflight > rate*30 {
		t.Fatalf("%v orders in flight at %v orders/s", inflight, rate)
	}
}

func Test_shouldFindServiceLogsWhenSearchingVictoriaLogs(t *testing.T) {
	// GIVEN: services writing JSON logs
	// WHEN: VictoriaLogs is searched for payment logs
	b := get(t, "logs", "/select/logsql/query?query=service:payment&limit=1")

	// THEN: at least one structured line is found
	if !contains(b, `"service":"payment"`) {
		t.Fatalf("no payment logs: %s", b)
	}
}

func Test_shouldStayWithinTheMemoryBudgetWhenTheWholeStackRuns(t *testing.T) {
	// GIVEN: every component running
	// WHEN: the working-set memory of all containers is summed
	used := scalar(t, `sum(container_memory_working_set_bytes{container!="",container!="POD"})`)

	// THEN: it stays under 10 GB
	if used > 10e9 {
		t.Fatalf("stack uses %.1f GB", used/1e9)
	}
	t.Logf("stack working set: %.2f GB", used/1e9)
}
