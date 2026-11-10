package dash

import g "kafkalab/monitoring/dashgen/internal/grafana"

// Reusable PromQL fragments of the lag dashboard and the alert rules.
const (
	lagByGroupTopic     = `sum by (group_id, topic_name) (kminion_kafka_consumer_group_topic_lag{@KG})`
	consumeRateByGT     = `sum by (group_id, topic_name) (rate(kminion_kafka_consumer_group_topic_offset_sum{@KG}[5m]))`
	groupLink           = "var-group=${__field.labels.group_id}"
	groupStateMappedSum = `max by (group_id) (
  (kminion_kafka_consumer_group_info{@C,group_id=~"$group",state="Stable"} * 4)
  or (kminion_kafka_consumer_group_info{@C,group_id=~"$group",state=~"PreparingRebalance|CompletingRebalance|Assigning|Reconciling"} * 3)
  or (kminion_kafka_consumer_group_info{@C,group_id=~"$group",state="Empty"} * 2)
  or (kminion_kafka_consumer_group_info{@C,group_id=~"$group",state=~"Dead|Unknown"} * 1)
)`
)

// Lag is the consumer groups & lag dashboard (uid kafka-lag).
func Lag() *g.Dashboard {
	d := bundle("kafka-lag", "Kafka · Consumer Groups & Lag",
		"Consumer lag in messages and in time, per group, topic and partition: is a consumer falling behind, stalled, or just slow on one partition? Data from KMinion (admin API: committed offsets vs. high watermarks).",
		clusterVar("kminion_kafka_cluster_info"), groupVar(), topicVar())
	d.Rows = []*g.Row{
		row("", false,
			g.Stat("Consumer groups", "Groups matching the filter (KMinion ignores its own probe groups).",
				"short", q(`count(kminion_kafka_consumer_group_info{@C,group_id=~"$group"})`, "")).Hue(g.Blue).Neutral(),
			g.Stat("Groups not stable", "Groups currently rebalancing, empty or dead. Rebalancing groups do not consume; a group stuck in rebalance is an outage for its application.",
				"short", q(`count(kminion_kafka_consumer_group_info{@C,group_id=~"$group",state!="Stable"}) or vector(0)`, "")).
				Thresholds(g.Green, g.Step{Color: g.Yellow, Value: 1}),
			g.Stat("Total lag", "Messages not yet consumed, summed over the selected groups and topics.",
				"short", q(`sum(kminion_kafka_consumer_group_topic_lag{@KG})`, "")).
				Thresholds(g.Green, g.Step{Color: g.Yellow, Value: 1000}, g.Step{Color: g.Red, Value: 10000}),
			g.Stat("Worst time behind", "Largest estimated time-behind of any selected group/topic: lag divided by the group's current consume rate. 'How long until this consumer catches up if producers stopped now'.",
				"s", q(`max(`+lagByGroupTopic+` / (`+consumeRateByGT+` > 0))`, "")).
				Thresholds(g.Green, g.Step{Color: g.Yellow, Value: 60}, g.Step{Color: g.Red, Value: 600}),
			okIfZero(g.Stat("Stalled consumers", "Group/topic pairs that have lag but committed nothing during the last 5 minutes. The application is stuck, paused or crashed while still holding its assignment (or has no members at all).",
				"short", q(`count((`+lagByGroupTopic+` > 0) unless on (group_id, topic_name) (`+consumeRateByGT+` > 0)) or vector(0)`, ""))),
			g.Stat("Lag growth", "How fast the total lag of the selected groups grows (positive) or shrinks (negative), messages per second over the last 10 minutes.",
				"suffix: msg/s", q(`sum(deriv(kminion_kafka_consumer_group_topic_lag{@KG}[10m]))`, "")).
				Thresholds(g.Green, g.Step{Color: g.Yellow, Value: 1}, g.Step{Color: g.Red, Value: 50}),
		),
		row("Lag", false,
			g.TimeSeries("Lag per group and topic",
				"Messages behind the end of the log per consumer group and topic. A flat line above zero is a constant backlog (consumer as fast as the producer); a rising line means the consumer is slower than the producer. Click a series to focus that group.",
				"short",
				q(lagByGroupTopic, "{{group_id}} · {{topic_name}}")).
				Link("Focus group ${__field.labels.group_id}", drill("kafka-lag", groupLink)).
				LegendTable("right", "lastNotNull", "max").Hue(g.Orange).Size(12, 9),
			g.TimeSeries("Estimated time behind",
				"Lag ÷ consume rate (5 m) per group and topic: seconds a consumer needs to drain its backlog at its current speed. Better than raw lag for SLOs because it is independent of throughput. Thresholds: 1 min (yellow), 10 min (red). Groups that consume nothing at all have no estimate — they appear under Stalled consumers.",
				"s",
				q(lagByGroupTopic+` / (`+consumeRateByGT+` > 0)`, "{{group_id}} · {{topic_name}}")).
				Thresholds(g.Green, g.Step{Color: g.Yellow, Value: 60}, g.Step{Color: g.Red, Value: 600}).Lines("dashed").
				Hue(g.Purple).Size(12, 9),
			g.TimeSeries("Lag growth rate",
				"Derivative of the lag (messages per second, 10-minute window). Above zero the consumer falls behind; below zero it catches up. The sign matters more than the absolute lag.",
				"suffix: msg/s",
				q(`sum by (group_id, topic_name) (deriv(kminion_kafka_consumer_group_topic_lag{@KG}[10m]))`, "{{group_id}} · {{topic_name}}")).
				Thresholds(g.Green, g.Step{Color: g.Red, Value: 1}).Lines("dashed").Hue(g.Blue).Size(12, 8),
			g.TimeSeries("Consume rate vs. produce rate",
				"Messages per second committed by each group (solid) and produced into the topic (dashed). When the consume line stays below the produce line, lag grows.",
				"suffix: msg/s",
				q(consumeRateByGT, "consume · {{group_id}} · {{topic_name}}"),
				q(`sum by (topic_name) (rate(kminion_kafka_topic_high_water_mark_sum{@KT}[5m]))`, "produce · {{topic_name}}")).
				Over("byRegexp", "produce.*", map[string]any{"custom.lineStyle": map[string]any{"fill": "dash", "dash": []int{10, 10}}, "color": map[string]any{"mode": "fixed", "fixedColor": g.Purple}}).
				Hue(g.Green).Size(12, 8),
		),
		row("Partitions", false,
			g.Heatmap("Partition skew: lag per partition",
				"Each row is one partition (group · topic · partition), colour = lag. A single hot row means one partition is slow (hot key, poison message, slow consumer instance); all rows hot means the whole group is under-provisioned.",
				"short",
				q(`sum by (group_id, topic_name, partition_id) (kminion_kafka_consumer_group_topic_partition_lag{@KG})`, "{{group_id}} · {{topic_name}} · p{{partition_id}}")).
				Size(14, 10),
			g.BarGauge("Top 15 partitions by lag",
				"The partitions with the largest lag right now. If one partition dominates, check the consumer instance that owns it and the message keys.",
				"short",
				iq(`topk(15, sum by (group_id, topic_name, partition_id) (kminion_kafka_consumer_group_topic_partition_lag{@KG}))`, "{{group_id}} · {{topic_name}} · p{{partition_id}}")).
				Thresholds(g.Green, g.Step{Color: g.Yellow, Value: 1000}, g.Step{Color: g.Red, Value: 10000}).Size(10, 10),
		),
		row("Stalled & group state", false,
			g.Table("Stalled consumers",
				"Group/topic pairs with lag > 0 and no committed progress during the last 5 minutes. Typical causes: the application hangs in a handler, is paused, crashed after rebalancing, or the group has no members. Empty table = nothing stalled.",
				"short",
				iq(`(`+lagByGroupTopic+` > 0) unless on (group_id, topic_name) (`+consumeRateByGT+` > 0)`, "")).
				Organize(map[string]string{"group_id": "Group", "topic_name": "Topic", "Value": "Lag"}).
				Thresholds(g.Red).
				Over("byName", "Lag", map[string]any{"custom.cellOptions": map[string]any{"type": "color-background", "mode": "basic"}}).
				Over("byName", "Group", map[string]any{"links": []any{map[string]any{"title": "Focus group", "url": drill("kafka-lag", "var-group=${__data.fields.Group}")}}}).
				Size(10, 8),
			g.StateTimeline("Group state",
				"State of each consumer group over time. Stable = consuming; rebalancing (classic Preparing/CompletingRebalance, KIP-848 Assigning/Reconciling) = paused; Empty = no members but committed offsets remain; Dead = being removed.",
				"none",
				q(groupStateMappedSum, "{{group_id}}")).
				Thresholds(g.Blue).
				Map("4", "stable", g.Green).Map("3", "rebalancing", g.Yellow).Map("2", "empty", g.Orange).Map("1", "dead", g.Red).Map("0", "unknown", g.Text).
				Size(14, 8),
			g.TimeSeries("Members per group",
				"Consumer instances per group. Members > partitions leaves members idle; a drop to 0 means the application is gone (lag will grow).",
				"short",
				q(`sum by (group_id) (kminion_kafka_consumer_group_members{@C,group_id=~"$group"})`, "{{group_id}}")).
				Hue(g.Blue).Size(12, 7),
			g.TimeSeries("Assigned partitions per group and topic",
				"Partitions of each topic assigned to the group's members. Should equal the topic's partition count while the group is stable.",
				"short",
				q(`sum by (group_id, topic_name) (kminion_kafka_consumer_group_topic_assigned_partitions{@KG})`, "{{group_id}} · {{topic_name}}")).
				Hue(g.Purple).Size(12, 7),
		),
	}
	return d
}
