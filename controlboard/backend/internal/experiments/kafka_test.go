package experiments

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"kafkalab/pkg/contracts"
	"kafkalab/pkg/events"
	"kafkalab/pkg/kit/serde"
	"kafkalab/pkg/kit/testkit"
)

func kafkaSetup(t *testing.T) (*testkit.Cluster, *kgo.Client, *serde.Serde) {
	t.Helper()
	c := testkit.NewCluster(t)
	reg := testkit.NewRegistry(t, true)
	sd, err := serde.New(ctx, serde.Options{Registry: reg.Client(t), Topics: []string{contracts.TopicPlatformControl, contracts.TopicPlatformAudit}})
	if err != nil {
		t.Fatal(err)
	}
	cl, err := kgo.NewClient(kgo.SeedBrokers(c.Brokers...), kgo.DefaultProduceTopic(contracts.TopicPlatformAudit), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)
	return c, cl, sd
}

func Test_shouldContinueAfterTheHighestGenerationWhenSnapshotsAreAlreadyOnTheTopic(t *testing.T) {
	// GIVEN: platform.control with payment at generation 7 and inventory at 12
	c, cl, sd := kafkaSetup(t)
	pub := NewKafkaPublisher(cl, sd)
	now := time.Now().UTC().Truncate(time.Millisecond)
	for svc, g := range map[string]int64{"payment": 7, "inventory": 12} {
		if err := pub.PublishControl(ctx, &events.ControlSnapshot{Service: svc, Generation: g, IssuedAt: now, Experiments: []string{}}); err != nil {
			t.Fatal(err)
		}
	}

	// WHEN: the generations are read back
	top, err := KafkaGenerations(c.Brokers, sd)(ctx)

	// THEN: the engine continues after 12
	if err != nil || top != 12 {
		t.Fatalf("top %d err %v", top, err)
	}
}

func Test_shouldFailToReadGenerationsWhenKafkaIsUnreachable(t *testing.T) {
	// GIVEN: no broker
	_, _, sd := kafkaSetup(t)
	short, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancel()

	// WHEN: the generations are read
	_, err := KafkaGenerations([]string{"127.0.0.1:1"}, sd)(short)

	// THEN: it fails
	if err == nil {
		t.Fatal("expected an error")
	}
}

func Test_shouldWriteAnAvroAuditEventKeyedByItsIdWhenAnActionIsAudited(t *testing.T) {
	// GIVEN: the audit topic
	c, cl, sd := kafkaSetup(t)
	pub := NewKafkaPublisher(cl, sd)
	scenario := "black-friday"

	// WHEN: an audit event is published
	err := pub.PublishAudit(ctx, &events.AuditEvent{EventID: "audit-1", At: time.Now().UTC().Truncate(time.Millisecond), Actor: "user",
		Action: "experiment.started", Scenario: &scenario, Details: map[string]string{}})

	// THEN: it can be read back and decoded
	rd, _ := kgo.NewClient(kgo.SeedBrokers(c.Brokers...), kgo.ConsumeTopics(contracts.TopicPlatformAudit), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	defer rd.Close()
	fs := rd.PollFetches(ctx)
	rec := fs.Records()[0]
	v, derr := sd.Decode(rec.Value)
	if err != nil || derr != nil || string(rec.Key) != "audit-1" || v.(*events.AuditEvent).Action != "experiment.started" {
		t.Fatalf("%v %v %+v", err, derr, v)
	}
	// AND: values the encoder cannot handle are refused
	if err := pub.PublishAudit(ctx, nil); err == nil {
		t.Fatal("expected an encode error")
	}
}

func Test_shouldEmptyTopicsAndMoveGroupsToTheEndWhenTheAdminResetsKafka(t *testing.T) {
	// GIVEN: 5 orders on orders.created, payment committed at offset 2, inventory without offsets,
	// shipping whose offsets were deleted
	_, cl, _ := kafkaSetup(t)
	for range 5 {
		cl.ProduceSync(ctx, &kgo.Record{Topic: contracts.TopicOrdersCreated, Partition: 0, Value: []byte("o")})
	}
	adm := kadm.NewClient(cl)
	var os kadm.Offsets
	os.Add(kadm.Offset{Topic: contracts.TopicOrdersCreated, Partition: 0, At: 2})
	if _, err := adm.CommitOffsets(ctx, "payment", os); err != nil {
		t.Fatal(err)
	}
	_, _ = adm.CommitOffsets(ctx, "shipping", os)
	_, _ = adm.DeleteOffsets(ctx, "shipping", kadm.TopicsSet{contracts.TopicOrdersCreated: {0: {}}})
	a := NewKafkaAdmin(cl)

	// WHEN: records are deleted to the end and the groups are committed to the end
	err1 := a.DeleteRecordsToEnd(ctx, []string{contracts.TopicOrdersCreated})
	err2 := a.CommitGroupsToEnd(ctx, []string{"payment", "inventory", "shipping"})

	// THEN: the log starts at the end and payment's offset is at the end too
	starts, _ := adm.ListStartOffsets(ctx, contracts.TopicOrdersCreated)
	got, _ := adm.FetchOffsets(ctx, "payment")
	st, _ := starts.Lookup(contracts.TopicOrdersCreated, 0)
	off, _ := got.Lookup(contracts.TopicOrdersCreated, 0)
	if err1 != nil || err2 != nil || st.Offset != 5 || off.At != 5 {
		t.Fatalf("start %d committed %d: %v %v", st.Offset, off.At, err1, err2)
	}
}

func Test_shouldFailTheKafkaResetWhenTopicsOrTheClusterAreMissing(t *testing.T) {
	// GIVEN: an admin on a live cluster and one on a closed client
	_, cl, _ := kafkaSetup(t)
	a := NewKafkaAdmin(cl)
	dead, _ := kgo.NewClient(kgo.SeedBrokers("127.0.0.1:1"))
	dead.Close()
	d := NewKafkaAdmin(dead)

	// WHEN: unknown topics are emptied and groups are committed without a cluster
	err1 := a.DeleteRecordsToEnd(ctx, []string{"no.such.topic"})
	err2 := d.CommitGroupsToEnd(ctx, []string{"payment"})

	// THEN: both fail
	if err1 == nil || err2 == nil {
		t.Fatalf("%v %v", err1, err2)
	}
}

func Test_shouldReadInstantValuesWhenVictoriaMetricsAnswers(t *testing.T) {
	// GIVEN: VictoriaMetrics returning two PVC ratios (one unparsable)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/query" || !strings.Contains(r.URL.Query().Get("query"), "kubelet") {
			w.WriteHeader(400)
			_, _ = w.Write([]byte(`{"status":"error","error":"bad query"}`))
			return
		}
		_, _ = w.Write([]byte(`{"status":"success","data":{"result":[
			{"metric":{"persistentvolumeclaim":"a"},"value":[1,"0.75"]},
			{"metric":{"persistentvolumeclaim":"b"},"value":[1,"NaNx"]}]}}`))
	}))
	defer srv.Close()
	q := VMQuery(srv.URL + "/")

	// WHEN: the disk query and a bad query run
	s, err := q(ctx, "kubelet_volume_stats_used_bytes")
	_, bad := q(ctx, "up")

	// THEN: the parsable sample comes back, the bad query fails
	if err != nil || len(s) != 1 || s[0].Value != 0.75 || s[0].Labels["persistentvolumeclaim"] != "a" || bad == nil {
		t.Fatalf("%v %v %v", s, err, bad)
	}
}

func Test_shouldFailTheQueryWhenVictoriaMetricsReportsAnErrorOrIsDown(t *testing.T) {
	// GIVEN: a server answering status error with 200 and an unreachable one
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"status":"error","error":"too many series"}`))
	}))
	defer srv.Close()

	// WHEN: queries run against both and with an invalid URL
	_, err1 := VMQuery(srv.URL)(ctx, "x")
	_, err2 := VMQuery("http://127.0.0.1:1")(ctx, "x")
	_, err3 := VMQuery("http://bad\x7f")(ctx, "x")

	// THEN: all fail
	if err1 == nil || !strings.Contains(err1.Error(), "too many series") || err2 == nil || err3 == nil {
		t.Fatalf("%v %v %v", err1, err2, err3)
	}
}

func Test_shouldCreateARegionAnnotationWhenGrafanaIsTold(t *testing.T) {
	// GIVEN: Grafana recording annotation calls (basic auth admin/admin)
	var calls []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		user, pass, _ := r.BasicAuth()
		calls = append(calls, r.Method+" "+r.URL.Path+" "+user+":"+pass+" "+string(b))
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 42, "message": "ok"})
	}))
	defer srv.Close()
	g := &GrafanaAnnotator{Base: srv.URL, User: "admin", Password: "admin"}
	start := time.UnixMilli(1000)

	// WHEN: an annotation is started, closed as region and re-tagged without end
	id, err1 := g.Start(ctx, start, "Black Friday", []string{"chaos", "black-friday"})
	err2 := g.Update(ctx, id, time.UnixMilli(5000), "Black Friday", []string{"chaos"})
	err3 := g.Update(ctx, id, time.Time{}, "renamed", nil)

	// THEN: Grafana got POST + 2 PATCH with basic auth, the region end only on the first PATCH
	if err1 != nil || err2 != nil || err3 != nil || id != 42 || len(calls) != 3 {
		t.Fatalf("%v %v %v %v", calls, err1, err2, err3)
	}
	if !strings.HasPrefix(calls[0], "POST /api/annotations admin:admin") || !strings.Contains(calls[1], `"timeEnd":5000`) || strings.Contains(calls[2], "timeEnd") {
		t.Fatalf("calls %v", calls)
	}
}

func Test_shouldFailAnnotationsWhenGrafanaRefusesThem(t *testing.T) {
	// GIVEN: Grafana answering 401
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(401) }))
	defer srv.Close()
	g := &GrafanaAnnotator{Base: srv.URL}
	bad := &GrafanaAnnotator{Base: "http://bad\x7f"}

	// WHEN: annotations are written
	_, err1 := g.Start(ctx, time.Now(), "x", nil)
	err2 := g.Update(ctx, 1, time.Now(), "x", nil)
	_, err3 := bad.Start(ctx, time.Now(), "x", nil)
	err4 := bad.Update(ctx, 1, time.Now(), "x", nil)

	// THEN: they fail
	if err1 == nil || err2 == nil || err3 == nil || err4 == nil {
		t.Fatalf("%v %v %v %v", err1, err2, err3, err4)
	}
}
