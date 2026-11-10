package tuning

import (
	"errors"
	"testing"

	"kafkalab/controlboard/backend/internal/api"
)

func Test_shouldFlagWeakAcksAsLossyWhenThePlayerTradesSafetyForSpeed(t *testing.T) {
	// GIVEN: payment's producer set to acks=leader with lz4 compression
	v := api.Values{"acks": "leader", "compression": "lz4"}

	// WHEN: the settings are checked for weakened delivery guarantees
	lossy := Lossy(v)

	// THEN: only the acks setting is flagged
	if len(lossy) != 1 || lossy[0] != "acks=leader" {
		t.Fatalf("lossy %v", lossy)
	}
}

func Test_shouldPublishOnlyChangedSettingsWhenConvertingToClientTuning(t *testing.T) {
	// GIVEN: a service with acks=all, zstd, 50 ms linger and 1000 records per poll (JSON and Go numbers)
	v := api.Values{"acks": "all", "compression": "zstd", "lingerMs": 50.0, "maxPollRecords": 1000}

	// WHEN: they are converted for the control snapshot
	c := ToClient(v)

	// THEN: the given settings are set, everything else keeps the service default (nil)
	if *c.Acks != "all" || *c.Compression != "zstd" || *c.LingerMs != 50 || *c.MaxPollRecords != 1000 {
		t.Fatalf("client %+v", c)
	}
	if c.BatchMaxBytes != nil || c.FetchMaxBytes != nil || c.FetchMinBytes != nil || c.FetchMaxWaitMs != nil {
		t.Fatalf("unexpected defaults overridden: %+v", c)
	}
}

func Test_shouldKeepEveryServiceDefaultWhenAServiceIsNotTuned(t *testing.T) {
	// GIVEN: a service nobody has tuned
	var v api.Values

	// WHEN: its client settings are built
	c := ToClient(v)

	// THEN: nothing is overridden
	if c != (ToClient(api.Values{})) || c.Acks != nil || c.LingerMs != nil {
		t.Fatalf("client %+v", c)
	}
}

func Test_shouldRejectSettingsOutsideTheAllowlistWhenValidating(t *testing.T) {
	// GIVEN: an unknown key, an unknown codec, a too long linger, a fractional poll size and a string number
	bad := []api.Values{
		{"retries": 3.0},
		{"compression": "brotli"},
		{"lingerMs": 501.0},
		{"maxPollRecords": 10.5},
		{"fetchMinBytes": "1"},
	}

	// WHEN / THEN: each is refused as an invalid request
	for _, v := range bad {
		if err := Validate(v); !errors.Is(err, api.ErrInvalid) {
			t.Fatalf("%v: %v", v, err)
		}
	}
}

func Test_shouldAcceptValidSettingsAndResetsWhenValidating(t *testing.T) {
	// GIVEN: allowed values plus a reset of linger to its default (nil)
	v := api.Values{"acks": "none", "batchMaxBytes": 65536, "fetchMaxWaitMs": 100.0, "lingerMs": nil}

	// WHEN: validated
	err := Validate(v)

	// THEN: they are accepted
	if err != nil {
		t.Fatal(err)
	}
}

func Test_shouldDropASettingWhenItIsResetToDefault(t *testing.T) {
	// GIVEN: a service tuned with linger 100 ms and gzip
	cur := api.Values{"lingerMs": 100.0, "compression": "gzip"}

	// WHEN: linger is reset and acks changed
	got := Merge(cur, api.Values{"lingerMs": nil, "acks": "all"})

	// THEN: linger is gone, compression stays, acks is added, the input is untouched
	if _, ok := got["lingerMs"]; ok || got["compression"] != "gzip" || got["acks"] != "all" || cur["lingerMs"] != 100.0 {
		t.Fatalf("merged %v (cur %v)", got, cur)
	}
}

func Test_shouldListEverySettingWithHelpWhenTheCatalogueIsRequested(t *testing.T) {
	// GIVEN: the Tuning Lab catalogue
	s := Settings()

	// WHEN: its keys are collected
	keys := map[string]bool{}
	for _, x := range s {
		keys[x.Key] = x.Help != ""
	}

	// THEN: all eight contract keys are there, each with a help text
	for _, k := range []string{"acks", "lingerMs", "batchMaxBytes", "compression", "fetchMaxBytes", "fetchMaxWaitMs", "fetchMinBytes", "maxPollRecords"} {
		if !keys[k] {
			t.Fatalf("missing %s", k)
		}
	}
}
