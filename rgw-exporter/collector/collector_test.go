package collector

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/pcuzner/rgw-exporter/defaults"
	"github.com/prometheus/client_golang/prometheus"
)

func TestCollectUsage(t *testing.T) {
	const buckets = `[
		{"bucket":"large","owner":"alice","num_shards":11,"usage":{"rgw.main":{
			"size":1075028,"size_actual":1331200,"size_kb":1050,"size_kb_actual":1300,"num_objects":104
		}}},
		{"bucket":"small","owner":"alice","num_shards":1,"usage":{"rgw.main":{
			"size":1,"size_actual":4096,"size_kb":1,"size_kb_actual":4,"num_objects":1
		}}}
	]`
	for _, threshold := range []uint64{0, 1075028, 1075029} {
		t.Run(fmt.Sprintf("threshold_%d", threshold), func(t *testing.T) {
			metrics := gatherMetrics(t, map[string]string{
				"alice": buckets,
				"bob": `[{"bucket":"other","owner":"bob","num_shards":1,"usage":{"rgw.main":{
					"size":42,"size_actual":4096,"size_kb":1,"size_kb_actual":4,"num_objects":1
				}}}]`,
				"empty": `[]`,
			}, threshold)
			for _, usage := range []struct {
				suffix              string
				large, small, other float64
			}{
				{"bytes", 1075028, 1, 42},
				{"actual_bytes", 1331200, 4096, 4096},
				{"kibibytes", 1050, 1, 1},
				{"actual_kibibytes", 1300, 4, 4},
			} {
				bucketMetric := "ceph_rgw_bucket_usage_" + usage.suffix
				assertMetric(t, metrics, bucketMetric, "alice/large", usage.large, threshold <= 1075028)
				assertMetric(t, metrics, bucketMetric, "alice/small", usage.small, threshold == 0)
				assertMetric(t, metrics, bucketMetric, "bob/other", usage.other, threshold == 0)
				userMetric := "ceph_rgw_user_total_usage_" + usage.suffix
				assertMetric(t, metrics, userMetric, "alice", usage.large+usage.small, true)
				assertMetric(t, metrics, userMetric, "bob", usage.other, true)
				assertMetric(t, metrics, userMetric, "empty", 0, true)
			}
			assertMetric(t, metrics, "ceph_rgw_user_total_bucket_count", "alice", 2, true)
			assertMetric(t, metrics, "ceph_rgw_bucket_shard_count", "alice/large", 11, threshold <= 1075028)
		})
	}
}

func TestCollectPartialUsage(t *testing.T) {
	for _, tc := range []struct {
		name, usage string
		threshold   uint64
		want        map[string]float64
	}{
		{"bytes_only", `{"size":12600,"size_actual":20480}`, 0,
			map[string]float64{"bytes": 12600, "actual_bytes": 20480}},
		{"kibibytes_only", `{"size_kb":13,"size_kb_actual":20}`, 13 * 1024,
			map[string]float64{"kibibytes": 13, "actual_kibibytes": 20}},
		{"kibibytes_below_threshold", `{"size_kb":13,"size_kb_actual":20}`, 13*1024 + 1, nil},
		{"byte_threshold_precedes_kibibyte_rounding", `{"size":1023,"size_actual":4096,"size_kb":1,"size_kb_actual":4}`, 1024, nil},
		{"empty_usage", `{}`, 0, nil},
		{"zero_usage", `{"size":0,"size_actual":0,"size_kb":0,"size_kb_actual":0}`, 0,
			map[string]float64{"bytes": 0, "actual_bytes": 0, "kibibytes": 0, "actual_kibibytes": 0}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			metrics := gatherMetrics(t, map[string]string{
				"alice": fmt.Sprintf(`[{"bucket":"partial","owner":"alice","usage":{"rgw.main":%s}}]`, tc.usage),
			}, tc.threshold)
			for _, suffix := range []string{"bytes", "actual_bytes", "kibibytes", "actual_kibibytes"} {
				value, present := tc.want[suffix]
				assertMetric(t, metrics, "ceph_rgw_bucket_usage_"+suffix, "alice/partial", value, present)
			}
			assertMetric(t, metrics, "ceph_rgw_bucket_shard_count", "alice/partial", 0, false)
		})
	}
}

func gatherMetrics(t *testing.T, bucketsByUser map[string]string, threshold uint64) map[string]map[string]float64 {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/admin/metadata/user":
			users := make([]string, 0, len(bucketsByUser))
			for uid := range bucketsByUser {
				users = append(users, uid)
			}
			if err := json.NewEncoder(w).Encode(users); err != nil {
				t.Error(err)
			}
		case "/admin/bucket":
			if r.URL.Query().Get("stats") != "true" {
				t.Error("bucket request is missing stats=true")
			}
			fmt.Fprint(w, bucketsByUser[r.URL.Query().Get("uid")])
		case "/admin/user":
			if !r.URL.Query().Has("quota") || r.URL.Query().Get("quota-type") != "user" {
				t.Error("unexpected user request: " + r.URL.String())
			}
			fmt.Fprint(w, `{"enabled":false}`)
		default:
			t.Error("unexpected RGW request: " + r.URL.String())
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	registry := prometheus.NewPedanticRegistry()
	registry.MustRegister(NewRGWCollector(&defaults.Config{
		Endpoints:        []string{server.URL},
		AccessKey:        "test-access-key",
		SecretKey:        "test-secret-key",
		ThresholdSize:    threshold,
		ThresholdObjects: 1000,
	}))
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	result := make(map[string]map[string]float64)
	for _, family := range families {
		values := make(map[string]float64)
		for _, metric := range family.Metric {
			var uid, bucket, category string
			for _, label := range metric.Label {
				switch label.GetName() {
				case "uid":
					uid = label.GetValue()
				case "bucket":
					bucket = "/" + label.GetValue()
				case "type":
					category = "/" + label.GetValue()
				}
			}
			if metric.GetGauge() == nil {
				t.Fatalf("%s is not a gauge", family.GetName())
			}
			values[uid+bucket+category] = metric.GetGauge().GetValue()
		}
		result[family.GetName()] = values
	}
	return result
}

func assertMetric(t *testing.T, metrics map[string]map[string]float64, name, labels string, want float64, present bool) {
	t.Helper()
	got, exists := metrics[name][labels]
	if exists != present || (present && got != want) {
		t.Errorf("%s{%s} = %v (present=%t), want %v (present=%t)", name, labels, got, exists, want, present)
	}
}
