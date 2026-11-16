// collector package handles all the functions related to gathering the data
// from RGW and marshalling into Prometheus metrics
package collector

import (
	"context"
	"time"

	// "log"
	"sync"

	"github.com/pcuzner/rgw-exporter/defaults"

	log "github.com/sirupsen/logrus"

	rgw "github.com/ceph/go-ceph/rgw/admin"
	"github.com/prometheus/client_golang/prometheus"
)

type rgwCollector struct {
	config               defaults.Config
	rgwUser              *prometheus.Desc
	bucketSize           *prometheus.Desc
	bucketSizeActual     *prometheus.Desc
	bucketSizeKB         *prometheus.Desc
	bucketSizeKBActual   *prometheus.Desc
	bucketObjects        *prometheus.Desc
	numShards            *prometheus.Desc
	summaryBuckets       *prometheus.Desc
	summaryUsage         *prometheus.Desc
	summaryUsageActual   *prometheus.Desc
	summaryUsageKB       *prometheus.Desc
	summaryUsageKBActual *prometheus.Desc
	bucketSizeQuota      *prometheus.Desc
	bucketObjectsQuota   *prometheus.Desc
	userSizeQuota        *prometheus.Desc
	userObjectsQuota     *prometheus.Desc
}

// UserStats holds the summary data for a given users S3 usage
type UserStats struct {
	bucketCount      float64
	totalBytes       float64
	totalActualBytes float64
	totalKB          float64
	totalKBActual    float64
}

// NewRGWCollector creates a new collector containing the the runtime
// configuration and the prometheus metrics that the collector will return
func NewRGWCollector(config *defaults.Config) *rgwCollector {
	return &rgwCollector{
		config: *config,
		rgwUser: prometheus.NewDesc("ceph_rgw_user",
			"RGW user",
			[]string{"uid"}, nil,
		),
		bucketSize: prometheus.NewDesc("ceph_rgw_bucket_usage_bytes",
			"Logical size of objects in a bucket in bytes (RGW size)",
			[]string{"uid", "bucket"}, nil,
		),
		bucketSizeActual: prometheus.NewDesc("ceph_rgw_bucket_usage_actual_bytes",
			"Size of objects in a bucket rounded per object to 4 KiB in bytes (RGW size_actual)",
			[]string{"uid", "bucket"}, nil,
		),
		bucketSizeKB: prometheus.NewDesc("ceph_rgw_bucket_usage_kibibytes",
			"Logical size of objects in a bucket in KiB as reported by RGW (size_kb)",
			[]string{"uid", "bucket"}, nil,
		),
		bucketSizeKBActual: prometheus.NewDesc("ceph_rgw_bucket_usage_actual_kibibytes",
			"Size of objects in a bucket rounded per object to 4 KiB in KiB as reported by RGW (size_kb_actual)",
			[]string{"uid", "bucket"}, nil,
		),
		bucketObjects: prometheus.NewDesc("ceph_rgw_bucket_object_count",
			"Count of objects stored in a bucket",
			[]string{"uid", "bucket", "type"}, nil,
		),
		numShards: prometheus.NewDesc("ceph_rgw_bucket_shard_count",
			"The number of RADOS objects(shards) a bucket index is using",
			[]string{"uid", "bucket"}, nil,
		),
		summaryBuckets: prometheus.NewDesc("ceph_rgw_user_total_bucket_count",
			"Total number of buckets owned by the user",
			[]string{"uid"}, nil,
		),
		summaryUsage: prometheus.NewDesc("ceph_rgw_user_total_usage_bytes",
			"Total logical size of objects owned by a user in bytes (RGW size)",
			[]string{"uid"}, nil,
		),
		summaryUsageActual: prometheus.NewDesc("ceph_rgw_user_total_usage_actual_bytes",
			"Total size of objects owned by a user rounded per object to 4 KiB in bytes (RGW size_actual)",
			[]string{"uid"}, nil,
		),
		summaryUsageKB: prometheus.NewDesc("ceph_rgw_user_total_usage_kibibytes",
			"Sum of bucket logical sizes owned by a user in KiB as reported by RGW (size_kb)",
			[]string{"uid"}, nil,
		),
		summaryUsageKBActual: prometheus.NewDesc("ceph_rgw_user_total_usage_actual_kibibytes",
			"Sum of bucket sizes owned by a user rounded per object to 4 KiB in KiB as reported by RGW (size_kb_actual)",
			[]string{"uid"}, nil,
		),
		bucketSizeQuota: prometheus.NewDesc("ceph_rgw_bucket_bytes_quota",
			"Quota defined for a bucket in bytes",
			[]string{"uid", "bucket"}, nil,
		),
		bucketObjectsQuota: prometheus.NewDesc("ceph_rgw_bucket_object_quota",
			"Quota defined for a bucket in a number of objects",
			[]string{"uid", "bucket"}, nil,
		),
		userSizeQuota: prometheus.NewDesc("ceph_rgw_user_bytes_quota",
			"Quota defined for a user in bytes",
			[]string{"uid"}, nil,
		),
		userObjectsQuota: prometheus.NewDesc("ceph_rgw_user_objects_quota",
			"Quota defined for a user in a number of objects",
			[]string{"uid"}, nil,
		),
	}
}

// Describe returns the metric metadata
func (collector *rgwCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- collector.rgwUser
	ch <- collector.bucketSize
	ch <- collector.bucketSizeActual
	ch <- collector.bucketSizeKB
	ch <- collector.bucketSizeKBActual
	ch <- collector.bucketObjects
	ch <- collector.numShards
	ch <- collector.summaryBuckets
	ch <- collector.summaryUsage
	ch <- collector.summaryUsageActual
	ch <- collector.summaryUsageKB
	ch <- collector.summaryUsageKBActual
	ch <- collector.bucketSizeQuota
	ch <- collector.bucketObjectsQuota
	ch <- collector.userSizeQuota
	ch <- collector.userObjectsQuota
}

// Collect handles the data gathering from RGW and pushes the metrics
// onto the channel for return to the Prometheus server
func (collector *rgwCollector) Collect(ch chan<- prometheus.Metric) {

	connection, err := getRGWConnection(collector.config.Endpoints, 0, collector.config.AccessKey, collector.config.SecretKey)
	if err != nil {
		log.Fatalln("Unable to connect to any endpoint. Aborting")
	}

	users := collectUsers(connection)
	var wg sync.WaitGroup
	queue := make(chan []rgw.Bucket, len(users))
	userQuotas := make(chan rgw.QuotaSpec, len(users))
	wg.Add(len(users) * 2)

	userSummary := make(map[string]UserStats, len(users))

	for _, uid := range users {
		userSummary[uid] = UserStats{}
		metric := prometheus.MustNewConstMetric(collector.rgwUser, prometheus.GaugeValue, 1, uid)
		ch <- metric
	}
	start := time.Now()
	hostIdx := 0
	for _, uid := range users {
		conn, err := getRGWConnection(collector.config.Endpoints, hostIdx, collector.config.AccessKey, collector.config.SecretKey)
		if err != nil {
			log.Fatalln("Unable to connect to any endpoint. Aborting")
		}
		log.Debug("bucket list for uid ", uid, " using ", conn.Endpoint)
		go collectBucketStats(conn, uid, &wg, queue)
		go collectUserQuota(conn, uid, &wg, userQuotas)
		hostIdx++
		if hostIdx > len(collector.config.Endpoints)-1 {
			hostIdx = 0
		}
	}

	log.Debug("Waiting for go routines to finish")
	wg.Wait()
	elapsed := time.Since(start)
	log.Debugf("go routines completed in : %s", elapsed)

	close(queue)
	close(userQuotas)

	log.Info("Processing the users quota data")
	for userQuota := range userQuotas {
		if userQuota.Enabled == nil || !*userQuota.Enabled {
			log.Debugf("No quota for uid %s", userQuota.UID)
			continue
		}
		log.Debugf("Quota found and enabled for uid %s", userQuota.UID)
		if userQuota.MaxSize != nil {
			metric := prometheus.MustNewConstMetric(
				collector.userSizeQuota, prometheus.GaugeValue,
				float64(*userQuota.MaxSize),
				userQuota.UID)
			ch <- metric
		}
		if userQuota.MaxObjects != nil {
			metric := prometheus.MustNewConstMetric(
				collector.userObjectsQuota, prometheus.GaugeValue,
				float64(*userQuota.MaxObjects),
				userQuota.UID)
			ch <- metric
		}
	}

	log.Info("Processing the bucket stats data")
	for bucketData := range queue {
		if len(bucketData) == 0 {
			continue
		}
		for _, bucketInfo := range bucketData {
			var sizeFlag, objectFlag bool = false, false

			log.Debug("Processing buckets for user: ", bucketInfo.Owner)
			var entry UserStats = userSummary[bucketInfo.Owner]

			usage := bucketInfo.Usage.RgwMain
			reportSize := usage.Size != nil && *usage.Size >= collector.config.ThresholdSize
			if usage.Size == nil && usage.SizeKb != nil {
				// Compare KiB to a byte threshold without overflowing the RGW counter.
				thresholdKB := collector.config.ThresholdSize / 1024
				if collector.config.ThresholdSize%1024 != 0 {
					thresholdKB++
				}
				reportSize = *usage.SizeKb >= thresholdKB
			}
			for _, size := range []struct {
				value *uint64
				desc  *prometheus.Desc
				total *float64
			}{
				{usage.Size, collector.bucketSize, &entry.totalBytes},
				{usage.SizeActual, collector.bucketSizeActual, &entry.totalActualBytes},
				{usage.SizeKb, collector.bucketSizeKB, &entry.totalKB},
				{usage.SizeKbActual, collector.bucketSizeKBActual, &entry.totalKBActual},
			} {
				if size.value == nil {
					continue
				}
				*size.total += float64(*size.value)
				if reportSize {
					ch <- prometheus.MustNewConstMetric(size.desc, prometheus.GaugeValue,
						float64(*size.value), bucketInfo.Owner, bucketInfo.Bucket)
					sizeFlag = true
				}
			}
			if bucketInfo.Usage.RgwMain.NumObjects != nil {
				if *bucketInfo.Usage.RgwMain.NumObjects >= collector.config.ThresholdObjects {
					metric := prometheus.MustNewConstMetric(
						collector.bucketObjects, prometheus.GaugeValue,
						float64(*bucketInfo.Usage.RgwMain.NumObjects),
						bucketInfo.Owner, bucketInfo.Bucket, "main")
					ch <- metric
					objectFlag = true
				}
			}
			if bucketInfo.Usage.RgwMultimeta.NumObjects != nil {
				if *bucketInfo.Usage.RgwMultimeta.NumObjects >= collector.config.ThresholdObjects {
					metric := prometheus.MustNewConstMetric(
						collector.bucketObjects, prometheus.GaugeValue,
						float64(*bucketInfo.Usage.RgwMultimeta.NumObjects),
						bucketInfo.Owner, bucketInfo.Bucket, "multimeta")
					ch <- metric
					objectFlag = true
				}
			}

			bucketQuota := bucketInfo.BucketQuota
			if bucketQuota.Enabled != nil && *bucketQuota.Enabled {
				if bucketQuota.MaxSize != nil {
					metric := prometheus.MustNewConstMetric(
						collector.bucketSizeQuota, prometheus.GaugeValue,
						float64(*bucketQuota.MaxSize),
						bucketInfo.Owner, bucketInfo.Bucket)
					ch <- metric
				}
				if bucketQuota.MaxObjects != nil {
					metric := prometheus.MustNewConstMetric(
						collector.bucketObjectsQuota, prometheus.GaugeValue,
						float64(*bucketQuota.MaxObjects),
						bucketInfo.Owner, bucketInfo.Bucket)
					ch <- metric
				}
			}

			if (sizeFlag || objectFlag) && bucketInfo.NumShards != nil {
				metric := prometheus.MustNewConstMetric(
					collector.numShards,
					prometheus.GaugeValue,
					float64(*bucketInfo.NumShards),
					bucketInfo.Owner, bucketInfo.Bucket)
				ch <- metric
			}

			entry.bucketCount += 1
			userSummary[bucketInfo.Owner] = entry
		}

	}
	for uid, summaryStats := range userSummary {
		totalBuckets := prometheus.MustNewConstMetric(
			collector.summaryBuckets, prometheus.GaugeValue,
			summaryStats.bucketCount,
			uid)
		ch <- totalBuckets

		for _, usage := range []struct {
			desc  *prometheus.Desc
			value float64
		}{
			{collector.summaryUsage, summaryStats.totalBytes},
			{collector.summaryUsageActual, summaryStats.totalActualBytes},
			{collector.summaryUsageKB, summaryStats.totalKB},
			{collector.summaryUsageKBActual, summaryStats.totalKBActual},
		} {
			ch <- prometheus.MustNewConstMetric(usage.desc, prometheus.GaugeValue, usage.value, uid)
		}
	}
	log.Info("Processing complete")
}

// getRGWConnection returns an API connection to an RGW gateway
// It receives the list of potenital hosts and an offset to try. If a connection fails it
// will continue to the next host until all hosts are exhausted - at which point it returns an error
func getRGWConnection(hosts []string, offset int, accessKey string, secretKey string) (*rgw.API, error) {
	var failures int = 0
	var conn *rgw.API
	var err error
	var targets []string

	if offset == 0 {
		targets = hosts
	} else {
		targets = append(targets, hosts[offset:]...)
		targets = append(targets, hosts[0:(offset-1)]...)
	}

	for _, hostName := range targets {
		conn, err = rgw.New(hostName, accessKey, secretKey, nil)
		if err == nil {
			break
		}
		log.Warning("Unable to connect to ", hostName)
		failures++

	}
	if failures == len(hosts) {
		log.Error("All hosts given have been tried and are NOT reachable.")
	}
	return conn, err
}

// collectUsers uses the rgw API to return a list of RGW users
func collectUsers(connection *rgw.API) []string {
	users, err := connection.GetUsers(context.Background())
	if err != nil {
		log.Error("Unable to get a user list: ", err)
		// GetUsers can return nil instead of an empty slice when there is a problem,
		// so to account for that, we return an empty slice here.
		return []string{}
	}
	return *users
}

// collectBucketStats returns the bucket stats for all buckets owned by a given user
func collectBucketStats(connection *rgw.API, uid string, wg *sync.WaitGroup, ch chan<- []rgw.Bucket) {
	log.Debugf("bucket stats for user '%s' starting", uid)
	userBuckets, err := connection.ListUsersBucketsWithStat(context.Background(), uid)
	if err != nil {
		log.Warning("Warning: Unable to list buckets for uid ", uid, " : ", err)
	}
	log.Debugf("bucket stats for user '%s' complete", uid)
	ch <- userBuckets
	defer wg.Done()
}

func collectUserQuota(connection *rgw.API, uid string, wg *sync.WaitGroup, ch chan<- rgw.QuotaSpec) {
	log.Debugf("user quota for user '%s' starting", uid)
	userQuota, err := connection.GetUserQuota(context.Background(), rgw.QuotaSpec{UID: uid})
	if err != nil {
		log.Warning("Warning: Unable to get quota for uid ", uid, " : ", err)
	}
	userQuota.UID = uid
	log.Debugf("user quota for user '%s' complete", uid)
	ch <- userQuota
	defer wg.Done()
}
