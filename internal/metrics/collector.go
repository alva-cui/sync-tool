package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// CDC & 同步引擎指标
var (
	CDCEventsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "sync_cdc_events_total",
		Help: "Total number of CDC events processed (insert/update/delete)",
	}, []string{"task_id", "driver", "event_type"})

	CDCReplicationLag = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "sync_cdc_replication_lag_seconds",
		Help: "Time difference between source event generation and target write",
	}, []string{"task_id", "driver"})

	TargetWriteDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "sync_target_write_duration_seconds",
		Help:    "Duration to write a batch of rows to the target database",
		Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5},
	}, []string{"task_id", "driver"})

	DLQSize = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "sync_dlq_size",
		Help: "Current number of failed rows pending in the Dead Letter Queue",
	}, []string{"task_id"})

	ActiveTasks = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "sync_active_tasks",
		Help: "Number of currently running sync tasks",
	})
)

// 便捷更新函数
func IncCDCEvent(taskID, driver, eventType string) {
	CDCEventsTotal.WithLabelValues(taskID, driver, eventType).Inc()
}

func SetReplicationLag(taskID, driver string, lagSeconds float64) {
	CDCReplicationLag.WithLabelValues(taskID, driver).Set(lagSeconds)
}

func ObserveTargetWrite(taskID, driver string, duration float64) {
	TargetWriteDuration.WithLabelValues(taskID, driver).Observe(duration)
}

func IncDLQSize(taskID string) {
	DLQSize.WithLabelValues(taskID).Inc()
}

func DecDLQSize(taskID string) {
	DLQSize.WithLabelValues(taskID).Dec()
}

func SetActiveTasks(count int) {
	ActiveTasks.Set(float64(count))
}
