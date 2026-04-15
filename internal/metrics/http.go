package metrics

import (
	"fmt"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	APIRequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "sync_api_request_duration_seconds",
		Help:    "HTTP API request latency",
		Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
	}, []string{"method", "path", "status_code"})

	APIRequestTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "sync_api_requests_total",
		Help: "Total HTTP API requests processed",
	}, []string{"method", "path", "status_code"})
)

func HTTPMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()
		elapsed := time.Since(start).Seconds()
		status := c.Writer.Status()
		path := c.FullPath()
		if path == "" {
			path = c.Request.URL.Path
		}

		labelStatus := fmt.Sprintf("%d", status)
		APIRequestDuration.WithLabelValues(c.Request.Method, path, labelStatus).Observe(elapsed)
		APIRequestTotal.WithLabelValues(c.Request.Method, path, labelStatus).Inc()
	}
}
