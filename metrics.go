package mux

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	PreciseBuckets = []float64{0.001, 0.003, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10}
)

var (
	GetDataFrameDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "GetDataFrameDuration",
		Help:    "获取 stream 数据帧耗时",
		Buckets: PreciseBuckets,
	})
)
