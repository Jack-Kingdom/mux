package mux

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	preciseBuckets    = []float64{0.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5}
	preciseObjectives = map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001}
)

var (
	dispatchFrameDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "DispatchFrameDuration",
		Help:    "session 分发数据帧耗时",
		Buckets: preciseBuckets,
	})
)
