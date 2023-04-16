package mux

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	preciseBuckets = []float64{0.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5}
)

var (
	sendFrameDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "SendFrameDuration",
		Help:    "stream 发送数据帧耗时",
		Buckets: preciseBuckets,
	})

	dispatchFrameDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "DispatchFrameDuration",
		Help:    "session 分发数据帧耗时",
		Buckets: preciseBuckets,
	})

	sessionLifetimeDurationSummary = promauto.NewSummary(prometheus.SummaryOpts{
		Name: "SessionLifetimeDurationSummary",
		Help: "session 存活时间",
	})

	streamLifetimeDurationSummary = promauto.NewSummary(prometheus.SummaryOpts{
		Name: "StreamLifetimeDurationSummary",
		Help: "stream 持续时间",
	})
)
