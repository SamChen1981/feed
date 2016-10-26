// Package instrument wraps memcache\redis\mysql resources operation with instrument options
// to observe latency and log verbose.
package instrument

import (
	"time"
)

type Options struct {
	Tag            string
	ObserveLatency func(latency time.Duration)
	LogVerbose     func(v ...interface{})
}

var defaultOptions = &Options{
	ObserveLatency: func(latency time.Duration) {},
	LogVerbose:     func(v ...interface{}) {},
}
