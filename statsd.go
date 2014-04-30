package metrics

import (
	"log"
	"time"
	"github.com/ninetwentyfour/go-statsdclient"
)

// StatsdConfig provides a container with configuration parameters for
// the Statsd exporter
type StatsdConfig struct {
	Addr          string  // Network address to connect to
	Registry      Registry      // Registry to be exported
	FlushInterval time.Duration // Flush interval
	DurationUnit  time.Duration // Time conversion unit for durations
	Prefix        string        // Prefix to be prepended to metric names
}

// Statsd is a blocking exporter function which reports metrics in r
// to a statsd server located at addr, flushing them every d duration
// and prepending metric names with prefix.
func Statsd(r Registry, d time.Duration, prefix string, addr string) {
	StatsdWithConfig(StatsdConfig{
		Addr:          addr,
		Registry:      r,
		FlushInterval: d,
		DurationUnit:  time.Nanosecond,
		Prefix:        prefix,
	})
}

// StatsdWithConfig is a blocking exporter function just like Statsd,
// but it takes a StatsdConfig instead.
func StatsdWithConfig(c StatsdConfig) {
	for _ = range time.Tick(c.FlushInterval) {
		if err := statsd(&c); nil != err {
			log.Println(err)
		}
	}
}

func statsd(c *StatsdConfig) error {
	du := float64(c.DurationUnit)
	s, _ := statsdclient.Dial(c.Addr)

	c.Registry.Each(func(name string, i interface{}) {
		switch metric := i.(type) {
		case Counter:
			s.Increment(c.Prefix+"."+name+".count", int(metric.Count()), c.FlushInterval.Seconds())
		case Gauge:
			s.GaugeInt64(c.Prefix+"."+name+".value", metric.Value(), c.FlushInterval.Seconds())
		case GaugeFloat64:
			s.GaugeFloat64(c.Prefix+"."+name+".value", metric.Value(), c.FlushInterval.Seconds())
		case Timer:
			t := metric.Snapshot()
			ps := t.Percentiles([]float64{0.5, 0.75, 0.95, 0.99, 0.999})
			s.GaugeInt64(c.Prefix+"."+name+".count", t.Count(), c.FlushInterval.Seconds())
			s.GaugeInt64(c.Prefix+"."+name+".min", int64(du)*t.Min(), c.FlushInterval.Seconds())
			s.GaugeInt64(c.Prefix+"."+name+".max", int64(du)*t.Max(), c.FlushInterval.Seconds())
			s.GaugeFloat64(c.Prefix+"."+name+".mean", du*t.Mean(), c.FlushInterval.Seconds())
			s.GaugeFloat64(c.Prefix+"."+name+".std-dev", du*t.StdDev(), c.FlushInterval.Seconds())
			s.GaugeFloat64(c.Prefix+"."+name+".50-percentile", du*ps[0], c.FlushInterval.Seconds())
			s.GaugeFloat64(c.Prefix+"."+name+".75-percentile", du*ps[1], c.FlushInterval.Seconds())
			s.GaugeFloat64(c.Prefix+"."+name+".95-percentile", du*ps[2], c.FlushInterval.Seconds())
			s.GaugeFloat64(c.Prefix+"."+name+".99-percentile", du*ps[3], c.FlushInterval.Seconds())
			s.GaugeFloat64(c.Prefix+"."+name+".999-percentile", du*ps[4], c.FlushInterval.Seconds())
			s.GaugeFloat64(c.Prefix+"."+name+".one-minute", t.Rate1(), c.FlushInterval.Seconds())
			s.GaugeFloat64(c.Prefix+"."+name+".five-minute", t.Rate5(), c.FlushInterval.Seconds())
			s.GaugeFloat64(c.Prefix+"."+name+".fifteen-minute", t.Rate15(), c.FlushInterval.Seconds())
			s.GaugeFloat64(c.Prefix+"."+name+".mean-rate", t.RateMean(), c.FlushInterval.Seconds())
		}
	})

	s.Close()
	return nil
}
