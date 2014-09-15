package metrics

import (
	"bufio"
	"fmt"
	"log"
	"math/rand"
	"net"
	"strconv"
	"sync"
	"time"
)

// StatsdConfig provides a container with configuration parameters for
// the Statsd exporter
type StatsdConfig struct {
	Addr          string        // Network address to connect to
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

	s, err := Dial(c.Addr)
	if err != nil {
		return err
	}

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

// statsd client stuff

const (
	defaultBufSize = 512
)

type StatsClient interface {
	Increment(stat string, count int, rate float64) error
	GaugeFloat64(stat string, value float64, rate float64) error
	GaugeInt64(stat string, value int64, rate float64) error
	Close() error
}

// A statsd client representing a connection to a statsd server.
type client struct {
	conn net.Conn
	buf  *bufio.Writer
	m    sync.Mutex

	// The prefix to be added to every key. Should include the "." at the end if desired
	prefix string
}

// Dial connects to the given address on the given network using net.Dial and then returns a new client for the connection.
func Dial(addr string) (StatsClient, error) {
	conn, err := net.Dial("udp", addr)
	if err != nil {
		return nil, err
	}
	return newClient(conn, 0), nil
}

// DialTimeout acts like Dial but takes a timeout. The timeout includes name resolution, if required.
func DialTimeout(addr string, timeout time.Duration) (StatsClient, error) {
	conn, err := net.DialTimeout("udp", addr, timeout)
	if err != nil {
		return nil, err
	}
	return newClient(conn, 0), nil
}

// DialSize acts like Dial but takes a packet size.
// By default, the packet size is 512, see https://github.com/etsy/statsd/blob/master/docs/metric_types.md#multi-metric-packets for guidelines.
func DialSize(addr string, size int) (StatsClient, error) {
	conn, err := net.Dial("udp", addr)
	if err != nil {
		return nil, err
	}
	return newClient(conn, size), nil
}

func newClient(conn net.Conn, size int) *client {
	if size <= 0 {
		size = defaultBufSize
	}
	return &client{
		conn: conn,
		buf:  bufio.NewWriterSize(conn, size),
	}
}

// Increment the counter for the given bucket.
func (c *client) Increment(stat string, count int, rate float64) error {
	return c.send(stat, rate, strconv.Itoa(count)+"|c")
}

// Record arbitrary values for the given bucket. float64
func (c *client) GaugeFloat64(stat string, value, rate float64) error {
	return c.send(stat, rate, strconv.FormatFloat(value, 'f', -1, 64)+"|g")
}

// Record arbitrary values for the given bucket. int64
func (c *client) GaugeInt64(stat string, value int64, rate float64) error {
	return c.send(stat, rate, strconv.FormatInt(value, 10)+"|g")
}

// Flush writes any buffered data to the network.
func (c *client) Flush() error {
	return c.buf.Flush()
}

// Closes the connection.
func (c *client) Close() error {
	if err := c.Flush(); err != nil {
		return err
	}
	c.buf = nil
	return c.conn.Close()
}

func (c *client) send(stat string, rate float64, format string, args ...interface{}) error {
	if rate < 1 {
		if rand.Float64() < rate {
			format = format + "|@" + strconv.FormatFloat(rate, 'f', -1, 64)
		} else {
			return nil
		}
	}

	format = c.prefix + stat + ":" + format

	c.m.Lock()
	defer c.m.Unlock()

	// Flush data if we have reach the buffer limit
	if c.buf.Available() < len(format) {
		if err := c.Flush(); err != nil {
			return nil
		}
	}

	// Buffer is not empty, start filling it
	if c.buf.Buffered() > 0 {
		format = "\n" + format
	}

	_, err := fmt.Fprintf(c.buf, format, args...)
	return err
}
