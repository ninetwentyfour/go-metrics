package metrics

import (
  "time"
)

func ExampleStatsd() {
  go Statsd(DefaultRegistry, 1*time.Second, "some.prefix", "localhost:8125")
}

func ExampleStatsdWithConfig() {
  go StatsdWithConfig(StatsdConfig{
    Addr:          "localhost:8125",
    Registry:      DefaultRegistry,
    FlushInterval: 1 * time.Second,
    DurationUnit:  time.Millisecond,
  })
}
