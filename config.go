package nosrueidis

import (
	"time"

	"github.com/raaaaaaaay86/nospubsub"
	"go.opentelemetry.io/otel/trace"
)

type RueidisConfig struct {
	Endpoints        []string
	User             string
	Password         string
	SelectDB         int
	ExecutionTimeout time.Duration
	TracerProvider   trace.TracerProvider
}

type SubscriberConfig struct {
	Channels       []string
	Patterns       []string
	Handlers       []nospubsub.HandlerFunc
	BatchHandlers  []nospubsub.BatchHandlerFunc
	ProcessTimeout time.Duration
	BatchSize      int
	BatchTimeout   time.Duration
}

func (c SubscriberConfig) GetBatchSize() int {
	if c.BatchSize <= 0 {
		return 1
	}
	return c.BatchSize
}

func (c SubscriberConfig) GetBatchTimeout() time.Duration {
	if c.BatchTimeout <= 0 {
		return 5 * time.Second
	}
	return c.BatchTimeout
}
