package nosrueidis

import (
	"time"

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
