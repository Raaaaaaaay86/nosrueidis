package nosrueidis

import "time"

type RueidisConfig struct {
	Endpoints        []string
	User             string
	Password         string
	SelectDB         int
	ExecutionTimeout time.Duration
}
