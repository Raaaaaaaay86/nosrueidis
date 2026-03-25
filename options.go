package nosrueidis

import (
	"log/slog"
	"time"

	"github.com/redis/rueidis"
)


type ExecutionOption func(options *ExecutionConfig) error

func WithClientSideTtl(d time.Duration) ExecutionOption {
	return func(options *ExecutionConfig) error {
		options.ClientSideTtl = d
		return nil
	}
}

func WithDebugLogging(key string) ExecutionOption {
	return func(options *ExecutionConfig) error {
		options.ResultLogging = func(result rueidis.RedisResult) {
			defer func() {
				if r := recover(); r != nil {
					slog.Error("panic in redis result logging", "error", r)
				}
			}()

			slog.Debug("redis command result", "key", key, "client_side", result.IsCacheHit())
		}
		return nil
	}
}
