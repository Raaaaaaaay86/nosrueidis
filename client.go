package nosrueidis

import (
	"context"
	"time"

	"github.com/redis/rueidis"
	"github.com/redis/rueidis/rueidislock"
	"golang.org/x/xerrors"
)

type CompletedCommand interface {
	Build() rueidis.Completed
}

type CacheableCommand interface {
	Build() rueidis.Completed
	Cache() rueidis.Cacheable
}

type ExecutionConfig struct {
	ClientSideTtl time.Duration
	ResultLogging func(rueidis.RedisResult)
}

type RueidisClient struct {
	client           rueidis.Client
	lock             rueidislock.Locker
	executionTimeout time.Duration
}

func NewRueidisClient(c RueidisConfig) (*RueidisClient, error) {
	var v RueidisClient

	v.executionTimeout = c.ExecutionTimeout

	client, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress: c.Endpoints,
		Username:    c.User,
		Password:    c.Password,
		SelectDB:    c.SelectDB,
		ShuffleInit: true,
	})
	if err != nil {
		return nil, err
	} else {
		v.client = client
	}

	locker, err := rueidislock.NewLocker(rueidislock.LockerOption{
		ClientOption: rueidis.ClientOption{
			InitAddress: c.Endpoints,
			Username:    c.User,
			Password:    c.Password,
			SelectDB:    c.SelectDB,
		},
		NoLoopTracking: true,
		KeyMajority:    1,
	})
	if err != nil {
		return nil, err
	} else {
		v.lock = locker
	}

	return &v, nil
}

func (r *RueidisClient) GetClient() rueidis.Client {
	return r.client
}

func (r *RueidisClient) Session(ctx context.Context) (context.Context, context.CancelFunc, rueidis.Client) {
	ctx, cancel := context.WithTimeout(ctx, r.executionTimeout)
	return ctx, cancel, r.client
}

func (r *RueidisClient) GetLock() rueidislock.Locker {
	return r.lock
}

func (r *RueidisClient) ExecuteCacheable(ctx context.Context, cmd CacheableCommand, options ...ExecutionOption) (rueidis.RedisResult, error) {
	var config ExecutionConfig
	for _, option := range options {
		if err := option(&config); err != nil {
			return rueidis.RedisResult{}, xerrors.Errorf("cannot apply redis command finalization option: %w", err)
		}
	}

	if config.ClientSideTtl.Seconds() > 0 {
		result := r.client.DoCache(ctx, cmd.Cache(), config.ClientSideTtl)
		if config.ResultLogging != nil {
			defer config.ResultLogging(result)
		}
		return result, nil
	}

	result := r.client.Do(ctx, cmd.Build())
	if config.ResultLogging != nil {
		defer config.ResultLogging(result)
	}

	return result, nil
}

func (r *RueidisClient) Execute(ctx context.Context, cmd CompletedCommand, options ...ExecutionOption) (rueidis.RedisResult, error) {
	var config ExecutionConfig
	for _, option := range options {
		if err := option(&config); err != nil {
			return rueidis.RedisResult{}, xerrors.Errorf("cannot apply redis command finalization option: %w", err)
		}
	}

	result := r.client.Do(ctx, cmd.Build())
	if config.ResultLogging != nil {
		defer config.ResultLogging(result)
	}

	return result, nil
}
