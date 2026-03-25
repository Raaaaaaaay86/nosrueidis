package nosrueidis

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
)

type RueidisClientTestSuite struct {
	suite.Suite
	config RueidisConfig
}

func (s *RueidisClientTestSuite) SetupSuite() {
	s.config = RueidisConfig{
		Endpoints: []string{
			"localhost:7001",
			"localhost:7002",
			"localhost:7003",
			"localhost:7004",
			"localhost:7005",
			"localhost:7006",
		},
		ExecutionTimeout: 5 * time.Second,
	}
}

func TestRueidisClientTestSuite(t *testing.T) {
	suite.Run(t, new(RueidisClientTestSuite))
}

// TestConstructor checks whether RueidisClient's private field of `client`, `locker` and `executionTimeout` has been initialized correctly
func (s *RueidisClientTestSuite) TestConstructor() {
	client, err := NewRueidisClient(s.config)
	s.NoError(err)
	s.NotNil(client)
	s.NotNil(client.GetClient())
	s.NotNil(client.GetLock())
	s.Equal(s.config.ExecutionTimeout, client.executionTimeout)
}

// TestGetClient checks can acquire rueidis.Client through GetClient() method and use PING command to make sure connection is established
func (s *RueidisClientTestSuite) TestGetClient() {
	rc, err := NewRueidisClient(s.config)
	s.NoError(err)

	client := rc.GetClient()
	s.NotNil(client)

	ctx := context.Background()
	err = client.Do(ctx, client.B().Ping().Build()).Error()
	s.NoError(err)
}

// TestSession checks can acquire rueidis.Client through Session with a context.Context which able to timeout
func (s *RueidisClientTestSuite) TestSession() {
	timeout := 2 * time.Second
	conf := s.config
	conf.ExecutionTimeout = timeout

	s.Run("when session no timeout", func() {
		rc, err := NewRueidisClient(conf)
		s.NoError(err)

		ctx, cancel, client := rc.Session(context.Background())
		defer cancel()

		s.NotNil(client)
		deadline, ok := ctx.Deadline()
		s.True(ok)
		s.WithinDuration(time.Now().Add(timeout), deadline, 100*time.Millisecond, "normal session should complete the execution within the context timeout duration")

		err = client.Do(ctx, client.B().Ping().Build()).Error()
		s.NoError(err)
	})

	s.Run("when session timeout", func() {
		// set the timeout in extreme short time.Duration to let later execution become not executable
		conf.ExecutionTimeout = 1 * time.Microsecond
		rcShort, _ := NewRueidisClient(conf)
		ctxShort, cancelShort, clientShort := rcShort.Session(context.Background())
		defer cancelShort()

		// wait more longer to make sure the context.Context timeout
		time.Sleep(10 * time.Millisecond)
		err := clientShort.Do(ctxShort, clientShort.B().Ping().Build()).Error()
		s.Error(err)
		s.Contains(err.Error(), context.DeadlineExceeded.Error())
	})
}

// TestGetLock checks can acquire rueidislock.Locker through GetLock() method
func (s *RueidisClientTestSuite) TestGetLock() {
	rc, err := NewRueidisClient(s.config)
	s.NoError(err)

	locker := rc.GetLock()
	s.NotNil(locker)

	client := locker.Client()
	s.NoError(client.Do(context.TODO(), client.B().Ping().Build()).Error())
}

// TestLock checks the behaviour of getting lock between different goroutines is correctly
func (s *RueidisClientTestSuite) TestLock() {
	rc1, err := NewRueidisClient(s.config)
	s.NoError(err)
	rc2, err := NewRueidisClient(s.config)
	s.NoError(err)

	lockKey := "test-lock-key"
	ctx := context.Background()

	// 1. First goroutine acquires the lock
	locker1 := rc1.GetLock()
	lockedCtx1, cancel1, err := locker1.WithContext(ctx, lockKey)
	s.NoError(err)
	s.NotNil(lockedCtx1)
	s.NotNil(cancel1)

	// 2. Second goroutine tries to acquire the same lock with timeout
	locker2 := rc2.GetLock()
	shortCtx, cancelShort := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancelShort()

	_, _, err = locker2.WithContext(shortCtx, lockKey)
	s.Error(err, "Second locker should fail to acquire the lock held by the first one")
	s.True(errors.Is(err, context.DeadlineExceeded), "Error should be context.DeadlineExceeded")

	// 3. First goroutine releases the lock
	cancel1()

	// 4. Second goroutine should now be able to acquire the lock
	lockedCtx2, cancel2, err := locker2.WithContext(ctx, lockKey)
	s.NoError(err, "Second locker should be able to acquire the lock after it's released")
	s.NotNil(lockedCtx2)
	s.NotNil(cancel2)
	cancel2()
}
