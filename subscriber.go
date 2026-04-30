package nosrueidis

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/raaaaaaaay86/nospubsub"
	"github.com/redis/rueidis"
	"go.opentelemetry.io/otel/trace"
)

const subscriberTracerName = "nosrueidis"

var _ nospubsub.Subscriber = (*Subscriber)(nil)

type Subscriber struct {
	name           string
	identifier     nospubsub.Identifier
	signalChan     chan<- nospubsub.Signal
	tracerProvider trace.TracerProvider
	config         SubscriberConfig
	client         RueidisClient
	ctx            context.Context
	cancel         context.CancelFunc
	wg             sync.WaitGroup
	started        atomic.Bool
	closed         atomic.Bool
}

func NewSubscriber(client RueidisClient, config SubscriberConfig) *Subscriber {
	s := &Subscriber{
		client: client,
		config: config,
	}
	if len(config.Handlers) > 0 {
		if name, ok := getSubscriberHandlerName(config.Handlers[len(config.Handlers)-1]); ok {
			s.name = name
		}
	}
	return s
}

func (s *Subscriber) Start(ctx context.Context) error {
	if s.started.Swap(true) {
		return nil
	}
	s.closed.Store(false)

	s.ctx, s.cancel = context.WithCancel(ctx)
	s.wg.Add(1)
	go s.run()

	return nil
}

func (s *Subscriber) run() {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("subscriber panic", "identifier", s.identifier, "recover", r, "channels", s.config.Channels, "patterns", s.config.Patterns)
		} else {
			slog.Info("subscriber exited", "identifier", s.identifier, "channels", s.config.Channels, "patterns", s.config.Patterns)
		}
	}()
	defer s.wg.Done()

	rc := s.client.GetClient()
	var wg sync.WaitGroup

	if len(s.config.Channels) > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cmd := rc.B().Subscribe().Channel(s.config.Channels...).Build()
			if err := rc.Receive(s.ctx, cmd, s.processMessage); err != nil && s.ctx.Err() == nil {
				slog.Error("channel subscribe error", "identifier", s.identifier, "channels", s.config.Channels, "error", err)
				s.sendSignal(nospubsub.ErrorSignalLevel, "channel receive error", err)
			}
		}()
	}

	if len(s.config.Patterns) > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cmd := rc.B().Psubscribe().Pattern(s.config.Patterns...).Build()
			if err := rc.Receive(s.ctx, cmd, s.processMessage); err != nil && s.ctx.Err() == nil {
				slog.Error("pattern subscribe error", "identifier", s.identifier, "patterns", s.config.Patterns, "error", err)
				s.sendSignal(nospubsub.ErrorSignalLevel, "pattern receive error", err)
			}
		}()
	}

	wg.Wait()
}

func (s *Subscriber) processMessage(msg rueidis.PubSubMessage) {
	m := &nospubsub.Message{
		Pattern: msg.Pattern,
		Channel: msg.Channel,
		Message: msg.Message,
	}

	ctx := context.Background()

	if s.tracerProvider != nil {
		tctx, span := s.withTracedContext(ctx, msg)
		defer span.End()
		ctx = tctx
	}

	if s.config.ProcessTimeout > 0 {
		pctx, cancel := context.WithTimeout(ctx, s.config.ProcessTimeout)
		defer cancel()
		ctx = pctx
	}

	nctx := nospubsub.NewContext(ctx, s.config.Handlers)
	nctx.SetMessage(m)
	nctx.Next()

	if nctx.GetError() != nil {
		s.sendSignal(nospubsub.ErrorSignalLevel, "message processing error", nctx.GetError())
	}
}

func (s *Subscriber) withTracedContext(ctx context.Context, msg rueidis.PubSubMessage) (context.Context, trace.Span) {
	channel := msg.Channel
	if channel == "" {
		channel = msg.Pattern
	}
	return s.tracerProvider.Tracer(subscriberTracerName).Start(ctx, fmt.Sprintf("pubsub.%s", channel))
}

func (s *Subscriber) sendSignal(level nospubsub.SignalLevel, message string, err error) {
	if s.signalChan != nil {
		s.signalChan <- nospubsub.Signal{
			Name:       s.name,
			Level:      level,
			Identifier: s.identifier,
			Message:    message,
			Error:      err,
		}
	}
}

func (s *Subscriber) SetIdentifier(identifier nospubsub.Identifier) {
	s.identifier = identifier
}

func (s *Subscriber) GetIdentifier() nospubsub.Identifier {
	return s.identifier
}

func (s *Subscriber) SetSignalChan(ch chan<- nospubsub.Signal) {
	s.signalChan = ch
}

func (s *Subscriber) SetTracerProvider(tracerProvider trace.TracerProvider) {
	s.tracerProvider = tracerProvider
}

func (s *Subscriber) Close() error {
	if s.closed.Swap(true) {
		return nil
	}
	if s.cancel != nil {
		s.cancel()
	}
	s.wg.Wait()
	s.started.Store(false)
	return nil
}

func getSubscriberHandlerName[T nospubsub.HandlerFunc | nospubsub.BatchHandlerFunc](handler T) (string, bool) {
	ptr := reflect.ValueOf(handler).Pointer()
	handlerName := runtime.FuncForPC(ptr).Name()
	base := filepath.Base(handlerName)
	namespaces := strings.Split(base, ".")
	if len(namespaces) == 0 {
		return "", false
	}
	element := namespaces[len(namespaces)-1]
	splitted := strings.Split(element, "-")
	if len(splitted) == 0 {
		return "", false
	}
	return splitted[0], true
}
