package nosrueidis

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/raaaaaaaay86/nospubsub"
	"github.com/redis/rueidis"
	"go.opentelemetry.io/otel/trace"
)

var _ nospubsub.Subscriber = (*BatchSubscriber)(nil)

type BatchSubscriber struct {
	Subscriber
}

func NewBatchSubscriber(client RueidisClient, config SubscriberConfig) *BatchSubscriber {
	s := &BatchSubscriber{
		Subscriber: Subscriber{
			client: client,
			config: config,
		},
	}
	if len(config.BatchHandlers) > 0 {
		if name, ok := getHandlerName[nospubsub.BatchHandlerFunc](config.BatchHandlers[len(config.BatchHandlers)-1]); ok {
			s.name = name
		}
	}
	return s
}

func (b *BatchSubscriber) Start(ctx context.Context) error {
	if b.started.Swap(true) {
		return nil
	}
	b.closed.Store(false)

	b.ctx, b.cancel = context.WithCancel(ctx)
	b.wg.Add(1)
	go b.runBatch()

	return nil
}

func (b *BatchSubscriber) runBatch() {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("batch subscriber panic", "identifier", b.identifier, "recover", r, "channels", b.config.Channels, "patterns", b.config.Patterns)
		} else {
			slog.Info("batch subscriber exited", "identifier", b.identifier, "channels", b.config.Channels, "patterns", b.config.Patterns)
		}
	}()
	defer b.wg.Done()

	rc := b.client.GetClient()
	batchSize := b.config.GetBatchSize()
	batchTimeout := b.config.GetBatchTimeout()

	msgChan := make(chan *nospubsub.Message, batchSize*2)

	var receiveWg sync.WaitGroup

	if len(b.config.Channels) > 0 {
		receiveWg.Add(1)
		go func() {
			defer receiveWg.Done()
			cmd := rc.B().Subscribe().Channel(b.config.Channels...).Build()
			if err := rc.Receive(b.ctx, cmd, func(msg rueidis.PubSubMessage) {
				select {
				case msgChan <- &nospubsub.Message{Pattern: msg.Pattern, Channel: msg.Channel, Message: msg.Message}:
				case <-b.ctx.Done():
				}
			}); err != nil && b.ctx.Err() == nil {
				slog.Error("batch channel subscribe error", "identifier", b.identifier, "channels", b.config.Channels, "error", err)
				b.sendSignal(nospubsub.ErrorSignalLevel, "channel receive error", err)
			}
		}()
	}

	if len(b.config.Patterns) > 0 {
		receiveWg.Add(1)
		go func() {
			defer receiveWg.Done()
			cmd := rc.B().Psubscribe().Pattern(b.config.Patterns...).Build()
			if err := rc.Receive(b.ctx, cmd, func(msg rueidis.PubSubMessage) {
				select {
				case msgChan <- &nospubsub.Message{Pattern: msg.Pattern, Channel: msg.Channel, Message: msg.Message}:
				case <-b.ctx.Done():
				}
			}); err != nil && b.ctx.Err() == nil {
				slog.Error("batch pattern subscribe error", "identifier", b.identifier, "patterns", b.config.Patterns, "error", err)
				b.sendSignal(nospubsub.ErrorSignalLevel, "pattern receive error", err)
			}
		}()
	}

	go func() {
		receiveWg.Wait()
		close(msgChan)
	}()

	buffer := make([]*nospubsub.Message, 0, batchSize)
	ticker := time.NewTicker(batchTimeout)
	defer ticker.Stop()

	flush := func() {
		if len(buffer) == 0 {
			return
		}
		b.processBatch(buffer)
		buffer = buffer[:0]
		ticker.Reset(batchTimeout)
	}

	for {
		select {
		case <-b.ctx.Done():
			flush()
			return
		case <-ticker.C:
			flush()
		case msg, ok := <-msgChan:
			if !ok {
				flush()
				return
			}
			buffer = append(buffer, msg)
			if len(buffer) >= batchSize {
				flush()
			}
		}
	}
}

func (b *BatchSubscriber) processBatch(messages []*nospubsub.Message) {
	ctx := context.Background()

	if b.tracerProvider != nil {
		tctx, span := b.withBatchTracedContext(ctx, messages)
		defer span.End()
		ctx = tctx
	}

	if b.config.ProcessTimeout > 0 {
		pctx, cancel := context.WithTimeout(ctx, b.config.ProcessTimeout)
		defer cancel()
		ctx = pctx
	}

	bctx := nospubsub.NewBatchContext(ctx, b.config.BatchHandlers)
	bctx.SetMessages(messages)
	bctx.Next()

	if bctx.GetError() != nil {
		b.sendSignal(nospubsub.ErrorSignalLevel, "batch processing error", bctx.GetError())
	}
}

func (b *BatchSubscriber) withBatchTracedContext(ctx context.Context, messages []*nospubsub.Message) (context.Context, trace.Span) {
	channel := ""
	if len(messages) > 0 {
		channel = messages[0].Channel
		if channel == "" {
			channel = messages[0].Pattern
		}
	}
	return b.tracerProvider.Tracer(subscriberTracerName).Start(ctx, fmt.Sprintf("pubsub.batch.%s", channel))
}
