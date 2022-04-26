package ctxsarama

import (
	"context"

	"github.com/Shopify/sarama"
)

type SyncProducer struct {
	sarama.SyncProducer
	saramaConfig *sarama.Config
	cfg          *config
}

func WrapSyncProducer(saramaConfig *sarama.Config, p sarama.SyncProducer, opts ...Option) *SyncProducer {
	cfg := &config{}
	for _, opt := range opts {
		opt(cfg)
	}

	return &SyncProducer{
		SyncProducer: p,
		saramaConfig: saramaConfig,
		cfg:          cfg,
	}
}

func (p *SyncProducer) SendMessage(ctx context.Context, msg *sarama.ProducerMessage) (int32, int64, error) {
	for _, interceptor := range p.cfg.producerInterceptors {
		interceptor.Before(ctx, msg)
	}
	partition, offset, err := p.SyncProducer.SendMessage(msg)
	for _, interceptor := range p.cfg.producerInterceptors {
		interceptor.After(ctx, msg, err)
	}
	return partition, offset, err
}

func (p *SyncProducer) SendMessages(ctx context.Context, msgs []*sarama.ProducerMessage) error {
	for _, interceptor := range p.cfg.producerInterceptors {
		for _, msg := range msgs {
			interceptor.Before(ctx, msg)
		}
	}
	err := p.SyncProducer.SendMessages(msgs)
	for _, interceptor := range p.cfg.producerInterceptors {
		for _, msg := range msgs {
			interceptor.After(ctx, msg, err)
		}
	}
	return err
}

func WrapAsyncProducer(saramaConfig *sarama.Config, p sarama.AsyncProducer, opts ...Option) *AsyncProducer {
	cfg := &config{}
	for _, opt := range opts {
		opt(cfg)
	}

	wrapped := &AsyncProducer{
		AsyncProducer: p,

		input:         make(chan *ProducerMessage),
		closeErr:      make(chan error),
		closeSig:      make(chan struct{}),
		closeAsyncSig: make(chan struct{}),

		cfg: cfg,
	}

	go func() {
		for {
			select {
			case <-wrapped.closeSig:
				wrapped.closeErr <- p.Close()
				return
			case <-wrapped.closeAsyncSig:
				p.AsyncClose()
				return
			case msg, ok := <-wrapped.input:
				if !ok {
					continue
				}
				if msg.ProducerMessage.Metadata == nil {
					msg.ProducerMessage.Metadata = msg.Context
				}
				for _, interceptor := range wrapped.cfg.producerInterceptors {
					interceptor.Before(msg.Context, msg.ProducerMessage)
				}
				p.Input() <- msg.ProducerMessage
			}
		}
	}()

	go func() {
		for msg := range p.Successes() {
			wrappedMsg := &ProducerMessage{
				ProducerMessage: msg,
			}
			if ctx, ok := msg.Metadata.(context.Context); ok {
				wrappedMsg.Context = ctx
			}
			for _, interceptor := range wrapped.cfg.producerInterceptors {
				interceptor.After(wrappedMsg.Context, msg, nil)
			}
		}
	}()

	go func() {
		for errMsg := range p.Errors() {
			wrappedMsg := &ProducerError{
				Msg: errMsg.Msg,
				Err: errMsg.Err,
			}
			if ctx, ok := errMsg.Msg.Metadata.(context.Context); ok {
				wrappedMsg.Context = ctx
			}
			for _, interceptor := range wrapped.cfg.producerInterceptors {
				interceptor.After(wrappedMsg.Context, errMsg.Msg, errMsg.Err)
			}
		}
	}()

	return wrapped
}

type AsyncProducer struct {
	sarama.AsyncProducer

	input         chan *ProducerMessage
	closeErr      chan error
	closeSig      chan struct{}
	closeAsyncSig chan struct{}

	cfg *config
}

func (p *AsyncProducer) Input() chan<- *ProducerMessage { return p.input }

func (p *AsyncProducer) AsyncClose() {
	close(p.input)
	close(p.closeAsyncSig)
}

func (p *AsyncProducer) Close() error {
	close(p.input)
	close(p.closeSig)
	return <-p.closeErr
}

type ProducerMessage struct {
	*sarama.ProducerMessage
	Context context.Context
}

type ProducerError struct {
	Msg     *sarama.ProducerMessage
	Err     error
	Context context.Context
}
