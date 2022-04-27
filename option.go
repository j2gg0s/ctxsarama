package ctxsarama

import (
	"context"

	"github.com/Shopify/sarama"
)

type ProducerInterceptor interface {
	Before(context.Context, *sarama.ProducerMessage)
	After(context.Context, *sarama.ProducerMessage, error)
}

type ProducerInterceptorFuncs struct {
	BeforeFn func(context.Context, *sarama.ProducerMessage)
	AfterFn  func(context.Context, *sarama.ProducerMessage, error)
}

var _ ProducerInterceptor = (*ProducerInterceptorFuncs)(nil)

func (p *ProducerInterceptorFuncs) Before(ctx context.Context, msg *sarama.ProducerMessage) {
	if p.BeforeFn != nil {
		p.BeforeFn(ctx, msg)
	}
}

func (p *ProducerInterceptorFuncs) After(ctx context.Context, msg *sarama.ProducerMessage, err error) {
	if p.AfterFn != nil {
		p.AfterFn(ctx, msg, err)
	}
}

type ConsumerHandler func(*ConsumerMessage)
type ConsumerInterceptor func(*ConsumerMessage, ConsumerHandler)

func getChainConsumerInterceptor(cfg *config) ConsumerInterceptor {
	interceptors := cfg.consumerInterceptors
	switch len(interceptors) {
	case 0:
		return nil
	case 1:
		return interceptors[0]
	default:
		return func(msg *ConsumerMessage, handler ConsumerHandler) {
			interceptors[0](msg, getChainHandler(interceptors, 0, handler))
		}
	}
}

func getChainHandler(interceptors []ConsumerInterceptor, curr int, finalHandler ConsumerHandler) ConsumerHandler {
	if curr+1 == len(interceptors) {
		return finalHandler
	}
	return func(msg *ConsumerMessage) {
		interceptors[curr+1](msg, getChainHandler(interceptors, curr+1, finalHandler))
	}
}

type config struct {
	producerInterceptors []ProducerInterceptor
	consumerInterceptors []ConsumerInterceptor
}

type Option func(*config)

func WithProducerInterceptors(interceptors ...ProducerInterceptor) Option {
	return func(cfg *config) {
		cfg.producerInterceptors = append(cfg.producerInterceptors, interceptors...)
	}
}

func WithConsumerInterceptors(interceptors ...ConsumerInterceptor) Option {
	return func(cfg *config) {
		cfg.consumerInterceptors = append(cfg.consumerInterceptors, interceptors...)
	}
}
