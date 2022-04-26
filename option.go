package ctxsarama

import (
	"context"

	"github.com/Shopify/sarama"
)

type ProducerInterceptor interface {
	Before(context.Context, *sarama.ProducerMessage)
	After(context.Context, *sarama.ProducerMessage, error)
}

type ConsumerInterceptor interface {
	Before(context.Context, *sarama.ConsumerMessage)
	After(context.Context, *sarama.ConsumerMessage, error)
}

type config struct {
	producerInterceptors []ProducerInterceptor
	consumerInterceptors []ConsumerInterceptor
}

type Option func(*config)
