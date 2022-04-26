package ctxsarama

import (
	"context"

	"github.com/Shopify/sarama"
)

type ConsumerGroupHandler interface {
	Setup(sarama.ConsumerGroupSession) error
	Cleanup(sarama.ConsumerGroupSession) error

	ConmsumeClaim(sarama.ConsumerGroupSession, ConsumerGroupClaim) error
}

type ConsumerGroupClaim interface {
	Topic() string
	Partition() int32
	InitialOffset() int64
	HighWaterMarkOffset() int64

	Messages() <-chan *ConsumerMessage
}

type ConsumerMessage struct {
	*sarama.ConsumerMessage
	Context context.Context
}

type consumerGroupHandler struct {
	handler ConsumerGroupHandler
	claim   *consumerGroupClaim

	cfg *config
}

func (h *consumerGroupHandler) Setup(sess sarama.ConsumerGroupSession) error {
	return h.handler.Setup(sess)
}

func (h *consumerGroupHandler) Cleanup(sess sarama.ConsumerGroupSession) error {
	return h.handler.Cleanup(sess)
}

func (h *consumerGroupHandler) ConsumeClaim(
	sess sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim,
) error {
	h.claim = &consumerGroupClaim{
		claim:    claim,
		messages: make(chan *ConsumerMessage),
	}
	for msg := range claim.Messages() {
		wrappedMsg := &ConsumerMessage{
			ConsumerMessage: msg,
			Context:         context.Background(),
		}
		for _, interceptor := range h.cfg.consumerInterceptors {
			interceptor.Before(wrappedMsg.Context, wrappedMsg.ConsumerMessage)
		}
		h.claim.messages <- wrappedMsg
	}
	return nil
}

type consumerGroupClaim struct {
	claim    sarama.ConsumerGroupClaim
	messages chan *ConsumerMessage
}

func (c *consumerGroupClaim) Topic() string                     { return c.claim.Topic() }
func (c *consumerGroupClaim) Partition() int32                  { return c.claim.Partition() }
func (c *consumerGroupClaim) InitialOffset() int64              { return c.claim.InitialOffset() }
func (c *consumerGroupClaim) HighWaterMarkOffset() int64        { return c.claim.HighWaterMarkOffset() }
func (c *consumerGroupClaim) Messages() <-chan *ConsumerMessage { return c.messages }

func WrapConsumerGroupHandler(h ConsumerGroupHandler, opts ...Option) sarama.ConsumerGroupHandler {
	handler := &consumerGroupHandler{
		handler: h,
		cfg:     &config{},
	}
	for _, opt := range opts {
		opt(handler.cfg)
	}
	return handler
}
