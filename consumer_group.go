package ctxsarama

import (
	"context"

	"github.com/Shopify/sarama"
)

type ConsumerGroupHandler interface {
	Setup(sarama.ConsumerGroupSession) error
	Cleanup(sarama.ConsumerGroupSession) error

	ConsumeClaim(sarama.ConsumerGroupSession, ConsumerGroupClaim) error
}

type ConsumerGroupClaim interface {
	Topic() string
	Partition() int32
	InitialOffset() int64
	HighWaterMarkOffset() int64

	Messages() <-chan *ConsumerMessage

	PartitionConsumer() (sarama.PartitionConsumer, bool)
}

type ConsumerMessage struct {
	*sarama.ConsumerMessage
	Context context.Context
}

type consumerGroupHandler struct {
	handler ConsumerGroupHandler
	cfg     *config
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
	wrappedClaim := &consumerGroupClaim{
		claim:    claim,
		messages: make(chan *ConsumerMessage),
	}
	go func() {
		for msg := range claim.Messages() {
			wrappedMsg := &ConsumerMessage{
				ConsumerMessage: msg,
				Context:         context.Background(),
			}
			if chainInt := getChainConsumerInterceptor(h.cfg); chainInt != nil {
				chainInt(wrappedMsg, func(msg *ConsumerMessage) {
					wrappedClaim.messages <- wrappedMsg
				})
				continue
			}
			wrappedClaim.messages <- wrappedMsg
		}
	}()
	return h.handler.ConsumeClaim(sess, wrappedClaim)
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

func (c *consumerGroupClaim) PartitionConsumer() (sarama.PartitionConsumer, bool) {
	pc, ok := c.claim.(sarama.PartitionConsumer)
	return pc, ok
}

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
