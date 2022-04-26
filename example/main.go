package main

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/Shopify/sarama"
	"github.com/spf13/cobra"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"github.com/j2gg0s/ctxsarama"
	"github.com/j2gg0s/ctxsarama/ot"
)

func main() {
	InitTracer()

	cmd := &cobra.Command{
		Use: "ctxsarama",
	}
	cmd.PersistentFlags().StringSliceVar(&brokers, "brokers", brokers, "Kafka brokers")
	cmd.PersistentFlags().StringVar(&topic, "topic", topic, "Produce&Consume topic")

	cmd.AddCommand(
		&cobra.Command{
			Use: "consume",
			RunE: func(*cobra.Command, []string) error {
				return consume(brokers, topic)
			},
		},
		&cobra.Command{
			Use: "produce",
			RunE: func(*cobra.Command, []string) error {
				return produce(brokers, topic)
			},
		},
	)

	if err := cmd.Execute(); err != nil {
		panic(err)
	}
}

var (
	brokers []string = []string{"localhost:9092"}
	topic   string   = "example"
)

func produce(brokers []string, topic string) error {
	config := sarama.NewConfig()
	config.Version = sarama.V2_0_0_0
	config.Producer.Return.Successes = true

	producer, err := sarama.NewAsyncProducer(brokers, config)
	if err != nil {
		return fmt.Errorf("failed to start producer: %w", err)
	}

	wrappedProducer := ctxsarama.WrapAsyncProducer(
		config,
		producer,
		ctxsarama.WithProducerInterceptors(ot.NewProducerInterceptor()),
	)

	ticker := time.NewTicker(time.Millisecond * 100)
	defer ticker.Stop()
	for range ticker.C {
		ctx, span := otel.Tracer("example").Start(context.Background(), "before_producer")

		wrappedProducer.Input() <- &ctxsarama.ProducerMessage{
			Context: ctx,
			ProducerMessage: &sarama.ProducerMessage{
				Topic: topic,
				Value: sarama.StringEncoder(strconv.FormatInt(time.Now().UnixMilli(), 10)),
			},
		}
		sc := span.SpanContext()
		fmt.Println("produce", sc.TraceID(), sc.SpanID())

		span.End()
	}
	return nil
}

func consume(brokers []string, topic string) error {
	handler := ctxsarama.WrapConsumerGroupHandler(
		&Consumer{},
		ctxsarama.WithConsumerInterceptors(ot.NewConsumerInterceptor()),
	)

	config := sarama.NewConfig()
	config.Version = sarama.V2_0_0_0
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	consumerGroup, err := sarama.NewConsumerGroup(brokers, "ctxsarama", config)
	if err != nil {
		return fmt.Errorf("start consumerGroup: %w", err)
	}

	err = consumerGroup.Consume(context.Background(), []string{topic}, handler)
	if err != nil {
		return fmt.Errorf("consume: %w", err)
	}

	select {}
}

type Consumer struct {
}

func (c *Consumer) Setup(sarama.ConsumerGroupSession) error {
	fmt.Println("setup")
	return nil
}

func (c *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	fmt.Println("cleanup")
	return nil
}

func (c *Consumer) ConsumeClaim(sess sarama.ConsumerGroupSession, claim ctxsarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		sc := trace.SpanContextFromContext(msg.Context)
		fmt.Println("consume", sc.TraceID(), sc.SpanID())
		sess.MarkMessage(msg.ConsumerMessage, "")
	}
	return nil
}
