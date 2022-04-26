package ot

import (
	"context"
	"strconv"

	"github.com/Shopify/sarama"
	"github.com/j2gg0s/ctxsarama"
	"go.opentelemetry.io/contrib/instrumentation/github.com/Shopify/sarama/otelsarama"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"go.opentelemetry.io/otel/trace"
)

func StartConsumerSpan(ctx context.Context, msg *sarama.ConsumerMessage) (context.Context, trace.Span) {
	carrier := otelsarama.NewConsumerMessageCarrier(msg)
	ctx = otel.GetTextMapPropagator().Extract(ctx, carrier)

	// Create a span.
	attrs := []attribute.KeyValue{
		semconv.MessagingSystemKey.String("kafka"),
		semconv.MessagingDestinationKindTopic,
		semconv.MessagingDestinationKey.String(msg.Topic),
		semconv.MessagingOperationReceive,
		semconv.MessagingMessageIDKey.String(strconv.FormatInt(msg.Offset, 10)),
		semconv.MessagingKafkaPartitionKey.Int64(int64(msg.Partition)),
	}
	opts := []trace.SpanStartOption{
		trace.WithAttributes(attrs...),
		trace.WithSpanKind(trace.SpanKindConsumer),
	}

	return otel.Tracer("github.com/j2gg0s/ctxsarama").Start(ctx, "kafka.consume", opts...)
}

func FinishConsumerSpan(span trace.Span) {
	span.End()
}

func NewConsumerInterceptor() ctxsarama.ConsumerInterceptor {
	return func(msg *ctxsarama.ConsumerMessage, handler ctxsarama.ConsumerHandler) {
		ctx, span := StartConsumerSpan(msg.Context, msg.ConsumerMessage)
		msg.Context = ctx
		handler(msg)
		FinishConsumerSpan(span)
	}
}
