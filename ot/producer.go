package ot

import (
	"context"
	"strconv"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/j2gg0s/ctxsarama"
	"go.opentelemetry.io/contrib/instrumentation/github.com/Shopify/sarama/otelsarama"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"go.opentelemetry.io/otel/trace"
)

func StartProducerSpan(ctx context.Context, msg *sarama.ProducerMessage) trace.Span {
	// Create a span.
	attrs := []attribute.KeyValue{
		semconv.MessagingSystemKey.String("kafka"),
		semconv.MessagingDestinationKindTopic,
		semconv.MessagingDestinationKey.String(msg.Topic),
	}
	opts := []trace.SpanStartOption{
		trace.WithAttributes(attrs...),
		trace.WithSpanKind(trace.SpanKindProducer),
	}
	ctx, span := otel.Tracer(
		"github.com/j2gg0s/ctxsarama/otel").Start(ctx, "kafka.produce", opts...)

	// Inject current span context, so consumers can use it to propagate span.
	otel.GetTextMapPropagator().Inject(ctx, otelsarama.NewProducerMessageCarrier(msg))

	return span
}

func FinishProducerSpan(span trace.Span, msg *sarama.ProducerMessage, err error) {
	span.SetAttributes(
		semconv.MessagingMessageIDKey.String(strconv.FormatInt(msg.Offset, 10)),
		semconv.MessagingKafkaPartitionKey.Int64(int64(msg.Partition)),
	)
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
	}
	span.End()
}

type ProducerInterceptor struct {
	sync.Mutex
	contexts        map[interface{}]messageContext
	returnSuccesses bool
}

func (p *ProducerInterceptor) Before(ctx context.Context, msg *sarama.ProducerMessage) {
	span := StartProducerSpan(ctx, msg)
	mc := messageContext{
		span:   span,
		backup: msg.Metadata,
	}
	msg.Metadata = span.SpanContext().SpanID()
	if p.returnSuccesses {
		p.Lock()
		p.contexts[msg.Metadata] = mc
		p.Unlock()
	}
}

func (p *ProducerInterceptor) After(ctx context.Context, msg *sarama.ProducerMessage, err error) {
	p.Lock()
	if mc, ok := p.contexts[msg.Metadata]; ok {
		delete(p.contexts, msg.Metadata)
		FinishProducerSpan(mc.span, msg, err)
		msg.Metadata = mc.backup
	}
	p.Unlock()
}

type messageContext struct {
	span   trace.Span
	backup interface{}
}

func NewProducerInterceptor() ctxsarama.ProducerInterceptor {
	return &ProducerInterceptor{
		Mutex:    sync.Mutex{},
		contexts: make(map[interface{}]messageContext),
	}
}
