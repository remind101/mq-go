package opentracing

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	mq "github.com/remind101/mq-go"
)

var DefaultSpanName = "sqs.message"
var DefaultTagger = func(span opentracing.Span, err error) {
	if err != nil {
		span.SetTag("error.error", err)
	}
}

type Middleware struct {
	Handler  mq.Handler
	SpanName string
	Tagger   func(opentracing.Span, error)
}

func (m *Middleware) HandleMessage(msg *mq.Message) error {
	tagger := DefaultTagger
	if m.Tagger != nil {
		tagger = m.Tagger
	}

	spanName := DefaultSpanName
	if m.SpanName != "" {
		spanName = m.SpanName
	}

	span := SpanFromMessage(spanName, msg.SQSMessage)
	defer span.Finish()

	ctx := opentracing.ContextWithSpan(msg.Context(), span)
	msg = msg.WithContext(ctx)
	err := m.Handler.HandleMessage(msg)
	tagger(span, err)

	return err
}

type SQSMessageAttributeCarrier map[string]*sqs.MessageAttributeValue

func (c SQSMessageAttributeCarrier) Set(key, val string) {
	c[key] = &sqs.MessageAttributeValue{
		DataType:    aws.String("String"),
		StringValue: aws.String(val),
	}
}

func (c SQSMessageAttributeCarrier) ForeachKey(handler func(key, val string) error) error {
	for k, v := range c {
		if aws.StringValue(v.DataType) == "String" {
			if err := handler(k, aws.StringValue(v.StringValue)); err != nil {
				return err
			}
		}
	}
	return nil
}

func InjectSpan(span opentracing.Span, m *sqs.Message) error {
	return opentracing.GlobalTracer().Inject(
		span.Context(),
		opentracing.TextMap,
		SQSMessageAttributeCarrier(m.MessageAttributes),
	)
}

func SpanFromMessage(spanName string, m *sqs.Message) opentracing.Span {
	span := opentracing.StartSpan(spanName)
	if m.MessageAttributes != nil {
		spanContext, err := opentracing.GlobalTracer().Extract(
			opentracing.TextMap,
			SQSMessageAttributeCarrier(m.MessageAttributes),
		)
		if err == nil {
			span = opentracing.StartSpan(spanName, ext.RPCServerOption(spanContext))
		}
	}
	return span
}
