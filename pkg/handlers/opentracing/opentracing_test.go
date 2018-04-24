package opentracing_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"github.com/opentracing/opentracing-go/mocktracer"
	mq "github.com/remind101/mq-go"
	mq_opentracing "github.com/remind101/mq-go/pkg/handlers/opentracing"
)

func TestCarrier(t *testing.T) {
	tracer := mocktracer.New()
	opentracing.SetGlobalTracer(tracer)

	span := opentracing.StartSpan("root")
	m := &sqs.Message{
		MessageAttributes: map[string]*sqs.MessageAttributeValue{},
	}

	// Inject span into message
	err := opentracing.GlobalTracer().Inject(
		span.Context(),
		opentracing.TextMap,
		mq_opentracing.SQSMessageAttributeCarrier(m.MessageAttributes),
	)
	require.NoError(t, err)

	// Extract span out of message
	wireContext, err := opentracing.GlobalTracer().Extract(
		opentracing.TextMap,
		mq_opentracing.SQSMessageAttributeCarrier(m.MessageAttributes),
	)
	require.NoError(t, err)

	childSpan := opentracing.StartSpan("child-span", ext.RPCServerOption(wireContext))
	childSpanParentID := childSpan.(*mocktracer.MockSpan).ParentID
	rootSpanID := span.(*mocktracer.MockSpan).SpanContext.SpanID
	require.Equal(t, rootSpanID, childSpanParentID)
}

func TestHandler(t *testing.T) {
	tracer := mocktracer.New()
	opentracing.SetGlobalTracer(tracer)

	span := opentracing.StartSpan("root")

	m := mq.NewMessage("http://sqs.example.com/myqueue", &sqs.Message{
		MessageAttributes: map[string]*sqs.MessageAttributeValue{},
		Body:              aws.String("hello"),
	}, nil)

	err := mq_opentracing.InjectSpan(span, m.SQSMessage)
	require.NoError(t, err)

	h := mq_opentracing.Middleware{
		Handler: mq.HandlerFunc(func(*mq.Message) error {
			return nil
		}),
		Tagger: func(span opentracing.Span, err error) {
			mq_opentracing.DefaultTagger(span, err)
			span.SetTag("foo", "bar")
		},
	}

	require.NoError(t, h.HandleMessage(m))
	require.Equal(t, 1, len(tracer.FinishedSpans()))

	handlerSpan := tracer.FinishedSpans()[0]
	assert.Equal(t, "sqs.message", handlerSpan.OperationName)
	assert.Equal(t, "bar", handlerSpan.Tags()["foo"].(string))

	assert.Equal(t, span.(*mocktracer.MockSpan).SpanContext.SpanID, handlerSpan.ParentID)
}
