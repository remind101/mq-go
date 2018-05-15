package mq_test

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"

	"github.com/aws/aws-sdk-go/service/sqs"
	mq "github.com/remind101/mq-go"
	"github.com/remind101/mq-go/pkg/memsqs"
	"github.com/stretchr/testify/assert"
)

func TestPublisherFlushesOnShutdown(t *testing.T) {
	qURL := "jobs"
	c := memsqs.New()

	p := newPublisher(t, qURL, c)
	p.Start()
	p.Publish(&sqs.SendMessageBatchRequestEntry{
		MessageBody: aws.String("job1"),
	})
	shutdownPublisher(t, p) // Wait for server to shutdown
	assert.Equal(t, 1, len(c.Queue(qURL)))
}

func TestPublisherSendsWhenBatchMaxReached(t *testing.T) {
	qURL := "jobs"
	c := memsqs.New()

	p := newPublisher(t, qURL, c)
	p.Start()
	defer shutdownPublisher(t, p)

	// Push enough messages into the queue to trigger a batch send call.
	for i := 0; i < mq.DefaultMaxNumberOfMessages; i++ {
		p.Publish(&sqs.SendMessageBatchRequestEntry{
			MessageBody: aws.String(fmt.Sprintf("job%d", i)),
		})
	}

	// Assert queue receives messages before mq.DefaultPublishInterval is reached.
	eventually(t, 500*time.Millisecond, func() bool {
		return len(c.Queue(qURL)) == mq.DefaultMaxNumberOfMessages
	}, "expected messages to be batch sent")
}

func TestPublisherSendsWhenIntervalIsReached(t *testing.T) {
	qURL := "jobs"
	c := memsqs.New()

	p := newPublisher(t, qURL, c)
	p.PublishInterval = 10 * time.Millisecond
	p.Start()
	defer shutdownPublisher(t, p)

	p.Publish(&sqs.SendMessageBatchRequestEntry{
		MessageBody: aws.String("job1"),
	})

	p.Publish(&sqs.SendMessageBatchRequestEntry{
		MessageBody: aws.String("job1"),
	})

	// Assert message is in queue after at least 2 intervals.
	eventually(t, 20*time.Millisecond, func() bool {
		return len(c.Queue(qURL)) == 2
	}, "expected messages to be batch sent after the PublishInterval")
}

func newPublisher(t *testing.T, qURL string, c sqsiface.SQSAPI) *mq.Publisher {
	return mq.NewPublisher(qURL, func(p *mq.Publisher) {
		p.Client = c
		p.OutputHandler = func(out *sqs.SendMessageBatchOutput, err error) {
			if err != nil {
				t.Fatal(err)
			}
		}
		if os.Getenv("DEBUG") == "true" {
			p.Logger = log.New(os.Stderr, "debug - ", 0)
		}
	})
}

func shutdownPublisher(t *testing.T, s *mq.Publisher) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	assert.NoError(t, s.Shutdown(ctx))
}
