package mq_test

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	mq "github.com/remind101/mq-go"
	"github.com/remind101/mq-go/pkg/memsqs"
	"github.com/stretchr/testify/assert"
)

func TestServer(t *testing.T) {
	qURL := "jobs"
	done := make(chan struct{})
	c := memsqs.New()

	c.SendMessage(&sqs.SendMessageInput{
		QueueUrl:    aws.String(qURL),
		MessageBody: aws.String(`{"name":"test"}`),
	})

	h := mq.HandlerFunc(func(m *mq.Message) error {
		assert.Equal(t, `{"name":"test"}`, aws.StringValue(m.SQSMessage.Body))
		close(done)
		return nil
	})

	sp := newServer(t, qURL, h, c)
	sp.Start()
	<-done             // Wait for server to handle message
	closeServer(t, sp) // Wait for server to shutdown
	assert.Empty(t, len(c.Queue(qURL)))
}

func TestServerWaitsForProcessingToFinish(t *testing.T) {
	qURL := "jobs"
	done := make(chan struct{})
	c := memsqs.New()

	c.SendMessage(&sqs.SendMessageInput{
		QueueUrl:    aws.String(qURL),
		MessageBody: aws.String(`{"name":"test"}`),
	})

	h := mq.HandlerFunc(func(m *mq.Message) error {
		close(done)
		time.Sleep(10 * time.Millisecond)
		return nil
	})

	sp := newServer(t, qURL, h, c)
	sp.Start()
	<-done             // Wait for server to receive message
	closeServer(t, sp) // Wait for server to shutdown
	assert.Empty(t, len(c.Queue(qURL)))
}

func TestServerDeletesWhenBatchMaxReached(t *testing.T) {
	qURL := "jobs"
	done := make(chan struct{})
	c := memsqs.New()

	// Push enough messages into the queue to trigger a batch delete call.
	for i := 0; i < mq.DefaultBatchDeleteMaxMessages; i++ {
		c.SendMessage(&sqs.SendMessageInput{
			QueueUrl:    aws.String(qURL),
			MessageBody: aws.String(fmt.Sprintf("message %d", i)),
		})
	}

	messagesReceived := 0
	h := mq.HandlerFunc(func(m *mq.Message) error {
		messagesReceived++
		if messagesReceived >= mq.DefaultBatchDeleteMaxMessages {
			close(done)
		}
		return nil
	})

	sp := newServer(t, qURL, h, c)
	sp.Start()
	defer closeServer(t, sp)

	// Wait for server to receive all messages, batch deletion should
	// trigger immediately at this point.
	<-done

	// Assert queue is emptied before mq.DefaultDeletionInterval is reached.
	eventually(t, 1*time.Second, func() bool {
		return len(c.Queue(qURL)) == 0
	}, "expected messages to be batch removed")
}

func TestServerDeletesWhenIntervalIsReached(t *testing.T) {
	qURL := "jobs"
	done := make(chan struct{})
	c := memsqs.New()

	// Push a message into the queue. This will not trigger a batch delete call.
	// until DeletionInterval is reached.
	c.SendMessage(&sqs.SendMessageInput{
		QueueUrl:    aws.String(qURL),
		MessageBody: aws.String("message 1"),
	})

	h := mq.HandlerFunc(func(m *mq.Message) error {
		close(done)
		return nil
	})

	sp := newServer(t, qURL, h, c)
	sp.DeletionInterval = 10 * time.Millisecond
	sp.Start()
	defer closeServer(t, sp)

	// Wait for server to receive the message. Message is now pending deletion.
	<-done

	// Assert message is still in queue.
	assert.Equal(t, 1, len(c.Queue(qURL)))

	// Assert queue is emptied after at least 2 intervals.
	eventually(t, 20*time.Millisecond, func() bool {
		return len(c.Queue(qURL)) == 0
	}, "expected messages to be batch removed after the DeletionInterval")
}

func newServer(t *testing.T, qURL string, h mq.Handler, c sqsiface.SQSAPI) *mq.Server {
	return mq.NewServer(qURL, h, func(s *mq.Server) {
		s.Client = c
		s.ErrorHandler = func(err error) {
			t.Fatal(err)
		}
		if os.Getenv("DEBUG") == "true" {
			s.Logger = log.New(os.Stderr, "server - ", 0)
		}
	})
}

func closeServer(t *testing.T, s *mq.Server) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	assert.NoError(t, s.Shutdown(ctx))
}

func eventually(t *testing.T, d time.Duration, fn func() bool, msg string) {
	timeout := time.After(d)
	ticker := time.NewTicker(1 * time.Millisecond)
	for {
		select {
		case <-timeout:
			t.Error(msg)
			return
		case <-ticker.C:
			if fn() {
				return
			}
		}
	}
}
