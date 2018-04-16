package mq

import (
	"context"
	"fmt"
	"os"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
)

const (
	DefaultConcurrency         = 1
	DefaultMaxNumberOfMessages = 10
	DefaultWaitTimeSeconds     = 1
	DefaultVisibilityTimeout   = 30
)

type Server struct {
	QueueURL     string
	Client       sqsiface.SQSAPI
	Handler      Handler
	ErrorHandler func(error)
	Concurrency  int

	AttributeNames        []*string
	MessageAttributeNames []*string
	MaxNumberOfMessages   *int64
	WaitTimeSeconds       *int64
	VisibilityTimeout     *int64

	shutdown chan struct{}
	done     chan struct{}
}

// ServerDefaults is used by NewServer to initialize a Server with defaults.
func ServerDefaults(s *Server) {
	s.Client = sqs.New(session.New())
	s.Concurrency = DefaultConcurrency
	s.ErrorHandler = func(err error) {
		fmt.Fprintln(os.Stderr, err.Error())
	}

	s.AttributeNames = []*string{
		aws.String(sqs.MessageSystemAttributeNameSentTimestamp),
	}
	s.MessageAttributeNames = []*string{
		aws.String(sqs.QueueAttributeNameAll),
	}
	s.MaxNumberOfMessages = aws.Int64(DefaultMaxNumberOfMessages)
	s.WaitTimeSeconds = aws.Int64(DefaultWaitTimeSeconds)
	s.VisibilityTimeout = aws.Int64(DefaultVisibilityTimeout)
}

// WithClient configures a Server with a custom sqs Client.
func WithClient(c sqsiface.SQSAPI) func(s *Server) {
	return func(s *Server) {
		s.Client = c
	}
}

// NewServer creates a new Server.
func NewServer(queueURL string, h Handler, opts ...func(*Server)) *Server {
	s := &Server{
		QueueURL: queueURL,
		Handler:  h,

		shutdown: make(chan struct{}),
		done:     make(chan struct{}),
	}

	opts = append([]func(*Server){ServerDefaults}, opts...)
	for _, opt := range opts {
		opt(s)
	}

	return s
}

func (c *Server) Start() {
	var wg sync.WaitGroup
	messagesCh := make(chan *Message)

	// ReceiveMessage request loop
	go func() {
		for {
			select {
			case <-c.shutdown:
				close(messagesCh)
				wg.Wait()
				close(c.done)
				return
			default:
				out, err := c.receiveMessage()
				if err != nil {
					c.ErrorHandler(err)
				} else {
					for _, message := range out.Messages {
						m := &Message{
							QueueURL:   c.QueueURL,
							SQSMessage: message,
							Retryer:    DefaultRetrier,

							client: c.Client,
							ctx:    context.Background(),
						}
						messagesCh <- m // this will block if Subscribers are not ready to receive
					}
				}
			}
		}
	}()

	for i := 0; i < c.Concurrency; i++ {
		c.startWorker(messagesCh, &wg)
	}
}

func (c *Server) Shutdown(ctx context.Context) error {
	close(c.shutdown)

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.done:
		return nil
	}
}

func (c *Server) receiveMessage() (*sqs.ReceiveMessageOutput, error) {
	return c.Client.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueUrl:              aws.String(c.QueueURL),
		AttributeNames:        c.AttributeNames,
		MessageAttributeNames: c.MessageAttributeNames,
		MaxNumberOfMessages:   c.MaxNumberOfMessages,
		WaitTimeSeconds:       c.WaitTimeSeconds,
		VisibilityTimeout:     c.VisibilityTimeout,
	})
}

func (c *Server) startWorker(messages <-chan *Message, wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		c.processMessages(messages)
	}()
}

func (c *Server) processMessages(messages <-chan *Message) {
	h := RootHandler(c.Handler)

	for m := range messages {
		if err := h.HandleMessage(m); err != nil {
			c.ErrorHandler(err)
		}
	}
}

// RootHandler will retry a message by extending its visibility timeout if any error occurs.
// Queues MUST have a dead letter queue for this to work properly.
func RootHandler(h Handler) Handler {
	return HandlerFunc(func(m *Message) error {
		// Process message.
		if err := h.HandleMessage(m); err != nil {
			// Processing failed. Add a delay to the message before retrying.
			return DelayVisibility(m)
		}

		// Processing successful, delete message
		return m.Delete()
	})
}
