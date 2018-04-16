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

const DefaultConcurrency = 1

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

func ServerDefaults(s *Server) {
	s.Client = sqs.New(session.New())
	s.Concurrency = DefaultConcurrency
	s.Handler = HandlerFunc(func(c sqsiface.SQSAPI, m *Message) error {
		fmt.Printf("%+v\n", m)
		return nil
	})
	s.ErrorHandler = func(err error) {
		fmt.Fprintln(os.Stderr, err.Error())
	}

	s.AttributeNames = []*string{
		aws.String(sqs.MessageSystemAttributeNameSentTimestamp),
	}
	s.MessageAttributeNames = []*string{
		aws.String(sqs.QueueAttributeNameAll),
	}
	s.MaxNumberOfMessages = aws.Int64(10)
	s.WaitTimeSeconds = aws.Int64(20)
	s.VisibilityTimeout = aws.Int64(30)
}

func NewServer(opts ...func(*Server)) *Server {
	s := &Server{
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
							ctx:        context.Background(),
							QueueURL:   c.QueueURL,
							SQSMessage: message,
							Retryer:    DefaultRetrier,
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
	for m := range messages {
		if err := c.Handler.HandleMessage(c.Client, m); err != nil {
			c.ErrorHandler(err)
		}
	}
}
