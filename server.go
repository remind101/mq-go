package mq

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
)

const (
	// DefaultConcurrency is the default concurrency for the Server.
	DefaultConcurrency = 1

	// DefaultMaxNumberOfMessages is the default maximum number of messages the
	// Server will request when receiving messages.
	DefaultMaxNumberOfMessages = 10

	// DefaultWaitTimeSeconds is the default WaitTimeSeconds used when receiving
	// messages.
	DefaultWaitTimeSeconds = 1

	// DefaultVisibilityTimeout is the default VisibilityTimeout used when
	// receiving messages.
	DefaultVisibilityTimeout = 30

	// DefaultDeletionTimeout is the default amount of time before any pending
	// deletions are batch deleted.
	DefaultDeletionTimeout = 10

	// BatchDeleteMaxMessages is the maximum allowed number of messages in a
	// batch delete request.
	BatchDeleteMaxMessages = 10
)

// Server is responsible for running the request loop to receive SQS messages
// from a single SQS Queue, and pass them to a Handler.
//
// Graceful shutdown:
//
// There are three parts to the message processing pipeline:
//
// 1. Receiving loop - pushes Messages to the messagesCh
// 2. Processing loops (multiple) - reads from messagesCh, pushes Messages to the deletionsCh
// 3. Deletion loop - reads from deletionsCh
//
// On shutdown, the receiving loop is closed, and closes the messagesCh used by
// processing loops.
//
// Once processing loops have drained the messagesCh and finished
// processing, they will close the doneProcessing channel.
//
// After doneProcessing is closed, the deletion loop will drain the deletionsCh
// and after finishing, close the doneDeleting channel.
//
// After the doneDeleting channel is closed, the done channel is closed, signaling
// that the Server has shutdown gracefully.
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

	messagesCh     chan *Message
	doneProcessing chan struct{}

	deletionsCh  chan *Message
	doneDeleting chan struct{}

	done chan struct{}
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

		messagesCh:     make(chan *Message),
		deletionsCh:    make(chan *Message, BatchDeleteMaxMessages),
		doneProcessing: make(chan struct{}),
		doneDeleting:   make(chan struct{}),
		shutdown:       make(chan struct{}),
		done:           make(chan struct{}),
	}

	opts = append([]func(*Server){ServerDefaults}, opts...)
	for _, opt := range opts {
		opt(s)
	}

	return s
}

// Start starts the request loop for receiving messages and a configurable
// number of Handler routines for message processing.
func (c *Server) Start() {
	var wg sync.WaitGroup

	go c.startDeleter()

	// ReceiveMessage request loop
	go func() {
		for {
			select {
			case <-c.shutdown:
				close(c.messagesCh)
				wg.Wait()
				close(c.doneProcessing)
				return
			default:
				out, err := c.receiveMessage()
				if err != nil {
					c.ErrorHandler(err)
					time.Sleep(1 * time.Second)
				} else {
					for _, message := range out.Messages {
						m := NewMessage(c.QueueURL, message, c.Client)
						c.messagesCh <- m // this will block if Subscribers are not ready to receive
					}
				}
			}
		}
	}()

	for i := 0; i < c.Concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.processMessages()
		}()
	}

	go func() {
		// Close main done channel once everything is done.
		<-c.doneProcessing
		<-c.doneDeleting
		close(c.done)
	}()
}

func (c *Server) startDeleter() {
	// DeleteMessageBatch request loop. Messages will be deleted when we have
	// 10 messages to delete or when the ticker ticks, whichever comes first.
	input := &sqs.DeleteMessageBatchInput{
		QueueUrl: aws.String(c.QueueURL),
		Entries:  []*sqs.DeleteMessageBatchRequestEntry{},
	}
	t := time.NewTicker(DefaultDeletionTimeout)
	var lastBatchDelete time.Time

	addToBatch := func(m *sqs.Message) {
		input.Entries = append(input.Entries, &sqs.DeleteMessageBatchRequestEntry{
			Id:            m.MessageId,
			ReceiptHandle: m.ReceiptHandle,
		})

		if len(input.Entries) >= BatchDeleteMaxMessages {
			c.deleteMessageBatch(input)
			// Clear entries for next batch.
			input.Entries = []*sqs.DeleteMessageBatchRequestEntry{}
			lastBatchDelete = time.Now()
		}
	}

	for {
		select {
		// If no batch deletes have occurred between ticks, trigger a
		// batch delete
		case tick := <-t.C:
			if tick.Sub(lastBatchDelete) > DefaultDeletionTimeout {
				c.deleteMessageBatch(input)
			}

		// If a deletion is received, append to the buffer, and flush
		// if the buffer is full.
		case m := <-c.deletionsCh:
			addToBatch(m.SQSMessage)

		// If processing is finished, drain the rest of the
		// deletion channel.
		case <-c.doneProcessing:
			close(c.deletionsCh)
			for m := range c.deletionsCh {
				addToBatch(m.SQSMessage)
			}

			// Flush the buffer if any entries remain
			if len(input.Entries) > 0 {
				c.deleteMessageBatch(input)
			}
			close(c.doneDeleting)
			return
		}
	}
}

// Shutdown gracefully shuts down the Server.
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

// processMessages processes messages by passing them to the Handler. If no
// error is returned the message is queued for deletion.
func (c *Server) processMessages() {
	for m := range c.messagesCh {
		if err := c.Handler.HandleMessage(m); err != nil {
			c.ErrorHandler(err)
		} else {
			c.deleteMessage(m)
		}
	}
}

func (c *Server) deleteMessage(m *Message) {
	c.deletionsCh <- m
}

func (c *Server) deleteMessageBatch(input *sqs.DeleteMessageBatchInput) {
	out, err := c.Client.DeleteMessageBatch(input)
	if err != nil {
		c.ErrorHandler(err)
	}
	for _, failure := range out.Failed {
		e := fmt.Errorf("failed to delete message id=%s code=%s error=%s sender_fault=%t",
			aws.StringValue(failure.Id),
			aws.StringValue(failure.Code),
			aws.StringValue(failure.Message),
			aws.BoolValue(failure.SenderFault),
		)
		c.ErrorHandler(e)
	}
}

// ServerGroup represents a list of Servers.
type ServerGroup struct {
	Servers []*Server
}

// Start starts all servers in the group.
func (sg *ServerGroup) Start() {
	for _, s := range sg.Servers {
		s.Start()
	}
}

// Shutdown gracefully shuts down all servers.
func (sg *ServerGroup) Shutdown(ctx context.Context) <-chan error {
	errCh := make(chan error, len(sg.Servers))
	for _, s := range sg.Servers {
		errCh <- s.Shutdown(ctx)
	}
	return errCh
}

// RootHandler is a root handler responsible for adding delay in messages
// that have error'd.
//
// Queues MUST have a dead letter queue or else messages that cannot succeed
// will never be removed from the queue.
func RootHandler(h Handler) Handler {
	return HandlerFunc(func(m *Message) error {
		// Process message.
		if err := h.HandleMessage(m); err != nil {
			if e, ok := err.(delayable); ok {
				return m.ChangeVisibility(e.Delay())
			}
			return ChangeVisibilityWithRetryPolicy(m)
		}

		return nil
	})
}

type delayable interface {
	Delay() *int64 // Seconds
}

// ChangeVisibilityWithRetryPolicy will change the visibility of a message based
// on the error of message retry policy.
//
// If the delay is equal to the zero, this is a no op.
func ChangeVisibilityWithRetryPolicy(m *Message) error {
	receiveCount := receiveCount(m)
	delay := m.RetryPolicy.Delay(receiveCount)

	if aws.Int64Value(delay) == 0 {
		return nil
	}

	return m.ChangeVisibility(delay)
}

func receiveCount(m *Message) int {
	v := m.SQSMessage.Attributes[sqs.MessageSystemAttributeNameApproximateReceiveCount]
	receiveCount, _ := strconv.Atoi(aws.StringValue(v))
	return receiveCount
}
