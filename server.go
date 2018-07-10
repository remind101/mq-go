package mq

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
)

const (
	// DefaultConcurrency is the default concurrency for the Server.
	DefaultConcurrency = 1

	// DefaultMaxNumberOfMessages defaults to the maximum number of messages the
	// Server can request when receiving messages.
	DefaultMaxNumberOfMessages = 10

	// DefaultWaitTimeSeconds is the default WaitTimeSeconds used when receiving
	// messages.
	DefaultWaitTimeSeconds = 1

	// DefaultVisibilityTimeout is the default VisibilityTimeout used when
	// receiving messages in seconds.
	DefaultVisibilityTimeout = 30

	// DefaultDeletionInterval is the default interval at which messages pending
	// deletion are batch deleted (if number of pending has not reached
	// BatchDeleteMaxMessages).
	DefaultDeletionInterval = 10 * time.Second

	// DefaultBatchDeleteMaxMessages defaults to the the maximum allowed number
	// of messages in a batch delete request.
	DefaultBatchDeleteMaxMessages = 10
)

// Logger defines a simple interface to support debug logging in the Server.
type Logger interface {
	Println(...interface{})
}

type discardLogger struct{}

func (l *discardLogger) Println(v ...interface{}) {}

// Server is responsible for running the request loop to receive messages
// from a single SQS Queue. It manages the message processing pipeline.
//
// There are three sections of the processing pipeline:
//
// 1. Receiving messages
// The Server starts a single goroutine to batch request messages from QueueURL.
// The frequency of this call is controlled with WaitTimeSeconds. Messages are
// sent to an unbuffered channel. This ensures that the Server does not continue
// requesting messages is the processing goroutines are unable to keep up.
//
// 2. Processing messages
// The Server starts one or more goroutines for processing messages from the
// messages channel. Concurrency is controlled by Server.Processor and
// Server.Concurrency. These goroutines simply pass messages to the Handler. If
// the Handler returns no error, the message will be sent to the deletions
// channel.
//
// 3. Deleting messages
// The Server starts a single goroutine to batch delete processed messages.
// It will delete messages when the batch size is reached or if no deletions
// have occurred within an interval. This interval must be smaller than the
// VisibilityTimeout or messages could be received again before deletion occurs.
//
// On shutdown, the receiving loop ends, and the messages channel used by
// processing loops is closed.
//
// Once processing loops have drained the messages channel and finished
// processing, they will signal to the deletion goroutine to finish.
//
// When the deletion goroutine receives the signal, the deletion loop will drain
// the deletions channel and after finishing, close the done channel, signaling
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

	BatchDeleteMaxMessages int
	DeletionInterval       time.Duration

	Logger    Logger
	Processor Processor

	shutdown chan struct{}

	messagesCh     chan *Message
	doneProcessing chan struct{}

	deletionsCh chan *Message
	errorsCh    chan error

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
	s.BatchDeleteMaxMessages = DefaultBatchDeleteMaxMessages
	s.DeletionInterval = DefaultDeletionInterval
	s.Logger = &discardLogger{}
	s.Processor = &BoundedProcessor{s}
}

// WithClient configures a Server with a custom sqs Client.
func WithClient(c sqsiface.SQSAPI) func(s *Server) {
	return func(s *Server) {
		s.Client = c
	}
}

// WithConcurrency configures a Server with c Concurrency.
func WithConcurrency(c int) func(s *Server) {
	return func(s *Server) {
		s.Concurrency = c
	}
}

// WithPartitionedProcessor configures a Server with a partitioned Processor.
var WithPartitionedProcessor = func(s *Server) {
	s.Processor = &PartitionedProcessor{s}
}

// NewServer creates a new Server.
func NewServer(queueURL string, h Handler, opts ...func(*Server)) *Server {
	s := &Server{
		QueueURL: queueURL,
		Handler:  h,
	}

	opts = append([]func(*Server){ServerDefaults}, opts...)
	for _, opt := range opts {
		opt(s)
	}

	s.messagesCh = make(chan *Message)
	s.deletionsCh = make(chan *Message, s.BatchDeleteMaxMessages)
	s.errorsCh = make(chan error)
	s.doneProcessing = make(chan struct{})
	s.shutdown = make(chan struct{})
	s.done = make(chan struct{})

	return s
}

// Start starts the request loop for receiving messages and a configurable
// number of Handler routines for message processing.
func (c *Server) Start() {
	go c.startReceiver()
	go c.startProcessor()
	go c.startErrorHandler()
	go c.startDeleter()
}

func (c *Server) startReceiver() {
	for {
		select {
		case <-c.shutdown:
			c.Logger.Println("received shutdown signal, closing messages channel")
			close(c.messagesCh)
			return
		default:
			out, err := c.receiveMessage()
			if err != nil {
				c.ErrorHandler(err)
				time.Sleep(1 * time.Second)
			} else {
				// TODO: If no messages are received, increase WaitTimeSeconds
				// on the next request. When messages are received reset
				// WaitTimeSeconds to original value.
				for _, message := range out.Messages {
					m := NewMessage(c.QueueURL, message, c.Client)
					c.Logger.Println(fmt.Sprintf("adding message to the messages channel: %s", aws.StringValue(m.SQSMessage.ReceiptHandle)))
					c.messagesCh <- m // this will block if Subscribers are not ready to receive
				}
			}
		}
	}
}

func (c *Server) startProcessor() {
	c.Processor.Process(c.messagesCh, c.deletionsCh, c.errorsCh, c.doneProcessing)
}

func (c *Server) startErrorHandler() {
	for err := range c.errorsCh {
		c.ErrorHandler(err)
	}
}

func (c *Server) startDeleter() {
	// DeleteMessageBatch request loop. Messages will be deleted when we have
	// 10 messages to delete or when the ticker ticks, whichever comes first.
	input := &sqs.DeleteMessageBatchInput{
		QueueUrl: aws.String(c.QueueURL),
		Entries:  []*sqs.DeleteMessageBatchRequestEntry{},
	}
	t := time.NewTicker(c.DeletionInterval)
	var lastBatchDelete time.Time

	addToBatch := func(m *sqs.Message) {
		c.Logger.Println(fmt.Sprintf("adding message for batch deletion: %s", aws.StringValue(m.ReceiptHandle)))
		input.Entries = append(input.Entries, &sqs.DeleteMessageBatchRequestEntry{
			Id:            m.MessageId,
			ReceiptHandle: m.ReceiptHandle,
		})

		if len(input.Entries) >= c.BatchDeleteMaxMessages {
			c.deleteMessageBatch(input)
			lastBatchDelete = time.Now()
		}
	}

	for {
		select {
		// If no batch deletes have occurred between ticks, trigger a
		// batch delete
		case tick := <-t.C:
			if tick.Sub(lastBatchDelete) > c.DeletionInterval && len(input.Entries) > 0 {
				c.Logger.Println("no message deleted within the DeletionInterval, triggering a deletion")
				c.deleteMessageBatch(input)
			}

		// If a deletion is received, append to the buffer, and flush
		// if the buffer is full.
		case m := <-c.deletionsCh:
			addToBatch(m.SQSMessage)

		// If processing is finished, drain the rest of the
		// deletion channel.
		case <-c.doneProcessing:
			c.Logger.Println("draining deletions channel")
			close(c.deletionsCh)
			close(c.errorsCh)
			for m := range c.deletionsCh {
				addToBatch(m.SQSMessage)
			}

			// Flush the buffer if any entries remain
			if len(input.Entries) > 0 {
				c.deleteMessageBatch(input)
			}

			// Signal that the server is done, this is the last step in the
			// processing pipeline.
			c.Logger.Println("shut down cleanly")
			close(c.done)
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

func (c *Server) deleteMessageBatch(input *sqs.DeleteMessageBatchInput) {
	messageHandles := make([]string, len(input.Entries))
	for i, e := range input.Entries {
		messageHandles[i] = aws.StringValue(e.ReceiptHandle)
	}
	c.Logger.Println(fmt.Sprintf("batch deleting %d messages: %s", len(input.Entries), strings.Join(messageHandles, ", ")))

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
	// Clear entries for next batch.
	input.Entries = []*sqs.DeleteMessageBatchRequestEntry{}
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
