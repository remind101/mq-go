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

	deletionsCh chan *Message
	shutdown    chan struct{}
	done        chan struct{}
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

		deletionsCh: make(chan *Message, BatchDeleteMaxMessages),
		shutdown:    make(chan struct{}),
		done:        make(chan struct{}),
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
	messagesCh := make(chan *Message)
	doneProcessing := make(chan struct{})
	doneDeleting := make(chan struct{})

	// DeleteMessageBatch request loop. Messages will be deleted when we have
	// 10 messages to delete or when the ticker ticks, whichever comes first.
	go func() {
		input := &sqs.DeleteMessageBatchInput{
			QueueUrl: aws.String(c.QueueURL),
			Entries:  []*sqs.DeleteMessageBatchRequestEntry{},
		}
		t := time.NewTicker(DefaultDeletionTimeout)
		var lastBatchDelete time.Time

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
				input.Entries = append(input.Entries, &sqs.DeleteMessageBatchRequestEntry{
					Id:            m.SQSMessage.MessageId,
					ReceiptHandle: m.SQSMessage.ReceiptHandle,
				})

				if len(input.Entries) >= BatchDeleteMaxMessages {
					c.deleteMessageBatch(input)
					lastBatchDelete = time.Now()
				}

			// If processing is finished, spawn a goroutine to drain the rest of the
			// deletion channel.
			case <-doneProcessing:
				for m := range c.deletionsCh {
					input.Entries = append(input.Entries, &sqs.DeleteMessageBatchRequestEntry{
						Id:            m.SQSMessage.MessageId,
						ReceiptHandle: m.SQSMessage.ReceiptHandle,
					})

					if len(input.Entries) >= BatchDeleteMaxMessages {
						c.deleteMessageBatch(input)
					}
				}

				// Flush the buffer if any entries remain
				if len(input.Entries) > 0 {
					c.deleteMessageBatch(input)
				}
				close(doneDeleting)
			}
		}
	}()

	// ReceiveMessage request loop
	go func() {
		for {
			select {
			case <-c.shutdown:
				close(messagesCh)
				wg.Wait()
				close(doneProcessing)
				close(c.deletionsCh)
				return
			default:
				out, err := c.receiveMessage()
				if err != nil {
					c.ErrorHandler(err)
					time.Sleep(1 * time.Second)
				} else {
					for _, message := range out.Messages {
						m := NewMessage(c.QueueURL, message, c.Client)
						messagesCh <- m // this will block if Subscribers are not ready to receive
					}
				}
			}
		}
	}()

	for i := 0; i < c.Concurrency; i++ {
		c.startWorker(messagesCh, &wg)
	}

	go func() {
		// Close main done channel once everything is done.
		<-doneProcessing
		<-doneDeleting
		close(c.done)
	}()
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

func (c *Server) startWorker(messages <-chan *Message, wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		c.processMessages(messages)
	}()
}

// processMessages processes messages by passing them to the Handler. If no
// error is returned the message is queued for deletion.
func (c *Server) processMessages(messages <-chan *Message) {
	for m := range messages {
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

// func handleDeletes() {
// 	batchInput := &sqs.DeleteMessageBatchInput{
// 		QueueUrl: s.queueURL,
// 	}
// 	var (
// 		err           error
// 		entriesBuffer []*sqs.DeleteMessageBatchRequestEntry
// 		delRequest    *deleteRequest
// 	)
// 	for delRequest = range s.toDelete {
// 		entriesBuffer = append(entriesBuffer, delRequest.entry)
// 		// if the subber is stopped and this is the last request,
// 		// flush quit!
// 		if s.isStopped() && s.inFlightCount() == 1 {
// 			break
// 		}
// 		// if buffer is full, send the request
// 		if len(entriesBuffer) > *s.cfg.DeleteBufferSize {
// 			batchInput.Entries = entriesBuffer
// 			_, err = s.sqs.DeleteMessageBatch(batchInput)
// 			// cleaer buffer
// 			entriesBuffer = []*sqs.DeleteMessageBatchRequestEntry{}
// 		}

// 		delRequest.receipt <- err
// 	}
// 	// clear any remainders before shutdown
// 	if len(entriesBuffer) > 0 {
// 		batchInput.Entries = entriesBuffer
// 		_, err = s.sqs.DeleteMessageBatch(batchInput)
// 		delRequest.receipt <- err
// 	}
// }

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
