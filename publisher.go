package mq

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
)

// DefaultPublishInterval is the default interval the Publisher will send messages
// if the batch is not full.
const DefaultPublishInterval = 1 * time.Second

// Publisher is a publisher that efficiently sends messages to a single SQS Queue.
// It maintains a buffer of messages that is sent to SQS when it is full or when
// the publish interval is reached.
type Publisher struct {
	QueueURL string
	Client   sqsiface.SQSAPI

	PublishInterval  time.Duration
	BatchMaxMessages int

	OutputHandler func(*sqs.SendMessageBatchOutput, error)
	Logger        Logger

	messagesCh chan *sqs.SendMessageBatchRequestEntry
	shutdown   chan struct{}
	done       chan struct{}
}

// PublisherOpt defines a function that configures a Publisher.
type PublisherOpt func(*Publisher)

// PublisherDefaults contains the default configuration for a new Publisher.
var PublisherDefaults = func(p *Publisher) {
	p.Client = sqs.New(session.New())
	p.PublishInterval = DefaultPublishInterval
	p.BatchMaxMessages = DefaultMaxNumberOfMessages
	p.OutputHandler = func(out *sqs.SendMessageBatchOutput, err error) {
		if err != nil {
			fmt.Println(err.Error())
		}
		if len(out.Failed) > 0 {
			for _, entry := range out.Failed {
				fmt.Printf("Failed message send: %+v\n", entry)
			}
		}
	}
	p.Logger = &discardLogger{}
}

// NewPublisher returns a new Publisher with sensible defaults.
func NewPublisher(queueURL string, opts ...PublisherOpt) *Publisher {
	p := &Publisher{
		QueueURL: queueURL,
	}

	opts = append([]PublisherOpt{PublisherDefaults}, opts...)
	for _, opt := range opts {
		opt(p)
	}

	p.messagesCh = make(chan *sqs.SendMessageBatchRequestEntry, p.BatchMaxMessages)
	p.shutdown = make(chan struct{})
	p.done = make(chan struct{})

	return p
}

// Publish adds entry to the internal messages buffer.
func (p *Publisher) Publish(entry *sqs.SendMessageBatchRequestEntry) {
	p.messagesCh <- entry
}

// Start starts the message batching routine.
func (p *Publisher) Start() {
	go p.startMessageBatcher()
}

// Shutdown shuts the Publisher message batching routine down cleanly.
func (p *Publisher) Shutdown(ctx context.Context) error {
	close(p.shutdown)

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-p.done:
		return nil
	}
}

func (p *Publisher) startMessageBatcher() {
	input := &sqs.SendMessageBatchInput{
		QueueUrl: aws.String(p.QueueURL),
		Entries:  []*sqs.SendMessageBatchRequestEntry{},
	}
	t := time.NewTicker(p.PublishInterval)
	var lastBatchSend time.Time

	addToBatch := func(entry *sqs.SendMessageBatchRequestEntry) {
		p.Logger.Println(fmt.Sprintf("adding message for batch sending: %s", aws.StringValue(entry.MessageBody)))
		input.Entries = append(input.Entries, entry)
		if len(input.Entries) >= p.BatchMaxMessages {
			p.sendMessageBatch(input)
			lastBatchSend = time.Now()
		}
	}

	for {
		select {
		// If no batch sends have occurred between ticks, trigger a
		// batch send
		case tick := <-t.C:
			if tick.Sub(lastBatchSend) > p.PublishInterval && len(input.Entries) > 0 {
				p.Logger.Println("no message sent within the PublishInterval, triggering a send")
				p.sendMessageBatch(input)
			}

		// If a deletion is received, append to the buffer, and flush
		// if the buffer is full.
		case m := <-p.messagesCh:
			addToBatch(m)

		// If processing is finished, drain the rest of the
		// messages channel.
		case <-p.shutdown:
			p.Logger.Println("received shutdown signal, closing messages channel")
			close(p.messagesCh)
			for m := range p.messagesCh {
				addToBatch(m)
			}

			// Flush the buffer if any entries remain
			if len(input.Entries) > 0 {
				p.sendMessageBatch(input)
			}

			p.Logger.Println("shut down cleanly")
			close(p.done)
			return
		}
	}
}

func (p *Publisher) sendMessageBatch(input *sqs.SendMessageBatchInput) {
	p.Logger.Println(fmt.Sprintf("batch sending %d messages", len(input.Entries)))

	out, err := p.Client.SendMessageBatch(input)

	// Clear entries for next batch.
	input.Entries = []*sqs.SendMessageBatchRequestEntry{}

	p.OutputHandler(out, err)
}
