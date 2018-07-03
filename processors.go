package mq

import (
	"fmt"
	"hash/fnv"
	"math/rand"
	"sync"
)

// Processor defines an interface for processing messages.
type Processor interface {
	// Process processes messages. A well behaved processor will:
	// * Receive messages from the messagesCh in a loop until that channel is
	//   closed.
	// * Send messages to the deletionsCh if message was successfully processed.
	// * Close the done channel when finished processing.
	Process(messagesCh <-chan *Message, deletionsCh chan<- *Message, done chan struct{})
}

// BoundedProcessor is the default message processor. It creates
// Server.Concurrency goroutines that all consume from the messages channel.
type BoundedProcessor struct {
	Server *Server
}

// Process satisfies the Processor interface.
func (p *BoundedProcessor) Process(messagesCh <-chan *Message, deletionsCh chan<- *Message, done chan struct{}) {
	var wg sync.WaitGroup
	for i := 0; i < p.Server.Concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p.processMessages(messagesCh, deletionsCh)
		}()
	}
	wg.Wait()
	p.Server.Logger.Println("finished processing messages")
	close(done)
}

func (p *BoundedProcessor) processMessages(messagesCh <-chan *Message, deletionsCh chan<- *Message) {
	for m := range messagesCh {
		if err := p.Server.Handler.HandleMessage(m); err != nil {
			p.Server.ErrorHandler(err)
		} else {
			deletionsCh <- m
		}
	}
}

// MessageAttributeNamePartitionKey is the messages attribute used to determine
// the partition to process the message in.
const MessageAttributeNamePartitionKey = "partition_key"

// PartitionedProcessor is a processor that creates Server.Concurrency goroutines
// to process messages except each message is partitioned to the same goroutine
// based on the a consistent hash of the message's partition key. Messages with
// the same partition key are guaranteed to be processed by the same goroutine.
type PartitionedProcessor struct {
	Server *Server
}

// Process satisfies the Processor interface.
func (p *PartitionedProcessor) Process(messagesCh <-chan *Message, deletionsCh chan<- *Message, done chan struct{}) {
	chPool := make([]chan *Message, p.Server.Concurrency)
	var wg sync.WaitGroup
	for i := 0; i < p.Server.Concurrency; i++ {
		chPool[i] = make(chan *Message)
		wg.Add(1)
		go func(ch <-chan *Message) {
			defer wg.Done()
			p.processMessages(ch, deletionsCh)
		}(chPool[i])
	}

	go func() {
		for m := range messagesCh {
			index := p.partitionMessage(m, p.Server.Concurrency)
			p.Server.Logger.Println(fmt.Sprintf("partitioning message: %d", index))
			chPool[index] <- m
		}
		for _, ch := range chPool {
			close(ch)
		}
	}()

	wg.Wait()
	p.Server.Logger.Println("finished processing messages")
	close(done)
}

// processMessages processes messages by passing them to the Handler. If no
// error is returned the message is queued for deletion.
func (p *PartitionedProcessor) processMessages(messagesCh <-chan *Message, deletionsCh chan<- *Message) {
	for m := range messagesCh {
		if err := p.Server.Handler.HandleMessage(m); err != nil {
			p.Server.ErrorHandler(err)
		} else {
			deletionsCh <- m
		}
	}
}
func (p *PartitionedProcessor) partitionMessage(m *Message, shards int) int {
	if key, ok := m.SQSMessage.MessageAttributes[MessageAttributeNamePartitionKey]; ok {
		hash := fnv.New32a()
		hash.Write(key.BinaryValue)
		return int(int64(hash.Sum32()) % int64(shards))
	}

	return rand.Int() % shards
}
