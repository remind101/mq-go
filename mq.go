package mq // import "github.com/remind101/mq-go"

// A Handler processes a Message.
type Handler interface {
	HandleMessage(*Message) error
}

// HandlerFunc is an adaptor to allow the use of ordinary functions as message Handlers.
type HandlerFunc func(*Message) error

// HandleMessage satisfies the Handler interface.
func (h HandlerFunc) HandleMessage(m *Message) error {
	return h(m)
}
