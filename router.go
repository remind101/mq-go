package mq

import (
	"errors"
	"fmt"
	"sync"

	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
)

// Router will route a message based on MessageAttributes to other registered Handlers.
type Router struct {
	sync.Mutex

	// Resolver maps a Message to a string identifier used to match to a registered Handler. The
	// default implementation returns a MessageAttribute named "route".
	Resolver func(*Message) (string, bool)

	// A map of handlers to route to. The return value of Resolver should match a key in this map.
	handlers map[string]Handler
}

func NewRouter() *Router {
	return &Router{
		Resolver: func(m *Message) (string, bool) {
			r := ""
			v, ok := m.SQSMessage.MessageAttributes["route"]
			if ok {
				r = *v.StringValue
			}
			return r, ok
		},
		handlers: map[string]Handler{},
	}
}

func (r *Router) Handle(route string, h Handler) {
	r.Lock()
	defer r.Unlock()

	r.handlers[route] = h
}

func (r *Router) HandleMessage(c sqsiface.SQSAPI, m *Message) error {
	key, ok := r.Resolver(m)
	if !ok {
		return errors.New("no routing key for message")
	}

	if h, ok := r.handlers[key]; ok {
		return h.HandleMessage(c, m)
	}

	return fmt.Errorf("no handler matched for routing key: %s", key)
}
