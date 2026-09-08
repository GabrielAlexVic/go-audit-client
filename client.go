package auditclient

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type Client struct {
	nc          *nats.Conn
	stream      jetstream.JetStream
	subject     string
	serviceName string
}

// NewClient creates a new Audit Client which implements io.Writer.
func NewClient(natsURL, serviceName string, opts ...Option) (*Client, error) {
	defaultOptions := Options{
		Subject: "audit.log",
		NatsOptions: []nats.Option{
			nats.Name(serviceName),
			nats.MaxReconnects(-1),
			nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
				log.Println("NATS: disconnected", "error", err)
			}),
			nats.ReconnectHandler(func(c *nats.Conn) {
				log.Println("NATS: reconnected")
			}),
			nats.ErrorHandler(func(_ *nats.Conn, sub *nats.Subscription, err error) {
				subject := "unknown"
				if sub != nil {
					subject = sub.Subject
				}
				log.Println("NATS: async error", "subject", subject, "error", err)
			}),
			nats.ClosedHandler(func(_ *nats.Conn) {
				log.Println("NATS: connection closed")
			}),
		},
	}

	for _, opt := range opts {
		opt(&defaultOptions)
	}

	nc, err := nats.Connect(natsURL, defaultOptions.NatsOptions...)
	if err != nil {
		return nil, err
	}

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, err
	}

	return &Client{
		nc:          nc,
		stream:      js,
		subject:     defaultOptions.Subject,
		serviceName: serviceName,
	}, nil
}

func (c *Client) Write(p []byte) (n int, err error) {
	event := AuditEvent{
		EventID:     uuid.NewString(),
		EventTime:   time.Now(),
		ServiceName: c.serviceName,
		LogLine:     string(p),
	}

	payload, err := json.Marshal(event)
	if err != nil {
		log.Println("AuditClient: Failed to marshal audit event", err)
		return len(p), nil
	}

	_, err = c.stream.Publish(context.Background(), c.subject, payload)
	if err != nil {
		log.Println("AuditClient: Failed to send log/audit to remote server", err)
	}

	return len(p), nil
}

func (c *Client) Close() {
	if c.nc != nil {
		c.nc.Drain()
	}
}
