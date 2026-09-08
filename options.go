package auditclient

import "github.com/nats-io/nats.go"

type Options struct {
	Subject     string
	NatsOptions []nats.Option
}

type Option func(*Options)

func WithSubject(subject string) Option {
	return func(o *Options) {
		o.Subject = subject
	}
}

// WithNatsOptions allows passing custom nats.Option like nats.MaxReconnects, etc.
func WithNatsOptions(opts ...nats.Option) Option {
	return func(o *Options) {
		o.NatsOptions = append(o.NatsOptions, opts...)
	}
}
