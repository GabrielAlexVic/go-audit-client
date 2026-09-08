package auditclient

import "time"

type AuditEvent struct {
	EventID     string    `json:"event_id"`
	EventTime   time.Time `json:"event_time"`
	ServiceName string    `json:"service_name"`
	LogLine     string    `json:"log_line"`
}
