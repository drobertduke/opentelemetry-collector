package telemetrybufferprocessor

import (
	"encoding/hex"
	"time"
)

// TelemetryItem is a common interface for all telemetry items.
type TelemetryItem interface {
	GetTraceID() string
	GetServiceName() string
	GetTimestamp() time.Time
}

// SpanItem represents a trace span.
type SpanItem struct {
	TraceID       []byte
	SpanID        []byte
	ParentSpanID  []byte
	Name          string
	Kind          int32
	StartTime     uint64
	EndTime       uint64
	Duration      time.Duration
	StatusCode    int32
	StatusMessage string
	ServiceName   string
	Attributes    map[string]interface{}
}

// GetTraceID returns the trace ID as a hex string.
func (s *SpanItem) GetTraceID() string {
	return hex.EncodeToString(s.TraceID)
}

// GetServiceName returns the service name.
func (s *SpanItem) GetServiceName() string {
	return s.ServiceName
}

// GetTimestamp returns the start time as a time.Time.
func (s *SpanItem) GetTimestamp() time.Time {
	return time.Unix(0, int64(s.StartTime))
}

// MetricItem represents a metric data point.
type MetricItem struct {
	Name        string
	Description string
	Unit        string
	Type        string
	Timestamp   uint64
	Value       interface{}
	ServiceName string
	Attributes  map[string]interface{}
}

// GetTraceID returns an empty string for metrics.
func (m *MetricItem) GetTraceID() string {
	return ""
}

// GetServiceName returns the service name.
func (m *MetricItem) GetServiceName() string {
	return m.ServiceName
}

// GetTimestamp returns the timestamp as a time.Time.
func (m *MetricItem) GetTimestamp() time.Time {
	return time.Unix(0, int64(m.Timestamp))
}

// LogItem represents a log record.
type LogItem struct {
	TraceID        []byte
	SpanID         []byte
	Timestamp      uint64
	SeverityText   string
	SeverityNumber int32
	Body           string
	ServiceName    string
	Attributes     map[string]interface{}
}

// GetTraceID returns the trace ID as a hex string.
func (l *LogItem) GetTraceID() string {
	return hex.EncodeToString(l.TraceID)
}

// GetServiceName returns the service name.
func (l *LogItem) GetServiceName() string {
	return l.ServiceName
}

// GetTimestamp returns the timestamp as a time.Time.
func (l *LogItem) GetTimestamp() time.Time {
	return time.Unix(0, int64(l.Timestamp))
}
