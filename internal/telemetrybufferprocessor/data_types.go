package telemetrybufferprocessor

import (
	"time"
)

// TelemetryItem is a common interface for all telemetry items.
type TelemetryItem interface {
	// GetTimestamp returns the timestamp of the item.
	GetTimestamp() time.Time
	// GetTraceID returns the trace ID of the item, if applicable.
	GetTraceID() string
	// GetServiceName returns the service name of the item.
	GetServiceName() string
	// GetAttributes returns the attributes of the item.
	GetAttributes() map[string]interface{}
}

// SpanItem represents a span in the ring buffer.
type SpanItem struct {
	TraceID       string
	SpanID        string
	ParentSpanID  string
	Name          string
	Kind          string
	StartTime     time.Time
	EndTime       time.Time
	Duration      time.Duration
	StatusCode    string
	StatusMessage string
	ServiceName   string
	Attributes    map[string]interface{}
}

// GetTimestamp implements TelemetryItem.
func (s *SpanItem) GetTimestamp() time.Time {
	return s.StartTime
}

// GetTraceID implements TelemetryItem.
func (s *SpanItem) GetTraceID() string {
	return s.TraceID
}

// GetServiceName implements TelemetryItem.
func (s *SpanItem) GetServiceName() string {
	return s.ServiceName
}

// GetAttributes implements TelemetryItem.
func (s *SpanItem) GetAttributes() map[string]interface{} {
	return s.Attributes
}

// LogItem represents a log record in the ring buffer.
type LogItem struct {
	TraceID        string
	SpanID         string
	Timestamp      time.Time
	SeverityText   string
	SeverityNumber int32
	Body           string
	ServiceName    string
	Attributes     map[string]interface{}
}

// GetTimestamp implements TelemetryItem.
func (l *LogItem) GetTimestamp() time.Time {
	return l.Timestamp
}

// GetTraceID implements TelemetryItem.
func (l *LogItem) GetTraceID() string {
	return l.TraceID
}

// GetServiceName implements TelemetryItem.
func (l *LogItem) GetServiceName() string {
	return l.ServiceName
}

// GetAttributes implements TelemetryItem.
func (l *LogItem) GetAttributes() map[string]interface{} {
	return l.Attributes
}

// MetricItem represents a metric data point in the ring buffer.
type MetricItem struct {
	Name        string
	Description string
	Unit        string
	Type        string
	Timestamp   time.Time
	Value       interface{}
	ServiceName string
	Attributes  map[string]interface{}
}

// GetTimestamp implements TelemetryItem.
func (m *MetricItem) GetTimestamp() time.Time {
	return m.Timestamp
}

// GetTraceID implements TelemetryItem.
func (m *MetricItem) GetTraceID() string {
	return ""
}

// GetServiceName implements TelemetryItem.
func (m *MetricItem) GetServiceName() string {
	return m.ServiceName
}

// GetAttributes implements TelemetryItem.
func (m *MetricItem) GetAttributes() map[string]interface{} {
	return m.Attributes
}

// OTLPConverter is an interface for converting OTLP data to our internal representations.
type OTLPConverter interface {
	// ConvertTraces converts OTLP trace data to our internal representation.
	ConvertTraces(data []byte) ([]*SpanItem, error)

	// ConvertMetrics converts OTLP metric data to our internal representation.
	ConvertMetrics(data []byte) ([]*MetricItem, error)

	// ConvertLogs converts OTLP log data to our internal representation.
	ConvertLogs(data []byte) ([]*LogItem, error)
}
