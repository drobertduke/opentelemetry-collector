package telemetryqueryextension

import (
	"context"
	"encoding/hex"
	"fmt"
	"time"

	"telemetrycollector/internal/telemetrybufferprocessor"

	"github.com/golang/protobuf/ptypes/empty"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TelemetryQueryServiceServerImpl implements the gRPC service for querying telemetry data.
type TelemetryQueryServiceServerImpl struct {
	UnimplementedTelemetryQueryServiceServer
}

// NewTelemetryQueryServiceServer creates a new TelemetryQueryServiceServer.
func NewTelemetryQueryServiceServer() *TelemetryQueryServiceServerImpl {
	return &TelemetryQueryServiceServerImpl{}
}

// Query handles requests to query telemetry data from the ring buffer.
func (s *TelemetryQueryServiceServerImpl) Query(ctx context.Context, req *QueryRequest) (*QueryResponse, error) {
	processor := telemetrybufferprocessor.GetGlobalProcessor()
	if processor == nil {
		return nil, status.Error(codes.Unavailable, "telemetry processor not available")
	}

	// Create an empty response
	response := &QueryResponse{}

	// Process the query based on the telemetry type
	switch req.TelemetryType {
	case TelemetryType_TELEMETRY_TYPE_TRACES:
		return s.queryTraces(ctx, req, processor, response)
	case TelemetryType_TELEMETRY_TYPE_METRICS:
		return s.queryMetrics(ctx, req, processor, response)
	case TelemetryType_TELEMETRY_TYPE_LOGS:
		return s.queryLogs(ctx, req, processor, response)
	default:
		return nil, status.Error(codes.InvalidArgument, "invalid telemetry type")
	}
}

// Subscribe handles requests to subscribe to telemetry data from the ring buffer.
func (s *TelemetryQueryServiceServerImpl) Subscribe(req *SubscribeRequest, stream TelemetryQueryService_SubscribeServer) error {
	processor := telemetrybufferprocessor.GetGlobalProcessor()
	if processor == nil {
		return status.Error(codes.Unavailable, "telemetry processor not available")
	}

	// Get the appropriate buffer based on the telemetry type
	var buffer *telemetrybufferprocessor.RingBuffer
	switch req.TelemetryType {
	case TelemetryType_TELEMETRY_TYPE_TRACES:
		buffer = processor.GetTracesBuffer()
	case TelemetryType_TELEMETRY_TYPE_METRICS:
		buffer = processor.GetMetricsBuffer()
	case TelemetryType_TELEMETRY_TYPE_LOGS:
		buffer = processor.GetLogsBuffer()
	default:
		return status.Error(codes.InvalidArgument, "invalid telemetry type")
	}

	if buffer == nil {
		return status.Error(codes.Unavailable, "buffer not available")
	}

	// First, send the current contents of the buffer
	items := buffer.GetAll()
	filteredItems := s.filterItems(items, req.TraceId, req.ServiceName, req.StartTime)
	if len(filteredItems) > 0 {
		response := s.convertItemsToResponse(req.TelemetryType, filteredItems)
		if err := stream.Send(response); err != nil {
			return err
		}
	}

	// Create a ticker to periodically check for new items
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	// Keep track of the last size we saw
	lastSize := buffer.Size()

	// Stream new items as they arrive
	for {
		select {
		case <-stream.Context().Done():
			return nil
		case <-ticker.C:
			currentSize := buffer.Size()
			if currentSize > lastSize {
				// New items have been added
				items := buffer.GetAll()
				// Only get the new items (this is a simplification, might miss some if buffer wrapped around)
				newItems := items[lastSize:]
				filteredItems := s.filterItems(newItems, req.TraceId, req.ServiceName, req.StartTime)
				if len(filteredItems) > 0 {
					response := s.convertItemsToResponse(req.TelemetryType, filteredItems)
					if err := stream.Send(response); err != nil {
						return err
					}
				}
				lastSize = currentSize
			}
		}
	}
}

// GetBufferInfo returns information about the ring buffers.
func (s *TelemetryQueryServiceServerImpl) GetBufferInfo(ctx context.Context, _ *empty.Empty) (*BufferInfoResponse, error) {
	processor := telemetrybufferprocessor.GetGlobalProcessor()
	if processor == nil {
		return nil, status.Error(codes.Unavailable, "telemetry processor not available")
	}

	// Create the response
	response := &BufferInfoResponse{
		Buffers: []*BufferInfo{},
	}

	// Add traces buffer info
	if tracesBuffer := processor.GetTracesBuffer(); tracesBuffer != nil {
		response.Buffers = append(response.Buffers, &BufferInfo{
			TelemetryType: TelemetryType_TELEMETRY_TYPE_TRACES,
			Size:          int32(tracesBuffer.Size()),
			Capacity:      int32(tracesBuffer.Capacity()),
			// Note: We don't have a way to get the oldest/newest timestamps directly
			// This would require additional tracking in the ring buffer
		})
	}

	// Add metrics buffer info
	if metricsBuffer := processor.GetMetricsBuffer(); metricsBuffer != nil {
		response.Buffers = append(response.Buffers, &BufferInfo{
			TelemetryType: TelemetryType_TELEMETRY_TYPE_METRICS,
			Size:          int32(metricsBuffer.Size()),
			Capacity:      int32(metricsBuffer.Capacity()),
		})
	}

	// Add logs buffer info
	if logsBuffer := processor.GetLogsBuffer(); logsBuffer != nil {
		response.Buffers = append(response.Buffers, &BufferInfo{
			TelemetryType: TelemetryType_TELEMETRY_TYPE_LOGS,
			Size:          int32(logsBuffer.Size()),
			Capacity:      int32(logsBuffer.Capacity()),
		})
	}

	return response, nil
}

// queryTraces handles trace queries.
func (s *TelemetryQueryServiceServerImpl) queryTraces(ctx context.Context, req *QueryRequest, processor *telemetrybufferprocessor.TelemetryProcessor, response *QueryResponse) (*QueryResponse, error) {
	buffer := processor.GetTracesBuffer()
	if buffer == nil {
		return nil, status.Error(codes.Unavailable, "traces buffer not available")
	}

	// Get all items from the buffer
	items := buffer.GetAll()

	// Convert start and end times
	var startTime, endTime time.Time
	if req.StartTime != nil {
		startTime = req.StartTime.AsTime()
	}
	if req.EndTime != nil {
		endTime = req.EndTime.AsTime()
	}

	// Filter items based on the request
	var filteredItems []interface{}
	if req.TraceId != "" {
		// Filter by trace ID
		for _, item := range items {
			if span, ok := item.(*telemetrybufferprocessor.SpanItem); ok {
				if hex.EncodeToString(span.TraceID) == req.TraceId {
					filteredItems = append(filteredItems, item)
				}
			}
		}
	} else if req.ServiceName != "" {
		// Filter by service name
		for _, item := range items {
			if span, ok := item.(*telemetrybufferprocessor.SpanItem); ok {
				if span.ServiceName == req.ServiceName {
					filteredItems = append(filteredItems, item)
				}
			}
		}
	} else if !startTime.IsZero() || !endTime.IsZero() {
		// Filter by time range
		for _, item := range items {
			if span, ok := item.(*telemetrybufferprocessor.SpanItem); ok {
				spanTime := time.Unix(0, int64(span.StartTime))
				if (!startTime.IsZero() && spanTime.Before(startTime)) || (!endTime.IsZero() && spanTime.After(endTime)) {
					continue
				}
				filteredItems = append(filteredItems, item)
			}
		}
	} else {
		// No filters, return all items
		filteredItems = items
	}

	// Apply limit if specified
	if req.Limit > 0 && int(req.Limit) < len(filteredItems) {
		filteredItems = filteredItems[:req.Limit]
	}

	// Convert items to response format
	response.Spans = make([]*Span, 0, len(filteredItems))
	for _, item := range filteredItems {
		if span, ok := item.(*telemetrybufferprocessor.SpanItem); ok {
			response.Spans = append(response.Spans, convertSpanToProto(span))
		}
	}

	return response, nil
}

// queryMetrics handles metric queries.
func (s *TelemetryQueryServiceServerImpl) queryMetrics(ctx context.Context, req *QueryRequest, processor *telemetrybufferprocessor.TelemetryProcessor, response *QueryResponse) (*QueryResponse, error) {
	buffer := processor.GetMetricsBuffer()
	if buffer == nil {
		return nil, status.Error(codes.Unavailable, "metrics buffer not available")
	}

	// Get all items from the buffer
	items := buffer.GetAll()

	// Convert start and end times
	var startTime, endTime time.Time
	if req.StartTime != nil {
		startTime = req.StartTime.AsTime()
	}
	if req.EndTime != nil {
		endTime = req.EndTime.AsTime()
	}

	// Filter items based on the request
	var filteredItems []interface{}
	if req.ServiceName != "" {
		// Filter by service name
		for _, item := range items {
			if metric, ok := item.(*telemetrybufferprocessor.MetricItem); ok {
				if metric.ServiceName == req.ServiceName {
					filteredItems = append(filteredItems, item)
				}
			}
		}
	} else if !startTime.IsZero() || !endTime.IsZero() {
		// Filter by time range
		for _, item := range items {
			if metric, ok := item.(*telemetrybufferprocessor.MetricItem); ok {
				metricTime := time.Unix(0, int64(metric.Timestamp))
				if (!startTime.IsZero() && metricTime.Before(startTime)) || (!endTime.IsZero() && metricTime.After(endTime)) {
					continue
				}
				filteredItems = append(filteredItems, item)
			}
		}
	} else {
		// No filters, return all items
		filteredItems = items
	}

	// Apply limit if specified
	if req.Limit > 0 && int(req.Limit) < len(filteredItems) {
		filteredItems = filteredItems[:req.Limit]
	}

	// Convert items to response format
	response.Metrics = make([]*MetricDataPoint, 0, len(filteredItems))
	for _, item := range filteredItems {
		if metric, ok := item.(*telemetrybufferprocessor.MetricItem); ok {
			response.Metrics = append(response.Metrics, convertMetricToProto(metric))
		}
	}

	return response, nil
}

// queryLogs handles log queries.
func (s *TelemetryQueryServiceServerImpl) queryLogs(ctx context.Context, req *QueryRequest, processor *telemetrybufferprocessor.TelemetryProcessor, response *QueryResponse) (*QueryResponse, error) {
	buffer := processor.GetLogsBuffer()
	if buffer == nil {
		return nil, status.Error(codes.Unavailable, "logs buffer not available")
	}

	// Get all items from the buffer
	items := buffer.GetAll()

	// Convert start and end times
	var startTime, endTime time.Time
	if req.StartTime != nil {
		startTime = req.StartTime.AsTime()
	}
	if req.EndTime != nil {
		endTime = req.EndTime.AsTime()
	}

	// Filter items based on the request
	var filteredItems []interface{}
	if req.TraceId != "" {
		// Filter by trace ID
		for _, item := range items {
			if log, ok := item.(*telemetrybufferprocessor.LogItem); ok {
				if hex.EncodeToString(log.TraceID) == req.TraceId {
					filteredItems = append(filteredItems, item)
				}
			}
		}
	} else if req.ServiceName != "" {
		// Filter by service name
		for _, item := range items {
			if log, ok := item.(*telemetrybufferprocessor.LogItem); ok {
				if log.ServiceName == req.ServiceName {
					filteredItems = append(filteredItems, item)
				}
			}
		}
	} else if !startTime.IsZero() || !endTime.IsZero() {
		// Filter by time range
		for _, item := range items {
			if log, ok := item.(*telemetrybufferprocessor.LogItem); ok {
				logTime := time.Unix(0, int64(log.Timestamp))
				if (!startTime.IsZero() && logTime.Before(startTime)) || (!endTime.IsZero() && logTime.After(endTime)) {
					continue
				}
				filteredItems = append(filteredItems, item)
			}
		}
	} else {
		// No filters, return all items
		filteredItems = items
	}

	// Apply limit if specified
	if req.Limit > 0 && int(req.Limit) < len(filteredItems) {
		filteredItems = filteredItems[:req.Limit]
	}

	// Convert items to response format
	response.Logs = make([]*LogRecord, 0, len(filteredItems))
	for _, item := range filteredItems {
		if log, ok := item.(*telemetrybufferprocessor.LogItem); ok {
			response.Logs = append(response.Logs, convertLogToProto(log))
		}
	}

	return response, nil
}

// filterItems filters items based on the provided criteria.
func (s *TelemetryQueryServiceServerImpl) filterItems(items []interface{}, traceID, serviceName string, startTime *timestamppb.Timestamp) []interface{} {
	if traceID == "" && serviceName == "" && startTime == nil {
		return items
	}

	var startTimeVal time.Time
	if startTime != nil {
		startTimeVal = startTime.AsTime()
	}

	var result []interface{}
	for _, item := range items {
		if telemetryItem, ok := item.(telemetrybufferprocessor.TelemetryItem); ok {
			// Check trace ID if specified
			if traceID != "" && telemetryItem.GetTraceID() != traceID {
				continue
			}

			// Check service name if specified
			if serviceName != "" && telemetryItem.GetServiceName() != serviceName {
				continue
			}

			// Check start time if specified
			if !startTimeVal.IsZero() && telemetryItem.GetTimestamp().Before(startTimeVal) {
				continue
			}

			result = append(result, item)
		}
	}

	return result
}

// convertItemsToResponse converts a list of items to a QueryResponse.
func (s *TelemetryQueryServiceServerImpl) convertItemsToResponse(telemetryType TelemetryType, items []interface{}) *QueryResponse {
	response := &QueryResponse{
		Spans:   nil,
		Metrics: nil,
		Logs:    nil,
	}

	// Convert items based on the telemetry type
	switch telemetryType {
	case TelemetryType_TELEMETRY_TYPE_TRACES:
		response.Spans = make([]*Span, 0, len(items))
		for _, item := range items {
			if span, ok := item.(*telemetrybufferprocessor.SpanItem); ok {
				response.Spans = append(response.Spans, convertSpanToProto(span))
			}
		}
	case TelemetryType_TELEMETRY_TYPE_METRICS:
		response.Metrics = make([]*MetricDataPoint, 0, len(items))
		for _, item := range items {
			if metric, ok := item.(*telemetrybufferprocessor.MetricItem); ok {
				response.Metrics = append(response.Metrics, convertMetricToProto(metric))
			}
		}
	case TelemetryType_TELEMETRY_TYPE_LOGS:
		response.Logs = make([]*LogRecord, 0, len(items))
		for _, item := range items {
			if log, ok := item.(*telemetrybufferprocessor.LogItem); ok {
				response.Logs = append(response.Logs, convertLogToProto(log))
			}
		}
	}

	return response
}

// convertSpanToProto converts a SpanItem to a Span proto message.
func convertSpanToProto(span *telemetrybufferprocessor.SpanItem) *Span {
	if span == nil {
		return nil
	}

	return &Span{
		TraceId:       hex.EncodeToString(span.TraceID),
		SpanId:        hex.EncodeToString(span.SpanID),
		ParentSpanId:  hex.EncodeToString(span.ParentSpanID),
		Name:          span.Name,
		Kind:          fmt.Sprintf("%d", span.Kind),
		StartTime:     timestamppb.New(time.Unix(0, int64(span.StartTime))),
		EndTime:       timestamppb.New(time.Unix(0, int64(span.EndTime))),
		DurationNanos: int64(span.EndTime - span.StartTime),
		StatusCode:    fmt.Sprintf("%d", span.StatusCode),
		StatusMessage: span.StatusMessage,
		ServiceName:   span.ServiceName,
		Attributes:    convertAttributesToProto(span.Attributes),
	}
}

// convertLogToProto converts a LogItem to a LogRecord proto message.
func convertLogToProto(log *telemetrybufferprocessor.LogItem) *LogRecord {
	if log == nil {
		return nil
	}

	return &LogRecord{
		TraceId:        hex.EncodeToString(log.TraceID),
		SpanId:         hex.EncodeToString(log.SpanID),
		Timestamp:      timestamppb.New(time.Unix(0, int64(log.Timestamp))),
		SeverityText:   log.SeverityText,
		SeverityNumber: log.SeverityNumber,
		Body:           log.Body,
		ServiceName:    log.ServiceName,
		Attributes:     convertAttributesToProto(log.Attributes),
	}
}

// convertMetricToProto converts a MetricItem to a MetricDataPoint proto message.
func convertMetricToProto(metric *telemetrybufferprocessor.MetricItem) *MetricDataPoint {
	if metric == nil {
		return nil
	}

	result := &MetricDataPoint{
		Name:        metric.Name,
		Description: metric.Description,
		Unit:        metric.Unit,
		Type:        metric.Type,
		Timestamp:   timestamppb.New(time.Unix(0, int64(metric.Timestamp))),
		ServiceName: metric.ServiceName,
		Attributes:  convertAttributesToProto(metric.Attributes),
	}

	// Set the value based on the type
	if metric.Value != nil {
		switch metric.Type {
		case "histogram":
			// For histograms, extract the count, sum, and buckets
			if histData, ok := metric.Value.(map[string]interface{}); ok {
				if count, ok := histData["count"].(uint64); ok {
					result.Value = &MetricDataPoint_Histogram{
						Histogram: &Histogram{
							Count: count,
						},
					}
					if sum, ok := histData["sum"].(float64); ok {
						result.Value.(*MetricDataPoint_Histogram).Histogram.Sum = sum
					}
					if buckets, ok := histData["buckets"].([]interface{}); ok {
						bucketValues := make([]*HistogramBucket, 0, len(buckets))
						for _, b := range buckets {
							if bucket, ok := b.(map[string]interface{}); ok {
								bucketValue := &HistogramBucket{}
								if count, ok := bucket["count"].(uint64); ok {
									bucketValue.Count = count
								}
								if upperBound, ok := bucket["upper_bound"].(float64); ok {
									bucketValue.UpperBound = upperBound
								}
								bucketValues = append(bucketValues, bucketValue)
							}
						}
						result.Value.(*MetricDataPoint_Histogram).Histogram.Buckets = bucketValues
					}
				}
			} else if count, ok := metric.Value.(uint64); ok {
				// Simple count for histogram
				result.Value = &MetricDataPoint_Histogram{
					Histogram: &Histogram{
						Count: count,
					},
				}
			}
		case "gauge", "sum":
			// For gauge and sum, extract the value
			switch v := metric.Value.(type) {
			case int64:
				result.Value = &MetricDataPoint_IntValue{IntValue: v}
			case float64:
				result.Value = &MetricDataPoint_DoubleValue{DoubleValue: v}
			case int:
				result.Value = &MetricDataPoint_IntValue{IntValue: int64(v)}
			case map[string]interface{}:
				// Handle complex values
				if count, ok := v["count"].(uint64); ok {
					result.Value = &MetricDataPoint_Count{Count: &Count{Count: int64(count)}}
				}
			}
		default:
			// Default handling for unknown types
			switch v := metric.Value.(type) {
			case int64:
				result.Value = &MetricDataPoint_IntValue{IntValue: v}
			case float64:
				result.Value = &MetricDataPoint_DoubleValue{DoubleValue: v}
			case int:
				result.Value = &MetricDataPoint_IntValue{IntValue: int64(v)}
			case uint64:
				result.Value = &MetricDataPoint_Count{Count: &Count{Count: int64(v)}}
			case map[string]interface{}:
				// Handle complex values
				if count, ok := v["count"].(uint64); ok {
					result.Value = &MetricDataPoint_Histogram{
						Histogram: &Histogram{
							Count: count,
						},
					}
				}
			}
		}
	}

	return result
}

// convertAttributesToProto converts a map of attributes to a map of AttributeValue proto messages.
func convertAttributesToProto(attrs map[string]interface{}) map[string]*AttributeValue {
	if attrs == nil {
		return nil
	}

	result := make(map[string]*AttributeValue, len(attrs))
	for k, v := range attrs {
		result[k] = convertAttributeValueToProto(v)
	}
	return result
}

// convertAttributeValueToProto converts an attribute value to an AttributeValue proto message.
func convertAttributeValueToProto(value interface{}) *AttributeValue {
	if value == nil {
		return nil
	}

	result := &AttributeValue{}

	switch v := value.(type) {
	case string:
		result.Value = &AttributeValue_StringValue{StringValue: v}
	case int64:
		result.Value = &AttributeValue_IntValue{IntValue: v}
	case int:
		result.Value = &AttributeValue_IntValue{IntValue: int64(v)}
	case float64:
		result.Value = &AttributeValue_DoubleValue{DoubleValue: v}
	case bool:
		result.Value = &AttributeValue_BoolValue{BoolValue: v}
	case []byte:
		result.Value = &AttributeValue_BytesValue{BytesValue: v}
	case []interface{}:
		arrayValue := &ArrayValue{
			Values: make([]*AttributeValue, 0, len(v)),
		}
		for _, item := range v {
			arrayValue.Values = append(arrayValue.Values, convertAttributeValueToProto(item))
		}
		result.Value = &AttributeValue_ArrayValue{ArrayValue: arrayValue}
	case map[string]interface{}:
		kvList := &KeyValueList{
			Values: make([]*KeyValue, 0, len(v)),
		}
		for k, val := range v {
			kvList.Values = append(kvList.Values, &KeyValue{
				Key:   k,
				Value: convertAttributeValueToProto(val),
			})
		}
		result.Value = &AttributeValue_KvlistValue{KvlistValue: kvList}
	default:
		// Default to string representation
		result.Value = &AttributeValue_StringValue{StringValue: fmt.Sprintf("%v", v)}
	}

	return result
}
