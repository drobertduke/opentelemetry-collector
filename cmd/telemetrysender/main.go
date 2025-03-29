package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	metricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricpb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
)

func main() {
	// Connect to the server
	conn, err := grpc.Dial("localhost:4327", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	// Create a client
	client := metricspb.NewMetricsServiceClient(conn)

	// Create a request with some test metrics
	req := &metricspb.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricpb.ResourceMetrics{
			{
				Resource: &resourcepb.Resource{
					Attributes: []*commonpb.KeyValue{
						{
							Key: "service.name",
							Value: &commonpb.AnyValue{
								Value: &commonpb.AnyValue_StringValue{
									StringValue: "test-service",
								},
							},
						},
					},
				},
				ScopeMetrics: []*metricpb.ScopeMetrics{
					{
						Scope: &commonpb.InstrumentationScope{
							Name:    "test-scope",
							Version: "1.0.0",
						},
						Metrics: []*metricpb.Metric{
							{
								Name:        "test.counter",
								Description: "A test counter",
								Unit:        "1",
								Data: &metricpb.Metric_Gauge{
									Gauge: &metricpb.Gauge{
										DataPoints: []*metricpb.NumberDataPoint{
											{
												TimeUnixNano: uint64(time.Now().UnixNano()),
												Value: &metricpb.NumberDataPoint_AsDouble{
													AsDouble: 42.0,
												},
												Attributes: []*commonpb.KeyValue{
													{
														Key: "test.attribute",
														Value: &commonpb.AnyValue{
															Value: &commonpb.AnyValue_StringValue{
																StringValue: "test-value",
															},
														},
													},
												},
											},
										},
									},
								},
							},
							{
								Name:        "test.gauge",
								Description: "A test gauge",
								Unit:        "bytes",
								Data: &metricpb.Metric_Gauge{
									Gauge: &metricpb.Gauge{
										DataPoints: []*metricpb.NumberDataPoint{
											{
												TimeUnixNano: uint64(time.Now().UnixNano()),
												Value: &metricpb.NumberDataPoint_AsInt{
													AsInt: 1024,
												},
												Attributes: []*commonpb.KeyValue{
													{
														Key: "test.attribute",
														Value: &commonpb.AnyValue{
															Value: &commonpb.AnyValue_StringValue{
																StringValue: "test-value",
															},
														},
													},
												},
											},
										},
									},
								},
							},
							{
								Name:        "test.sum",
								Description: "A test sum",
								Unit:        "requests",
								Data: &metricpb.Metric_Sum{
									Sum: &metricpb.Sum{
										DataPoints: []*metricpb.NumberDataPoint{
											{
												TimeUnixNano: uint64(time.Now().UnixNano()),
												Value: &metricpb.NumberDataPoint_AsInt{
													AsInt: 100,
												},
												Attributes: []*commonpb.KeyValue{
													{
														Key: "test.attribute",
														Value: &commonpb.AnyValue{
															Value: &commonpb.AnyValue_StringValue{
																StringValue: "test-value",
															},
														},
													},
												},
											},
										},
										AggregationTemporality: metricpb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE,
										IsMonotonic:            true,
									},
								},
							},
						},
					},
				},
			},
		},
	}

	// Send the request
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := client.Export(ctx, req)
	if err != nil {
		log.Fatalf("Failed to export metrics: %v", err)
	}

	fmt.Printf("Metrics exported successfully: %v\n", resp)
}
