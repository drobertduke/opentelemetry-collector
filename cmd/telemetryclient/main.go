package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "telemetrycollector/internal/telemetryqueryextension"
)

func main() {
	// Connect to the server
	conn, err := grpc.Dial("localhost:4320", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	// Create a client
	client := pb.NewTelemetryQueryServiceClient(conn)

	// Create a request
	req := &pb.QueryRequest{
		TelemetryType: pb.TelemetryType_TELEMETRY_TYPE_METRICS,
	}

	// Send the request
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := client.Query(ctx, req)
	if err != nil {
		log.Fatalf("Failed to query: %v", err)
	}

	// Print the response
	fmt.Printf("Received %d metrics\n", len(resp.Metrics))
	for i, metric := range resp.Metrics {
		fmt.Printf("Metric %d: %s\n", i, metric.Name)
		fmt.Printf("  Description: %s\n", metric.Description)
		fmt.Printf("  Unit: %s\n", metric.Unit)
		fmt.Printf("  Type: %s\n", metric.Type)
		fmt.Printf("  Timestamp: %s\n", metric.Timestamp.AsTime())
		fmt.Printf("  Service: %s\n", metric.ServiceName)

		// Print the value based on the type
		switch v := metric.Value.(type) {
		case *pb.MetricDataPoint_IntValue:
			fmt.Printf("  Value: %d\n", v.IntValue)
		case *pb.MetricDataPoint_DoubleValue:
			fmt.Printf("  Value: %f\n", v.DoubleValue)
		case *pb.MetricDataPoint_Count:
			fmt.Printf("  Count: %d\n", v.Count.Count)
		default:
			fmt.Printf("  Value: unknown\n")
		}
		fmt.Println()
	}
}
