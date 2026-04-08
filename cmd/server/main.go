package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/coeusj/rock-tel/internal/telemetry"
	"github.com/joho/godotenv"
	"github.com/segmentio/kafka-go"
)

func main() {
	godotenv.Load("../../dev.env")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	influxClient, err := telemetry.ConnectToInfluxDB(ctx)
	if err != nil {
		log.Println("ERROR: Could not connect to InfluxDB")
		os.Exit(1)
	}
	defer influxClient.Close()

	bucketName := os.Getenv("INFLUXDB_BUCKET_ROCKET_TELEMETRY")
	orgName := os.Getenv("INFLUXDB_ORG_ROCKTEL")

	// Channel to collect startup errors
	telStartErrChan := make(chan error, 2)

	navReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{os.Getenv("KAFKA_BROKER")},
		GroupID:  os.Getenv("KAFKA_GROUP_ROCKET_TELEMETRY"),
		Topic:    os.Getenv("KAFKA_TOPIC_NAVIGATION"),
		MaxWait:  500 * time.Millisecond, // How long to wait for new data before polling
		MinBytes: 10e3,                   // 10KB - batching for efficiency
		MaxBytes: 10e6,                   // 10MB
	})
	navTelemetryWatcher := telemetry.NewNavigationTelemetryWatcher(ctx, navReader, influxClient, orgName, bucketName)

	go func() {
		err := navTelemetryWatcher.Start(ctx)
		if err != nil {
			telStartErrChan <- err
		}
	}()
	defer navTelemetryWatcher.Stop()

	propulsionReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{os.Getenv("KAFKA_BROKER")},
		GroupID:  os.Getenv("KAFKA_GROUP_ROCKET_TELEMETRY"),
		Topic:    os.Getenv("KAFKA_TOPIC_PROPULSION"),
		MaxWait:  500 * time.Millisecond, // How long to wait for new data before polling
		MinBytes: 10e3,                   // 10KB - batching for efficiency
		MaxBytes: 10e6,                   // 10MB
	})
	propulsionTelemetryWatcher := telemetry.NewPropulsionTelemetryWatcher(ctx, propulsionReader, influxClient, orgName, bucketName)

	go func() {
		err := propulsionTelemetryWatcher.Start(ctx)
		if err != nil {
			telStartErrChan <- err
		}
	}()
	defer propulsionTelemetryWatcher.Stop()

	// Check for telemetry start errors
	select {
	case err := <-telStartErrChan:
		log.Printf("Watcher startup error: %v", err)
		return
	default:
		// No errors, continue
	}

	// Wait for shutdown signal
	<-ctx.Done()
	log.Println("Shutting down server...")
}
