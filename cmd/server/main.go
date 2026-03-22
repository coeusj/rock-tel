package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/coeusj/rock-tel/internal/telemetry"
	influxdb2 "github.com/influxdata/influxdb-client-go"
	"github.com/joho/godotenv"
)

func main() {
	godotenv.Load("../../test_influxdb.env")

	// Create a context that is cancelled when the OS sends an interrupt signal
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	influxClient, err := telemetry.ConnectToInfluxDB(ctx)
	if err != nil {
		log.Println("ERROR: Could not connect to InfluxDB")
		os.Exit(1)
	}

	bucketName := os.Getenv("INFLUXDB_BUCKET_ROCKET_TELEMETRY")
	orgName := os.Getenv("INFLUXDB_ORG_ROCKTEL")
	writeApi, err := telemetry.CreateWriteAPIBlocking(ctx, influxClient, orgName, bucketName)
	if err != nil {
		os.Exit(1)
	}

	consumer := telemetry.NewRocketTelemetryReader()
	defer consumer.Reader.Close()

	log.Println("Listening messages..")

	for {
		// 1. Fetch the message
		// FetchMessage is better than ReadMessage for manual offset control
		msg, err := consumer.Reader.FetchMessage(ctx)
		if err != nil {
			// If the error is because the context was cancelled, exit the loop normally
			if ctx.Err() != nil {
				log.Println("Shutdown signal received, exiting loop...")
				break
			}

			log.Fatalf("Error while fetching message: %v", err)
			continue
		}

		// 2. Process data (Your Logic Here)
		var telemetryMsg telemetry.RocketTelemetry
		unmarshalErr := json.Unmarshal(msg.Value, &telemetryMsg)
		if unmarshalErr != nil {
			log.Fatalf("Error while trying to unmarshal message: %v", err)
			continue
		}

		point := influxdb2.NewPoint("rocket-telemetry",
			map[string]string{"rocket_id": string(msg.Key)},
			map[string]interface{}{"velocity": telemetryMsg.Velocity, "Altitude": telemetryMsg.Altitude},
			time.Now())

		// 4. Write processed message to InfluxDB
		writeErr := writeApi.WritePoint(ctx, point)
		if writeErr != nil {
			log.Printf("Error while trying to write to Influx: %v", writeErr)
			continue
		}

		// 5. COMMIT the offset
		// This tells Kafka that the message was processed successfully
		if err := consumer.Reader.CommitMessages(ctx, msg); err != nil {
			log.Printf("Failed to commit message: %v", err)
		}

		log.Println("Message processed")
	}
}
