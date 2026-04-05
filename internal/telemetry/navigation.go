package telemetry

import (
	"context"
	"encoding/json"
	"log"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go"
	"github.com/segmentio/kafka-go"
)

type NavigationTelemetryWatcher struct {
	*TelemetryWatcher
}

func NewNavigationTelemetryWatcher(ctx context.Context, reader *kafka.Reader, influxClient influxdb2.Client, orgName string, bucketName string) *NavigationTelemetryWatcher {
	return &NavigationTelemetryWatcher{
		TelemetryWatcher: NewTelemetryWatcher(ctx, reader, influxClient, orgName, bucketName),
	}
}

func (ntw *NavigationTelemetryWatcher) Start(ctx context.Context) error {
	influxWriterApi, err := CreateInfluxWriteAPIBlocking(ctx, ntw.influxClient, ntw.orgName, ntw.bucketName)
	if err != nil {
		return err
	}

	log.Println("Starting NavigationTelemetryWatcher...")

	for {
		select {
		case <-ctx.Done():
			log.Println("Shutting down NavigationTelemetryWatcher...")
			return ctx.Err()
		default:
			msg, err := ntw.reader.FetchMessage(ctx)
			if err != nil {
				// If the error is because the context was cancelled, exit the loop normally
				if ctx.Err() != nil {
					log.Println("Shutting down NavigationTelemetryWatcher...")
					return ctx.Err()
				}

				log.Printf("Error fetching message: %v", err)
				continue
			}

			var navigationData Navigation
			unmarshalErr := json.Unmarshal(msg.Value, &navigationData)
			if unmarshalErr != nil {
				log.Printf("Error unmarshalling message: %v", unmarshalErr)
				continue
			}

			point := influxdb2.NewPoint("navigation",
				map[string]string{"rocket_id": string(msg.Key)},
				map[string]interface{}{
					"velocity": navigationData.Velocity,
					"altitude": navigationData.Altitude,
					"pitch":    navigationData.Pitch,
					"yaw":      navigationData.Yaw,
					"roll":     navigationData.Roll,
				},
				time.Now(),
			)

			writeErr := influxWriterApi.WritePoint(ctx, point)
			if writeErr != nil {
				log.Printf("Error writing to InfluxDB: %v", writeErr)
				continue
			}

			if err := ntw.reader.CommitMessages(ctx, msg); err != nil {
				log.Printf("Failed to commit message: %v", err)
				continue
			}

			log.Println("Navigation message processed")
		}
	}
}
