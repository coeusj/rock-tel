package telemetry

import (
	"context"
	"encoding/json"
	"log"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go"
	"github.com/segmentio/kafka-go"
)

type PropulsionTelemetryWatcher struct {
	*TelemetryWatcher
}

func NewPropulsionTelemetryWatcher(ctx context.Context, reader *kafka.Reader, influxClient influxdb2.Client, orgName string, bucketName string) *PropulsionTelemetryWatcher {
	return &PropulsionTelemetryWatcher{
		TelemetryWatcher: NewTelemetryWatcher(ctx, reader, influxClient, orgName, bucketName),
	}
}

func (ptw *PropulsionTelemetryWatcher) Start(ctx context.Context) error {
	influxWriterApi, err := CreateInfluxWriteAPIBlocking(ctx, ptw.influxClient, ptw.orgName, ptw.bucketName)
	if err != nil {
		return err
	}

	log.Println("Starting PropulsionTelemetryWatcher...")

	for {
		select {
		case <-ctx.Done():
			log.Println("Shutting down PropulsionTelemetryWatcher...")
			return ctx.Err()
		default:
			msg, err := ptw.reader.FetchMessage(ctx)
			if err != nil {
				// If the error is because the context was cancelled, exit the loop normally
				if ctx.Err() != nil {
					log.Println("Shutting down PropulsionTelemetryWatcher...")
					return ctx.Err()
				}

				log.Printf("Error fetching message: %v", err)
				continue
			}

			var propulsionData Propulsion
			unmarshalErr := json.Unmarshal(msg.Value, &propulsionData)
			if unmarshalErr != nil {
				log.Printf("Error unmarshalling message: %v", unmarshalErr)
				continue
			}

			point := influxdb2.NewPoint("propulsion",
				map[string]string{"rocket_id": string(msg.Key)},
				map[string]interface{}{
					"fuel_perc": propulsionData.FuelPerc,
				},
				time.Now(),
			)

			writeErr := influxWriterApi.WritePoint(ctx, point)
			if writeErr != nil {
				log.Printf("Error writing point: %v", writeErr)
				continue
			}

			commitErr := ptw.reader.CommitMessages(ctx, msg)
			if commitErr != nil {
				log.Printf("Error committing message: %v", commitErr)
				continue
			}

			log.Println("Propulsion message processed")
		}
	}
}
