package telemetry

import (
	"context"
	"log"

	influxdb2 "github.com/influxdata/influxdb-client-go"
	"github.com/segmentio/kafka-go"
)

type Watcher interface {
	Start(ctx context.Context) error
	Stop() error
}

type TelemetryWatcher struct {
	reader       *kafka.Reader
	influxClient influxdb2.Client
	orgName      string
	bucketName   string
}

func NewTelemetryWatcher(ctx context.Context, reader *kafka.Reader, influxClient influxdb2.Client, orgName string, bucketName string) *TelemetryWatcher {
	return &TelemetryWatcher{
		reader:       reader,
		influxClient: influxClient,
		orgName:      orgName,
		bucketName:   bucketName,
	}
}

func (tw *TelemetryWatcher) Start(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			msg, err := tw.reader.FetchMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}

				return err
			}

			log.Printf("Received message: %v", msg)
			return nil
		}
	}
}

func (tw *TelemetryWatcher) Stop() error {
	return tw.reader.Close()
}
