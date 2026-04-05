package telemetry

import (
	"context"
	"errors"

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
	return errors.New("Start method not implemented for TelemetryWatcher")
}

func (tw *TelemetryWatcher) Stop() error {
	return tw.reader.Close()
}
