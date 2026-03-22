package telemetry

import (
	"os"
	"time"

	"github.com/joho/godotenv"
	"github.com/segmentio/kafka-go"
)

type RocketTelemetryReader struct {
	Reader *kafka.Reader
}

func NewRocketTelemetryReader() *RocketTelemetryReader {
	godotenv.Load("../../test_kafka.env")
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{os.Getenv("KAFKA_BROKER")},
		GroupID:  os.Getenv("KAFKA_GROUP_ROCKET_TELEMETRY"),
		Topic:    os.Getenv("KAFKA_TOPIC_ROCKET_TELEMETRY"),
		MaxWait:  500 * time.Millisecond, // How long to wait for new data before polling
		MinBytes: 10e3,                   // 10KB - batching for efficiency
		MaxBytes: 10e6,                   // 10MB
	})

	return &RocketTelemetryReader{
		Reader: reader,
	}
}
