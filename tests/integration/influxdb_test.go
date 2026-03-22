//go:build integration

package influxdb_test

import (
	"context"
	"os"
	"testing"

	influxdb2 "github.com/influxdata/influxdb-client-go"
	"github.com/joho/godotenv"
)

func Test_connectToInfluxDB(t *testing.T) {
	// 1. Setup
	godotenv.Load("../../test_influxdb.env")
	url := os.Getenv("INFLUXDB_URL")
	token := os.Getenv("INFLUXDB_TOKEN")

	// 2. Initialize client
	client := influxdb2.NewClient(url, token)
	defer client.Close()

	// 3. Perform Action
	_, err := client.Health(context.Background())

	// 4. Assert
	if err != nil {
		t.Fatalf("Failed to write to InfluxDB: %v", err)
	}
}
