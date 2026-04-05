package telemetry

import (
	"context"
	"errors"
	"log"
	"os"

	influxdb2 "github.com/influxdata/influxdb-client-go"
	"github.com/influxdata/influxdb-client-go/api"
	"github.com/joho/godotenv"
)

func ConnectToInfluxDB(ctx context.Context) (influxdb2.Client, error) {
	godotenv.Load("../../test_influxdb.env")

	token := os.Getenv("INFLUXDB_TOKEN")
	if token == "" {
		return nil, errors.New("INFLUXDB_TOKEN not found!")
	}

	url := os.Getenv("INFLUXDB_URL")
	if url == "" {
		return nil, errors.New("INFLUXDB_URL not found!")
	}

	client := influxdb2.NewClient(url, token)
	_, err := client.Health(ctx)

	//TODO: I should probably check the health

	return client, err
}

func CreateInfluxWriteAPIBlocking(ctx context.Context, client influxdb2.Client, orgName string, bucketName string) (api.WriteAPIBlocking, error) {
	bucketsApi := client.BucketsAPI()
	_, findBucketErr := bucketsApi.FindBucketByName(ctx, bucketName)
	if findBucketErr != nil {
		log.Printf("WARN: Could not find Bucket '%s'. Trying to create it\n", bucketName)

		// Find Org
		rockTelOrg, orgErr := client.OrganizationsAPI().FindOrganizationByName(ctx, orgName)
		if orgErr != nil {
			log.Printf("WARN: Could not find Org '%s'. Trying to create it\n ", orgName)

			// Create Org
			_, createOrgErr := client.OrganizationsAPI().CreateOrganizationWithName(ctx, orgName)
			if createOrgErr != nil {
				log.Println("ERROR: Could not create Org")
				return nil, createOrgErr
			}
		}

		// Create Bucket
		_, createBucketErr := bucketsApi.CreateBucketWithNameWithID(ctx, *rockTelOrg.Id, bucketName)
		if createBucketErr != nil {
			return nil, createBucketErr
		}
	}

	return client.WriteAPIBlocking(orgName, bucketName), nil
}
