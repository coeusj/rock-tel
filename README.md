# Rocket Telemetry Reader

Read telemetry from Kafka and push it into InfluxDB

InfluxDB Container - compose and run container:

```bash
docker-compose --env-file test_influxdb.env up
```

Run the application:

```bash
go run ./cmd/server/main.go
```

Run tests:

```bash
go test -v -tags=integration ./tests/integration/...
```
