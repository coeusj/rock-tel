# InfluxDB Setup

CREATE CONTAINER: compose and run InfluxDB container
> docker-compose --env-file test_influxdb.env up

RUN TEST:
> go test -v -tags=integration ./tests/integration/...