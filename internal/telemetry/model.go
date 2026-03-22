package telemetry

type RocketTelemetry struct {
	RocketID string  `json:"rocket_id"`
	Velocity float64 `json:"velocity"`
	Altitude float64 `json:"altitude"`
}
