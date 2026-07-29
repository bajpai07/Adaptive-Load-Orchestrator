package simulation

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestPoissonArrivalGenerator_StatisticalProperties(t *testing.T) {
	gen := NewRandomGenerator(42)
	lambda := 10.0 // 10 arrivals per second -> expected mean inter-arrival time = 0.1s
	samples := 100000

	var totalDuration time.Duration
	for i := 0; i < samples; i++ {
		totalDuration += gen.NextInterArrivalTime(lambda)
	}

	avgSec := totalDuration.Seconds() / float64(samples)
	expectedAvgSec := 1.0 / lambda

	// Verify mean inter-arrival time is within 2% error tolerance
	assert.InDelta(t, expectedAvgSec, avgSec, expectedAvgSec*0.02,
		"Mean inter-arrival time should match 1/lambda within 2% tolerance")
}

func TestExponentialServiceGenerator_StatisticalProperties(t *testing.T) {
	gen := NewRandomGenerator(123)
	mu := 0.05 // 0.05 services per second -> expected mean service time = 20s
	samples := 100000

	var totalDuration time.Duration
	for i := 0; i < samples; i++ {
		totalDuration += gen.NextServiceTime(mu)
	}

	avgSec := totalDuration.Seconds() / float64(samples)
	expectedAvgSec := 1.0 / mu

	// Verify mean service time is within 2% error tolerance
	assert.InDelta(t, expectedAvgSec, avgSec, expectedAvgSec*0.02,
		"Mean service time should match 1/mu within 2% tolerance")
}

func TestHaversineDistance(t *testing.T) {
	// Connaught Place (28.6315, 77.2167) to India Gate (28.6129, 77.2295) ~2.46 km
	lat1, lng1 := 28.6315, 77.2167
	lat2, lng2 := 28.6129, 77.2295

	dist := HaversineDistance(lat1, lng1, lat2, lng2)
	assert.InDelta(t, 2.46, dist, 0.1, "Distance should be approx 2.46 km")

	// Same coordinates -> 0 km
	distSame := HaversineDistance(lat1, lng1, lat1, lng1)
	assert.Equal(t, 0.0, distSame)
}
