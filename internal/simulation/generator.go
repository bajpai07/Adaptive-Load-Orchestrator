package simulation

import (
	"math"
	"math/rand"
	"sync"
	"time"
)

// RandomGenerator provides thread-safe exponential random distribution generators.
type RandomGenerator struct {
	mu   sync.Mutex
	rand *rand.Rand
}

// NewRandomGenerator creates a new RandomGenerator initialized with a seed.
func NewRandomGenerator(seed int64) *RandomGenerator {
	return &RandomGenerator{
		rand: rand.New(rand.NewSource(seed)),
	}
}

// NextInterArrivalTime returns the duration until the next arrival for a Poisson process with rate lambda (arrivals per second).
// Formula: dt = -ln(U) / lambda
func (g *RandomGenerator) NextInterArrivalTime(lambda float64) time.Duration {
	if lambda <= 0 {
		return time.Hour // effectively infinite delay
	}
	g.mu.Lock()
	u := g.rand.Float64()
	g.mu.Unlock()

	// Prevent log(0)
	for u == 0 {
		g.mu.Lock()
		u = g.rand.Float64()
		g.mu.Unlock()
	}

	dtSec := -math.Log(u) / lambda
	return time.Duration(dtSec * float64(time.Second))
}

// NextServiceTime returns the pick service duration for exponential distribution with rate mu (services per second).
// Formula: st = -ln(U) / mu
func (g *RandomGenerator) NextServiceTime(mu float64) time.Duration {
	if mu <= 0 {
		return time.Hour
	}
	g.mu.Lock()
	u := g.rand.Float64()
	g.mu.Unlock()

	for u == 0 {
		g.mu.Lock()
		u = g.rand.Float64()
		g.mu.Unlock()
	}

	stSec := -math.Log(u) / mu
	return time.Duration(stSec * float64(time.Second))
}
