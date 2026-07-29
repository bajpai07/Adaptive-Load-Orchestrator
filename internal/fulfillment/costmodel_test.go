package fulfillment

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCostModel_FormulasAndBoundaryConditions(t *testing.T) {
	cfg := DefaultCostModelConfig() // Base: ₹25.00 (2500p), PerKm: ₹10.00 (1000p), Acceptable: 1.0m, Penalty: ₹15.00/m (1500p)

	// Test reRouteCost calculation
	assert.Equal(t, int64(2500), ComputeReRouteCost(cfg, 0.0), "0km distance cost must equal BaseFee (₹25.00)")
	assert.Equal(t, int64(4000), ComputeReRouteCost(cfg, 1.5), "1.5km distance cost must equal 2500 + 1500 = ₹40.00")
	assert.Equal(t, int64(7500), ComputeReRouteCost(cfg, 5.0), "5.0km distance cost must equal 2500 + 5000 = ₹75.00")

	// Test slaBreachPenalty calculation
	assert.Equal(t, int64(0), ComputeSLABreachPenalty(cfg, 0.5), "Delay <= 1.0 min must yield 0 penalty")
	assert.Equal(t, int64(0), ComputeSLABreachPenalty(cfg, 1.0), "Delay == 1.0 min must yield 0 penalty")
	assert.Equal(t, int64(1500), ComputeSLABreachPenalty(cfg, 2.0), "Delay 2.0 min must yield (2.0 - 1.0) * 1500 = ₹15.00")
	assert.Equal(t, int64(4500), ComputeSLABreachPenalty(cfg, 4.0), "Delay 4.0 min must yield (4.0 - 1.0) * 1500 = ₹45.00")
}

func TestCostModel_ConstructedScenario_CostGateBlocksReRoute(t *testing.T) {
	cfg := DefaultCostModelConfig()

	// Scenario A: Short delay (1.8 min), distance 1.2km
	// reRouteCost = 2500 + 1200 = 3700 Paise (₹37.00)
	// slaPenalty = (1.8 - 1.0) * 1500 = 1200 Paise (₹12.00)
	// reRouteCost (3700) >= slaPenalty (1200) -> SHOULD BE BLOCKED BY COST GATE!
	shouldReRoute, cost, penalty := EvaluateReRouteCostGate(cfg, 1.2, 1.8)
	assert.False(t, shouldReRoute, "Cost gate must BLOCK re-route when delivery leg cost (₹37.00) exceeds SLA penalty (₹12.00)")
	assert.Equal(t, int64(3700), cost)
	assert.Equal(t, int64(1200), penalty)

	// Scenario B: Severe delay (5.0 min), distance 1.2km
	// reRouteCost = 2500 + 1200 = 3700 Paise (₹37.00)
	// slaPenalty = (5.0 - 1.0) * 1500 = 6000 Paise (₹60.00)
	// reRouteCost (3700) < slaPenalty (6000) -> SHOULD BE ALLOWED!
	shouldReRouteB, costB, penaltyB := EvaluateReRouteCostGate(cfg, 1.2, 5.0)
	assert.True(t, shouldReRouteB, "Cost gate must ALLOW re-route when SLA penalty (₹60.00) exceeds delivery leg cost (₹37.00)")
	assert.Equal(t, int64(3700), costB)
	assert.Equal(t, int64(6000), penaltyB)
}
