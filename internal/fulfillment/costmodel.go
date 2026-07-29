package fulfillment

import (
	"math"
)

type CostModelConfig struct {
	BaseFeePaise       int64   `json:"base_fee_paise"`        // Fixed 2nd leg delivery cost: ₹25.00 (2500 Paise)
	PerKmRatePaise     int64   `json:"per_km_rate_paise"`     // Per-km transit cost: ₹10.00/km (1000 Paise/km)
	AcceptableDelayMin float64 `json:"acceptable_delay_min"` // SLA grace wait duration: 1.0 minute
	PenaltyPerMinPaise int64   `json:"penalty_per_min_paise"` // Churn/voucher penalty: ₹15.00/min (1500 Paise/min)
}

func DefaultCostModelConfig() CostModelConfig {
	return CostModelConfig{
		BaseFeePaise:       2500, // ₹25.00
		PerKmRatePaise:     1000, // ₹10.00/km
		AcceptableDelayMin: 1.0,  // 1.0 min
		PenaltyPerMinPaise: 1500, // ₹15.00/min
	}
}

// ComputeReRouteCost calculates the second delivery leg cost as a function of haversine distance.
// reRouteCost = BaseFee + (PerKmRate * distKm)
func ComputeReRouteCost(cfg CostModelConfig, distKm float64) int64 {
	if distKm < 0 {
		distKm = 0
	}
	costFloat := float64(cfg.BaseFeePaise) + (float64(cfg.PerKmRatePaise) * distKm)
	return int64(math.Round(costFloat))
}

// ComputeSLABreachPenalty calculates the customer delay penalty as a function of predicted queue wait delay.
// slaBreachPenalty = max(0, predictedDelayMin - AcceptableDelayMin) * PenaltyPerMin
func ComputeSLABreachPenalty(cfg CostModelConfig, predictedDelayMin float64) int64 {
	if predictedDelayMin <= cfg.AcceptableDelayMin {
		return 0
	}
	excessDelayMin := predictedDelayMin - cfg.AcceptableDelayMin
	penaltyFloat := excessDelayMin * float64(cfg.PenaltyPerMinPaise)
	return int64(math.Round(penaltyFloat))
}

// EvaluateReRouteCostGate evaluates whether re-routing is financially justified.
// Re-route occurs ONLY IF reRouteCost < slaBreachPenalty.
func EvaluateReRouteCostGate(cfg CostModelConfig, distKm, predictedDelayMin float64) (bool, int64, int64) {
	reRouteCost := ComputeReRouteCost(cfg, distKm)
	slaPenalty := ComputeSLABreachPenalty(cfg, predictedDelayMin)
	shouldReRoute := reRouteCost < slaPenalty
	return shouldReRoute, reRouteCost, slaPenalty
}
