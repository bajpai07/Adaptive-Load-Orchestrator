package fulfillment

import (
	"context"
	"log"
	"sync"
	"sync/atomic"

	"adaptive-load-orchestrator/internal/simulation"
)

type ReRouteDecisionOutcome string

const (
	OutcomeNoAction              ReRouteDecisionOutcome = "NO_ACTION"
	OutcomeBatchingNudgeIssued   ReRouteDecisionOutcome = "BATCHING_NUDGE_ISSUED"
	OutcomeReRouteRejectedOnCost ReRouteDecisionOutcome = "RE_ROUTE_REJECTED_ON_COST"
	OutcomeReRouteExecuted       ReRouteDecisionOutcome = "RE_ROUTE_EXECUTED"
	OutcomeReRouteFailedNoStock  ReRouteDecisionOutcome = "RE_ROUTE_FAILED_NO_STOCK"
)

type ReRouteDecisionEvent struct {
	Outcome           ReRouteDecisionOutcome `json:"outcome"`
	SourceStoreID     string                 `json:"source_store_id"`
	TargetStoreID     string                 `json:"target_store_id,omitempty"`
	OrderID           string                 `json:"order_id,omitempty"`
	DistanceKm        float64                `json:"distance_km"`
	ReRouteCostPaise  int64                  `json:"reroute_cost_paise"`
	SLAPenaltyPaise   int64                  `json:"sla_penalty_paise"`
	PredictedDelayMin float64                `json:"predicted_delay_min"`
	SimTimeNs         int64                  `json:"sim_time_ns"`
}

type ReRouteDecisionSummary struct {
	TotalEvaluations    int64 `json:"total_evaluations"`
	NoActionCount       int64 `json:"no_action_count"`
	RejectedOnCostCount int64 `json:"rejected_on_cost_count"`
	ExecutedCount       int64 `json:"executed_count"`
	FailedNoStockCount  int64 `json:"failed_no_stock_count"`
}

type CostGatedReRouter struct {
	mu               sync.Mutex
	costConfig       CostModelConfig
	reservationStore *StockReservationStore
	costGatedEnabled bool
	cooldownNs       int64
	recoveryCap      float64
	maxDistKm        float64
	summary          ReRouteDecisionSummary
}

func NewCostGatedReRouter(costCfg CostModelConfig, resStore *StockReservationStore, costGatedEnabled bool) *CostGatedReRouter {
	return &CostGatedReRouter{
		costConfig:       costCfg,
		reservationStore: resStore,
		costGatedEnabled: costGatedEnabled,
		cooldownNs:       30 * 1e9, // 30 seconds simulated cooldown
		recoveryCap:      0.70,     // 70% load hysteresis recovery threshold
		maxDistKm:        1.5,      // 1.5km proximity limit
	}
}

func (r *CostGatedReRouter) EvaluateAndReRoute(ctx context.Context, sourceStore *Store, allStores []*Store, simTimeNs int64) *ReRouteDecisionEvent {
	atomic.AddInt64(&r.summary.TotalEvaluations, 1)

	sourceLoad := sourceStore.ComputeLoad()
	if sourceLoad <= 0.85 {
		atomic.AddInt64(&r.summary.NoActionCount, 1)
		return &ReRouteDecisionEvent{
			Outcome:       OutcomeNoAction,
			SourceStoreID: sourceStore.ID,
			SimTimeNs:     simTimeNs,
		}
	}

	// Check 30s simulated cooldown
	lastTime := sourceStore.LastReRouteSimTimeNs()
	if lastTime > 0 && (simTimeNs-lastTime) < r.cooldownNs {
		atomic.AddInt64(&r.summary.NoActionCount, 1)
		return &ReRouteDecisionEvent{
			Outcome:       OutcomeNoAction,
			SourceStoreID: sourceStore.ID,
			SimTimeNs:     simTimeNs,
		}
	}

	// Find best candidate target store (<1.5km distance, load < 70%)
	var bestTarget *Store
	bestDist := r.maxDistKm + 1.0

	for _, s := range allStores {
		if s.ID == sourceStore.ID {
			continue
		}
		targetLoad := s.ComputeLoad()
		if targetLoad < r.recoveryCap {
			dist := simulation.HaversineDistance(sourceStore.Lat, sourceStore.Lng, s.Lat, s.Lng)
			if dist <= r.maxDistKm && dist < bestDist {
				bestDist = dist
				bestTarget = s
			}
		}
	}

	if bestTarget == nil {
		atomic.AddInt64(&r.summary.NoActionCount, 1)
		return &ReRouteDecisionEvent{
			Outcome:       OutcomeNoAction,
			SourceStoreID: sourceStore.ID,
			SimTimeNs:     simTimeNs,
		}
	}

	// Predict queue wait delay (in minutes)
	waitingOrders := sourceStore.WaitingOrdersCount()
	// Average service time ~ 6.68s per order across c=5 pickers -> 1.336s per order network throughput
	predictedDelaySec := float64(waitingOrders) * 1.336
	predictedDelayMin := predictedDelaySec / 60.0

	shouldReRoute, reRouteCost, slaPenalty := EvaluateReRouteCostGate(r.costConfig, bestDist, predictedDelayMin)

	// Cost Gate Check
	if r.costGatedEnabled && !shouldReRoute {
		atomic.AddInt64(&r.summary.RejectedOnCostCount, 1)
		log.Printf("[DECISION] SimTime: %.1fs | Store: %s -> %s | COST GATE BLOCKED | Cost: ₹%.2f >= SLA Penalty: ₹%.2f (Delay: %.2f min)",
			float64(simTimeNs)/1e9, sourceStore.ID, bestTarget.ID, float64(reRouteCost)/100.0, float64(slaPenalty)/100.0, predictedDelayMin)
		return &ReRouteDecisionEvent{
			Outcome:           OutcomeReRouteRejectedOnCost,
			SourceStoreID:     sourceStore.ID,
			TargetStoreID:     bestTarget.ID,
			DistanceKm:        bestDist,
			ReRouteCostPaise:  reRouteCost,
			SLAPenaltyPaise:   slaPenalty,
			PredictedDelayMin: predictedDelayMin,
			SimTimeNs:         simTimeNs,
		}
	}

	// Extract non-perishable order from source queue
	extractedOrders := sourceStore.Queue().ExtractNonPerishables(1)
	if len(extractedOrders) == 0 {
		atomic.AddInt64(&r.summary.NoActionCount, 1)
		return &ReRouteDecisionEvent{
			Outcome:       OutcomeNoAction,
			SourceStoreID: sourceStore.ID,
			SimTimeNs:     simTimeNs,
		}
	}

	orderToReRoute := extractedOrders[0]

	// Stock Reservation Check
	if r.reservationStore != nil {
		reserved, _, err := r.reservationStore.ReserveStockAtomic(ctx, bestTarget.ID, "SKU-GENERIC", 1)
		if err != nil || !reserved {
			// Reservation failed: Return order to source queue
			sourceStore.Queue().Push(orderToReRoute)
			atomic.AddInt64(&r.summary.FailedNoStockCount, 1)
			log.Printf("[DECISION] SimTime: %.1fs | Store: %s -> %s | STOCK UNAVAILABLE | Order: %s returned to source queue",
				float64(simTimeNs)/1e9, sourceStore.ID, bestTarget.ID, orderToReRoute.ID)
			return &ReRouteDecisionEvent{
				Outcome:           OutcomeReRouteFailedNoStock,
				SourceStoreID:     sourceStore.ID,
				TargetStoreID:     bestTarget.ID,
				OrderID:           orderToReRoute.ID,
				DistanceKm:        bestDist,
				ReRouteCostPaise:  reRouteCost,
				SLAPenaltyPaise:   slaPenalty,
				PredictedDelayMin: predictedDelayMin,
				SimTimeNs:         simTimeNs,
			}
		}
	}

	// Execute Re-Route
	sourceStore.SetLastReRouteSimTimeNs(simTimeNs)
	sourceStore.IncrReRoutedOut()
	bestTarget.IncrReRoutedIn()

	bestTarget.EnqueueOrder(orderToReRoute)
	atomic.AddInt64(&r.summary.ExecutedCount, 1)

	log.Printf("[DECISION] SimTime: %.1fs | Store: %s -> %s | RE-ROUTE EXECUTED | Cost: ₹%.2f < SLA Penalty: ₹%.2f (Delay: %.2f min) | Order: %s",
		float64(simTimeNs)/1e9, sourceStore.ID, bestTarget.ID, float64(reRouteCost)/100.0, float64(slaPenalty)/100.0, predictedDelayMin, orderToReRoute.ID)

	return &ReRouteDecisionEvent{
		Outcome:           OutcomeReRouteExecuted,
		SourceStoreID:     sourceStore.ID,
		TargetStoreID:     bestTarget.ID,
		OrderID:           orderToReRoute.ID,
		DistanceKm:        bestDist,
		ReRouteCostPaise:  reRouteCost,
		SLAPenaltyPaise:   slaPenalty,
		PredictedDelayMin: predictedDelayMin,
		SimTimeNs:         simTimeNs,
	}
}

func (r *CostGatedReRouter) GetSummary() ReRouteDecisionSummary {
	return ReRouteDecisionSummary{
		TotalEvaluations:    atomic.LoadInt64(&r.summary.TotalEvaluations),
		NoActionCount:       atomic.LoadInt64(&r.summary.NoActionCount),
		RejectedOnCostCount: atomic.LoadInt64(&r.summary.RejectedOnCostCount),
		ExecutedCount:       atomic.LoadInt64(&r.summary.ExecutedCount),
		FailedNoStockCount:  atomic.LoadInt64(&r.summary.FailedNoStockCount),
	}
}
