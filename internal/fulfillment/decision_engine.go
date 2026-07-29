package fulfillment

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"

	"adaptive-load-orchestrator/internal/groupcart"
	"adaptive-load-orchestrator/internal/simulation"
)

type DecisionEngineMode string

const (
	ModeNaive            DecisionEngineMode = "naive"
	ModeCostGated        DecisionEngineMode = "cost-gated"
	ModeFullOrchestration DecisionEngineMode = "full-orchestration"
)

type UnifiedDecisionEvent struct {
	Outcome              ReRouteDecisionOutcome `json:"outcome"`
	Mode                 DecisionEngineMode     `json:"mode"`
	SourceStoreID        string                 `json:"source_store_id"`
	TargetStoreID        string                 `json:"target_store_id,omitempty"`
	NudgeCartID          string                 `json:"nudge_cart_id,omitempty"`
	OrderID              string                 `json:"order_id,omitempty"`
	DistanceKm           float64                `json:"distance_km"`
	ReRouteCostPaise     int64                  `json:"reroute_cost_paise"`
	SLAPenaltyPaise      int64                  `json:"sla_penalty_paise"`
	PredictedDelayMin    float64                `json:"predicted_delay_min"`
	ConsolidatedCount    int                    `json:"consolidated_count,omitempty"`
	SimTimeNs            int64                  `json:"sim_time_ns"`
}

type UnifiedDecisionSummary struct {
	TotalEvaluations    int64 `json:"total_evaluations"`
	NoActionCount       int64 `json:"no_action_count"`
	BatchingNudgeCount  int64 `json:"batching_nudge_count"`
	RejectedOnCostCount int64 `json:"rejected_on_cost_count"`
	ExecutedCount       int64 `json:"executed_count"`
	FailedNoStockCount  int64 `json:"failed_no_stock_count"`
	TotalOrdersMerged   int64 `json:"total_orders_merged"`
}

type DecisionEngine struct {
	mu                   sync.Mutex
	mode                 DecisionEngineMode
	costConfig           CostModelConfig
	cartStore            *groupcart.RedisCartStore
	reservationStore     *StockReservationStore
	cooldownNs           int64
	nudgeGraceNs         int64
	recoveryCap          float64
	maxDistKm            float64
	lastNudgeSimTime     map[string]int64
	lastReRouteSimTime   map[string]int64
	summary              UnifiedDecisionSummary
	eventStream          chan *UnifiedDecisionEvent
}

func NewDecisionEngine(mode DecisionEngineMode, costCfg CostModelConfig, cartStore *groupcart.RedisCartStore, resStore *StockReservationStore) *DecisionEngine {
	return &DecisionEngine{
		mode:               mode,
		costConfig:         costCfg,
		cartStore:          cartStore,
		reservationStore:   resStore,
		cooldownNs:         30 * 1e9, // 30s re-route cooldown
		nudgeGraceNs:       30 * 1e9, // 30s batching nudge grace window
		recoveryCap:        0.70,
		maxDistKm:          1.5,
		lastNudgeSimTime:   make(map[string]int64),
		lastReRouteSimTime: make(map[string]int64),
		eventStream:        make(chan *UnifiedDecisionEvent, 1000),
	}
}

func (de *DecisionEngine) EventStream() <-chan *UnifiedDecisionEvent {
	return de.eventStream
}

func (de *DecisionEngine) EvaluateBreach(ctx context.Context, sourceStore *Store, allStores []*Store, simTimeNs int64) *UnifiedDecisionEvent {
	atomic.AddInt64(&de.summary.TotalEvaluations, 1)

	sourceLoad := sourceStore.ComputeLoad()
	if sourceLoad <= 0.85 {
		atomic.AddInt64(&de.summary.NoActionCount, 1)
		return &UnifiedDecisionEvent{Outcome: OutcomeNoAction, SourceStoreID: sourceStore.ID, Mode: de.mode, SimTimeNs: simTimeNs}
	}

	de.mu.Lock()
	lastNudge := de.lastNudgeSimTime[sourceStore.ID]
	lastReRoute := de.lastReRouteSimTime[sourceStore.ID]
	de.mu.Unlock()

	// Mode (c) Full Orchestration: Demand-Side Group Cart Batching First!
	if de.mode == ModeFullOrchestration {
		// Step 1: Check active Group Carts in Redis for this store's geofence
		geofenceID := fmt.Sprintf("geofence-%s", sourceStore.ID)
		if de.cartStore != nil {
			activeCartKey := fmt.Sprintf("geofence_cart:%s", geofenceID)
			cartID, err := de.cartStore.Client().Get(ctx, activeCartKey).Result()
			if err == nil && cartID != "" {
				cart, err := de.cartStore.GetCart(ctx, cartID)
				if err == nil && cart != nil && cart.Status == groupcart.CartStatusActive {
					// Apply queue order consolidation
					_, merged := sourceStore.Queue().ConsolidateOrdersByCart(cart.ID)
					if merged > 1 {
						de.mu.Lock()
						de.lastNudgeSimTime[sourceStore.ID] = simTimeNs
						de.mu.Unlock()

						atomic.AddInt64(&de.summary.BatchingNudgeCount, 1)
						atomic.AddInt64(&de.summary.TotalOrdersMerged, int64(merged))

						ev := &UnifiedDecisionEvent{
							Outcome:           OutcomeBatchingNudgeIssued,
							Mode:              de.mode,
							SourceStoreID:     sourceStore.ID,
							NudgeCartID:       cart.ID,
							ConsolidatedCount: merged,
							SimTimeNs:         simTimeNs,
						}
						log.Printf("[DECISION ENGINE] SimTime: %.1fs | Store: %s | BATCHING_NUDGE_ISSUED | GroupCart: %s | Consolidated %d orders into 1 picking pass",
							float64(simTimeNs)/1e9, sourceStore.ID, cart.ID, merged)

						de.publishEvent(ev)
						return ev
					}
				}
			}
		}

		// Step 2: Check 30s Nudge Grace Window before allowing fallback re-routing
		if lastNudge > 0 && (simTimeNs-lastNudge) < de.nudgeGraceNs {
			atomic.AddInt64(&de.summary.NoActionCount, 1)
			return &UnifiedDecisionEvent{Outcome: OutcomeNoAction, SourceStoreID: sourceStore.ID, Mode: de.mode, SimTimeNs: simTimeNs}
		}
	}

	// Step 3: Check 30s Re-route cooldown
	if lastReRoute > 0 && (simTimeNs-lastReRoute) < de.cooldownNs {
		atomic.AddInt64(&de.summary.NoActionCount, 1)
		return &UnifiedDecisionEvent{Outcome: OutcomeNoAction, SourceStoreID: sourceStore.ID, Mode: de.mode, SimTimeNs: simTimeNs}
	}

	// Find best candidate target store (<1.5km, load < 70%)
	var bestTarget *Store
	bestDist := de.maxDistKm + 1.0

	for _, s := range allStores {
		if s.ID == sourceStore.ID {
			continue
		}
		if s.ComputeLoad() < de.recoveryCap {
			dist := simulation.HaversineDistance(sourceStore.Lat, sourceStore.Lng, s.Lat, s.Lng)
			if dist <= de.maxDistKm && dist < bestDist {
				bestDist = dist
				bestTarget = s
			}
		}
	}

	if bestTarget == nil {
		atomic.AddInt64(&de.summary.NoActionCount, 1)
		return &UnifiedDecisionEvent{Outcome: OutcomeNoAction, SourceStoreID: sourceStore.ID, Mode: de.mode, SimTimeNs: simTimeNs}
	}

	waitingOrders := sourceStore.WaitingOrdersCount()
	predictedDelaySec := float64(waitingOrders) * 1.336
	predictedDelayMin := predictedDelaySec / 60.0

	shouldReRoute, reRouteCost, slaPenalty := EvaluateReRouteCostGate(de.costConfig, bestDist, predictedDelayMin)

	// Mode (b) & (c): Cost-Gated Decision Gate Check
	if de.mode != ModeNaive && !shouldReRoute {
		atomic.AddInt64(&de.summary.RejectedOnCostCount, 1)
		ev := &UnifiedDecisionEvent{
			Outcome:           OutcomeReRouteRejectedOnCost,
			Mode:              de.mode,
			SourceStoreID:     sourceStore.ID,
			TargetStoreID:     bestTarget.ID,
			DistanceKm:        bestDist,
			ReRouteCostPaise:  reRouteCost,
			SLAPenaltyPaise:   slaPenalty,
			PredictedDelayMin: predictedDelayMin,
			SimTimeNs:         simTimeNs,
		}
		log.Printf("[DECISION ENGINE] SimTime: %.1fs | Store: %s -> %s | RE_ROUTE_REJECTED_ON_COST | Cost: ₹%.2f >= SLA Penalty: ₹%.2f (Delay: %.2f min)",
			float64(simTimeNs)/1e9, sourceStore.ID, bestTarget.ID, float64(reRouteCost)/100.0, float64(slaPenalty)/100.0, predictedDelayMin)

		de.publishEvent(ev)
		return ev
	}

	// Extract order from source queue
	extracted := sourceStore.Queue().ExtractNonPerishables(1)
	if len(extracted) == 0 {
		atomic.AddInt64(&de.summary.NoActionCount, 1)
		return &UnifiedDecisionEvent{Outcome: OutcomeNoAction, SourceStoreID: sourceStore.ID, Mode: de.mode, SimTimeNs: simTimeNs}
	}
	ord := extracted[0]

	// Stock Reservation Safety Check
	if de.reservationStore != nil {
		reserved, _, err := de.reservationStore.ReserveStockAtomic(ctx, bestTarget.ID, "SKU-GENERIC", 1)
		if err != nil || !reserved {
			sourceStore.Queue().Push(ord)
			atomic.AddInt64(&de.summary.FailedNoStockCount, 1)
			ev := &UnifiedDecisionEvent{
				Outcome:           OutcomeReRouteFailedNoStock,
				Mode:              de.mode,
				SourceStoreID:     sourceStore.ID,
				TargetStoreID:     bestTarget.ID,
				OrderID:           ord.ID,
				DistanceKm:        bestDist,
				ReRouteCostPaise:  reRouteCost,
				SLAPenaltyPaise:   slaPenalty,
				PredictedDelayMin: predictedDelayMin,
				SimTimeNs:         simTimeNs,
			}
			log.Printf("[DECISION ENGINE] SimTime: %.1fs | Store: %s -> %s | RE_ROUTE_FAILED_NO_STOCK | Order: %s returned to queue",
				float64(simTimeNs)/1e9, sourceStore.ID, bestTarget.ID, ord.ID)

			de.publishEvent(ev)
			return ev
		}
	}

	// Execute Re-Route
	de.mu.Lock()
	de.lastReRouteSimTime[sourceStore.ID] = simTimeNs
	de.mu.Unlock()

	sourceStore.SetLastReRouteSimTimeNs(simTimeNs)
	sourceStore.IncrReRoutedOut()
	bestTarget.IncrReRoutedIn()
	bestTarget.EnqueueOrder(ord)

	atomic.AddInt64(&de.summary.ExecutedCount, 1)

	ev := &UnifiedDecisionEvent{
		Outcome:           OutcomeReRouteExecuted,
		Mode:              de.mode,
		SourceStoreID:     sourceStore.ID,
		TargetStoreID:     bestTarget.ID,
		OrderID:           ord.ID,
		DistanceKm:        bestDist,
		ReRouteCostPaise:  reRouteCost,
		SLAPenaltyPaise:   slaPenalty,
		PredictedDelayMin: predictedDelayMin,
		SimTimeNs:         simTimeNs,
	}
	log.Printf("[DECISION ENGINE] SimTime: %.1fs | Store: %s -> %s | RE_ROUTE_EXECUTED | Cost: ₹%.2f < SLA Penalty: ₹%.2f (Delay: %.2f min) | Order: %s",
		float64(simTimeNs)/1e9, sourceStore.ID, bestTarget.ID, float64(reRouteCost)/100.0, float64(slaPenalty)/100.0, predictedDelayMin, ord.ID)

	de.publishEvent(ev)
	return ev
}

func (de *DecisionEngine) publishEvent(ev *UnifiedDecisionEvent) {
	select {
	case de.eventStream <- ev:
	default:
	}
}

func (de *DecisionEngine) GetSummary() UnifiedDecisionSummary {
	return UnifiedDecisionSummary{
		TotalEvaluations:    atomic.LoadInt64(&de.summary.TotalEvaluations),
		NoActionCount:       atomic.LoadInt64(&de.summary.NoActionCount),
		BatchingNudgeCount:  atomic.LoadInt64(&de.summary.BatchingNudgeCount),
		RejectedOnCostCount: atomic.LoadInt64(&de.summary.RejectedOnCostCount),
		ExecutedCount:       atomic.LoadInt64(&de.summary.ExecutedCount),
		FailedNoStockCount:  atomic.LoadInt64(&de.summary.FailedNoStockCount),
		TotalOrdersMerged:   atomic.LoadInt64(&de.summary.TotalOrdersMerged),
	}
}
