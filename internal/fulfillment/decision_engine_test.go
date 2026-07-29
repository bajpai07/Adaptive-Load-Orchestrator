package fulfillment

import (
	"context"
	"fmt"
	"testing"
	"time"

	"adaptive-load-orchestrator/internal/groupcart"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDecisionEngine_RealOrderConsolidation(t *testing.T) {
	q := NewOrderQueue()

	// Push 4 orders: 3 from cart-101, 1 individual
	q.Push(&Order{ID: "ord-1", GroupCartID: "cart-101", Status: StatusQueued})
	q.Push(&Order{ID: "ord-2", GroupCartID: "cart-101", Status: StatusQueued})
	q.Push(&Order{ID: "ord-3", GroupCartID: "other", Status: StatusQueued})
	q.Push(&Order{ID: "ord-4", GroupCartID: "cart-101", Status: StatusQueued})

	assert.Equal(t, 4, q.Len())

	passes, merged := q.ConsolidateOrdersByCart("cart-101")
	assert.Equal(t, 1, passes)
	assert.Equal(t, 3, merged)
	assert.Equal(t, 2, q.Len(), "Queue length should reduce from 4 to 2 (1 consolidated pass + 1 other)")

	primary, ok := q.Pop()
	require.True(t, ok)
	assert.Equal(t, "ord-1", primary.ID)
	assert.True(t, primary.IsConsolidated)
	assert.Equal(t, 3, primary.ConsolidatedCount)
}

func TestDecisionEngine_BatchingFirstThenFallback(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	cartStore := groupcart.NewRedisCartStore(rdb)
	resStore := NewStockReservationStore(rdb)
	ctx := context.Background()

	costCfg := DefaultCostModelConfig()
	engine := NewDecisionEngine(ModeFullOrchestration, costCfg, cartStore, resStore)

	// Create 2 stores
	s1 := NewStore("store-1", 28.6315, 77.2167, 10, 1.2, 0.15, 2)
	s2 := NewStore("store-2", 28.6350, 77.2200, 10, 1.2, 0.15, 2)
	allStores := []*Store{s1, s2}

	// Create active group cart in Redis for store-1 geofence
	geofenceKey := "geofence_cart:geofence-store-1"
	_ = rdb.Set(ctx, geofenceKey, "cart-999", 0).Err()
	cartJSON := `{"id":"cart-999","geofence_id":"geofence-store-1","status":"ACTIVE"}`
	_ = rdb.Set(ctx, "cart:cart-999", cartJSON, 0).Err()

	// Fill store-1 queue with 9 orders (90% load), 3 sharing cart-999
	for i := 1; i <= 9; i++ {
		cartID := ""
		if i <= 3 {
			cartID = "cart-999"
		}
		s1.EnqueueOrder(&Order{ID: fmt.Sprintf("ord-%d", i), GroupCartID: cartID, Status: StatusQueued})
	}

	assert.Greater(t, s1.ComputeLoad(), 0.85)

	// Evaluation 1: Should fire BATCHING_NUDGE_ISSUED
	ev1 := engine.EvaluateBreach(ctx, s1, allStores, 10*1e9)
	assert.Equal(t, OutcomeBatchingNudgeIssued, ev1.Outcome)
	assert.Equal(t, "cart-999", ev1.NudgeCartID)
	assert.Equal(t, 3, ev1.ConsolidatedCount)

	// Evaluation 2 (at +10s): Should be blocked by 30s Nudge Grace Window -> NO_ACTION
	ev2 := engine.EvaluateBreach(ctx, s1, allStores, 20*1e9)
	assert.Equal(t, OutcomeNoAction, ev2.Outcome)

	// Fill s1 queue with additional orders so load > 85% even after consolidation
	for i := 10; i <= 15; i++ {
		s1.EnqueueOrder(&Order{ID: fmt.Sprintf("ord-%d", i), Status: StatusQueued})
	}

	// Evaluation 3 (at +40s, post grace window): Should fall through to Cost-Gated Decision Gate
	ev3 := engine.EvaluateBreach(ctx, s1, allStores, 50*1e9)
	assert.True(t, ev3.Outcome == OutcomeReRouteRejectedOnCost || ev3.Outcome == OutcomeReRouteExecuted)
}

func TestDecisionEngine_Concurrency_20RunsLoop(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	cartStore := groupcart.NewRedisCartStore(rdb)
	resStore := NewStockReservationStore(rdb)
	ctx := context.Background()

	costCfg := DefaultCostModelConfig()

	for run := 1; run <= 20; run++ {
		t.Run("Run", func(t *testing.T) {
			engine := NewDecisionEngine(ModeFullOrchestration, costCfg, cartStore, resStore)
			s1 := NewStore("store-1", 28.6315, 77.2167, 10, 1.2, 0.15, 2)
			s2 := NewStore("store-2", 28.6350, 77.2200, 10, 1.2, 0.15, 2)
			allStores := []*Store{s1, s2}

			for i := 1; i <= 9; i++ {
				s1.EnqueueOrder(&Order{ID: "ord-conc", Status: StatusQueued, QueuedAt: time.Now()})
			}

			done := make(chan bool, 5)
			for g := 0; g < 5; g++ {
				go func(id int) {
					engine.EvaluateBreach(ctx, s1, allStores, int64(id)*1e9)
					done <- true
				}(g)
			}

			for g := 0; g < 5; g++ {
				<-done
			}

			summary := engine.GetSummary()
			assert.Equal(t, int64(5), summary.TotalEvaluations)
		})
	}
}
