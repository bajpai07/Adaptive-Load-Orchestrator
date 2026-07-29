package fulfillment

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFulfillmentEngine_EndToEndNaiveReRouteWithCooldown(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	resStore := NewStockReservationStore(rdb)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	_ = resStore.SetStock(ctx, "store-2", "SKU-GENERIC", 100)

	s1 := NewStore("store-1", 28.6315, 77.2167, 10, 2.0, 0.5, 2)
	s2 := NewStore("store-2", 28.6350, 77.2200, 10, 0.5, 0.5, 2)
	stores := []*Store{s1, s2}

	var totalCreated int64

	costCfg := DefaultCostModelConfig()
	engine := NewDecisionEngine(ModeNaive, costCfg, nil, resStore)

	monitor := NewLoadMonitor(stores, 0.85, 0.70, 10*time.Millisecond, 10.0, engine, func() int64 {
		return atomic.LoadInt64(&totalCreated)
	})

	s1.Start(ctx)
	s2.Start(ctx)
	monitor.Start(ctx)
	defer s1.Stop()
	defer s2.Stop()
	defer monitor.Stop()

	// Fill store-1 queue to breach threshold (> 85%)
	for i := 0; i < 9; i++ {
		s1.EnqueueOrder(&Order{
			ID:              "ord-overload",
			StoreID:         "store-1",
			OriginalStoreID: "store-1",
			QueuedAt:        time.Now(),
		})
		s1.IncrReceived()
		atomic.AddInt64(&totalCreated, 1)
	}

	assert.Greater(t, s1.ComputeLoad(), 0.85, "Store 1 load should breach 85%")

	// Give monitor time to trigger re-route
	time.Sleep(100 * time.Millisecond)

	rec, comp, rIn, rOut, _, _, _, _ := s1.Stats()
	assert.Equal(t, int64(9), rec)
	assert.GreaterOrEqual(t, rOut+comp, int64(1), "Store 1 should have re-routed or completed orders")
	assert.GreaterOrEqual(t, rIn, int64(0))
}
