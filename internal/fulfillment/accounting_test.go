package fulfillment

import (
	"context"
	"fmt"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"

	"adaptive-load-orchestrator/internal/simulation"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAccountingIntegrity_InvariantHoldsUnderConcurrentSurgeAndReRouting(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	resStore := NewStockReservationStore(rdb)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Pre-populate stock
	_ = resStore.SetStock(ctx, "store-1", "SKU-GENERIC", 100)
	_ = resStore.SetStock(ctx, "store-2", "SKU-GENERIC", 100)

	s1 := NewStore("store-1", 28.6315, 77.2167, 10, 5.0, 0.5, 3)
	s2 := NewStore("store-2", 28.6350, 77.2200, 10, 1.0, 0.5, 3)
	stores := []*Store{s1, s2}

	var totalCreated int64

	costCfg := DefaultCostModelConfig()
	engine := NewDecisionEngine(ModeCostGated, costCfg, nil, resStore)

	monitor := NewLoadMonitor(stores, 0.85, 0.70, 10*time.Millisecond, 10.0, engine, func() int64 {
		return atomic.LoadInt64(&totalCreated)
	})

	s1.Start(ctx)
	s2.Start(ctx)
	monitor.Start(ctx)
	defer s1.Stop()
	defer s2.Stop()
	defer monitor.Stop()

	// Launch order arrival generator
	go func() {
		_ = simulation.NewRandomGenerator(42)
		for i := 0; i < 50; i++ {
			select {
			case <-ctx.Done():
				return
			default:
				ordID := fmt.Sprintf("ord-%d", i)
				st := stores[rand.Intn(2)]
				atomic.AddInt64(&totalCreated, 1)
				st.EnqueueOrder(&Order{
					ID:              ordID,
					StoreID:         st.ID,
					OriginalStoreID: st.ID,
					QueuedAt:        time.Now(),
				})
				st.IncrReceived()

				// Periodically assert accounting invariant
				created := atomic.LoadInt64(&totalCreated)
				summary, err := VerifyNetworkAccountingInvariant(stores, created)
				assert.NoError(t, err, "Accounting invariant must hold on every tick")
				assert.Equal(t, created, summary.TotalCreated)
				time.Sleep(20 * time.Millisecond)
			}
		}
	}()

	<-ctx.Done()

	// Final verification
	created := atomic.LoadInt64(&totalCreated)
	summary, err := VerifyNetworkAccountingInvariant(stores, created)
	assert.NoError(t, err, "Final accounting invariant check must pass")
	assert.Equal(t, created, summary.TotalCreated)
}
