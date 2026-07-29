package fulfillment

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTestReservationStore(t *testing.T) (*StockReservationStore, func()) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})
	resStore := NewStockReservationStore(rdb)

	cleanup := func() {
		rdb.Close()
		mr.Close()
	}
	return resStore, cleanup
}

func TestReservation_ConcurrentContention(t *testing.T) {
	resStore, cleanup := setupTestReservationStore(t)
	defer cleanup()

	ctx := context.Background()
	storeID := "store-target"
	sku := "SKU-LIMITED"

	// Set destination store stock to exactly 1 unit
	err := resStore.SetStock(ctx, storeID, sku, 1)
	require.NoError(t, err)

	numAttempts := 2
	var wg sync.WaitGroup
	results := make([]bool, numAttempts)

	for i := 0; i < numAttempts; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			success, _, err := resStore.ReserveStockAtomic(ctx, storeID, sku, 1)
			assert.NoError(t, err)
			results[idx] = success
		}(i)
	}

	wg.Wait()

	// Count successes vs failures
	successCount := 0
	failureCount := 0
	for _, res := range results {
		if res {
			successCount++
		} else {
			failureCount++
		}
	}

	assert.Equal(t, 1, successCount, "Exactly ONE re-route reservation attempt must succeed when stock=1")
	assert.Equal(t, 1, failureCount, "Second re-route reservation attempt must be rejected (RE_ROUTE_FAILED_NO_STOCK)")

	// Check final stock in Redis
	remaining, err := resStore.GetStock(ctx, storeID, sku)
	require.NoError(t, err)
	assert.Equal(t, int64(0), remaining, "Final stock after reservation must equal 0")
}

func TestReservation_ConcurrentContention_20RunsLoop(t *testing.T) {
	// Execute the concurrent stock contention test 20 consecutive times in a loop
	for run := 1; run <= 20; run++ {
		t.Run(fmt.Sprintf("Run_%d", run), func(t *testing.T) {
			TestReservation_ConcurrentContention(t)
		})
	}
}
