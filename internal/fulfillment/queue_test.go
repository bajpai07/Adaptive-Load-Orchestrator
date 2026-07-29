package fulfillment

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestOrderQueue_ConcurrentPushesAndPops(t *testing.T) {
	queue := NewOrderQueue()
	numProducers := 10
	numConsumers := 10
	ordersPerProducer := 100

	var wg sync.WaitGroup

	// Concurrently push orders
	for i := 0; i < numProducers; i++ {
		wg.Add(1)
		go func(producerID int) {
			defer wg.Done()
			for j := 0; j < ordersPerProducer; j++ {
				queue.Push(&Order{
					ID:           fmt.Sprintf("ord-%d-%d", producerID, j),
					StoreID:      "store-1",
					IsPerishable: j%2 == 0,
					QueuedAt:     time.Now(),
				})
			}
		}(i)
	}

	poppedCount := 0
	var popMu sync.Mutex

	// Concurrently pop orders
	for i := 0; i < numConsumers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				ord, ok := queue.Pop()
				if ok {
					assert.NotNil(t, ord)
					popMu.Lock()
					poppedCount++
					popMu.Unlock()
				} else {
					// Check if all producers might be done
					popMu.Lock()
					currentPopped := poppedCount
					popMu.Unlock()
					if currentPopped == numProducers*ordersPerProducer {
						break
					}
					time.Sleep(1 * time.Millisecond)
				}
			}
		}()
	}

	wg.Wait()

	assert.Equal(t, 0, queue.Len(), "Queue should be empty after processing all orders")
	assert.Equal(t, numProducers*ordersPerProducer, poppedCount, "Total popped orders should match total pushed orders")
}

func TestOrderQueue_ExtractNonPerishables(t *testing.T) {
	queue := NewOrderQueue()
	queue.Push(&Order{ID: "1", IsPerishable: true})
	queue.Push(&Order{ID: "2", IsPerishable: false})
	queue.Push(&Order{ID: "3", IsPerishable: true})
	queue.Push(&Order{ID: "4", IsPerishable: false})
	queue.Push(&Order{ID: "5", IsPerishable: false})

	assert.Equal(t, 5, queue.Len())

	// Extract up to 2 non-perishables
	extracted := queue.ExtractNonPerishables(2)
	assert.Len(t, extracted, 2)
	assert.Equal(t, "2", extracted[0].ID)
	assert.Equal(t, "4", extracted[1].ID)

	assert.Equal(t, 3, queue.Len())

	// Remaining items in queue should be IDs 1, 3, 5
	ord1, ok1 := queue.Pop()
	assert.True(t, ok1)
	assert.Equal(t, "1", ord1.ID)

	ord3, ok3 := queue.Pop()
	assert.True(t, ok3)
	assert.Equal(t, "3", ord3.ID)

	ord5, ok5 := queue.Pop()
	assert.True(t, ok5)
	assert.Equal(t, "5", ord5.ID)
}
