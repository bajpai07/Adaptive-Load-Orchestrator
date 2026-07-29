package fulfillment

import (
	"sync"
	"time"
)

// OrderQueue is a goroutine-safe FIFO queue for orders.
type OrderQueue struct {
	mu     sync.Mutex
	orders []*Order
}

// NewOrderQueue initializes an empty OrderQueue.
func NewOrderQueue() *OrderQueue {
	return &OrderQueue{
		orders: make([]*Order, 0),
	}
}

// Push adds an order to the end of the queue.
func (q *OrderQueue) Push(order *Order) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if order.Status == "" {
		order.Status = StatusQueued
	}
	q.orders = append(q.orders, order)
}

// Pop removes and returns the first order in the queue.
func (q *OrderQueue) Pop() (*Order, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.orders) == 0 {
		return nil, false
	}
	order := q.orders[0]
	q.orders = q.orders[1:]
	return order, true
}

// Len returns the current number of orders in the queue.
func (q *OrderQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.orders)
}

// ExtractNonPerishables extracts up to maxCount non-perishable orders from the queue for re-routing.
func (q *OrderQueue) ExtractNonPerishables(maxCount int) []*Order {
	q.mu.Lock()
	defer q.mu.Unlock()

	extracted := make([]*Order, 0, maxCount)
	remaining := make([]*Order, 0, len(q.orders))

	for _, ord := range q.orders {
		if !ord.IsPerishable && len(extracted) < maxCount {
			extracted = append(extracted, ord)
		} else {
			remaining = append(remaining, ord)
		}
	}

	q.orders = remaining
	return extracted
}

// ConsolidateOrdersByCart merges queued orders sharing a GroupCartID into a single consolidated picking pass.
func (q *OrderQueue) ConsolidateOrdersByCart(cartID string) (consolidatedCount int, itemsMerged int) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.orders) < 2 || cartID == "" {
		return 0, 0
	}

	var matching []*Order
	var remaining []*Order

	for _, ord := range q.orders {
		if ord.GroupCartID == cartID && !ord.IsConsolidated {
			matching = append(matching, ord)
		} else {
			remaining = append(remaining, ord)
		}
	}

	if len(matching) < 2 {
		return 0, 0
	}

	// Consolidate matching orders into the primary order
	primary := matching[0]
	primary.IsConsolidated = true
	primary.ConsolidatedCount = len(matching)

	q.orders = append([]*Order{primary}, remaining...)
	return 1, len(matching)
}

// WaitingOrdersStats computes current wait time statistics (count, min, max, avg seconds) for remaining queued orders.
func (q *OrderQueue) WaitingOrdersStats(now time.Time) (count int, minWaitSec, maxWaitSec, avgWaitSec float64) {
	q.mu.Lock()
	defer q.mu.Unlock()

	count = len(q.orders)
	if count == 0 {
		return 0, 0, 0, 0
	}

	var sumWait float64
	minWaitSec = now.Sub(q.orders[0].QueuedAt).Seconds()
	maxWaitSec = minWaitSec

	for _, ord := range q.orders {
		w := now.Sub(ord.QueuedAt).Seconds()
		sumWait += w
		if w < minWaitSec {
			minWaitSec = w
		}
		if w > maxWaitSec {
			maxWaitSec = w
		}
	}

	avgWaitSec = sumWait / float64(count)
	return count, minWaitSec, maxWaitSec, avgWaitSec
}
