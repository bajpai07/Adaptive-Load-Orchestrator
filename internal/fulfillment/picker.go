package fulfillment

import (
	"context"
	"sync"
	"time"

	"adaptive-load-orchestrator/internal/simulation"
)

// PickerPool runs N picker goroutines processing orders from a store's queue (M/M/c queueing system).
type PickerPool struct {
	store     *Store
	rng       *simulation.RandomGenerator
	timeScale float64
	wg        sync.WaitGroup
	cancel    context.CancelFunc
}

func NewPickerPool(store *Store, seed int64, timeScale float64) *PickerPool {
	if timeScale <= 0 {
		timeScale = 1.0
	}
	return &PickerPool{
		store:     store,
		rng:       simulation.NewRandomGenerator(seed),
		timeScale: timeScale,
	}
}

func (p *PickerPool) Start(ctx context.Context) {
	ctx, p.cancel = context.WithCancel(ctx)
	for i := 0; i < p.store.PickerCount; i++ {
		p.wg.Add(1)
		go p.worker(ctx, i)
	}
}

func (p *PickerPool) Stop() {
	if p.cancel != nil {
		p.cancel()
	}
	p.wg.Wait()
}

func (p *PickerPool) worker(ctx context.Context, workerID int) {
	defer p.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		default:
			order, ok := p.store.CurrentQueue.Pop()
			if !ok {
				select {
				case <-ctx.Done():
					return
				case <-time.After(time.Duration(2.0 / p.timeScale * float64(time.Millisecond))):
					continue
				}
			}

			simNow := p.store.SimNow()
			order.PickStartedAt = simNow
			order.Status = StatusPicking
			p.store.IncrPicking()

			// Calculate service duration from exponential distribution (rate = mu)
			serviceDuration := p.rng.NextServiceTime(p.store.ServiceRate)
			order.ServiceDuration = serviceDuration

			// Simulated picking delay
			simulatedDelay := time.Duration(float64(serviceDuration) / p.timeScale)
			time.Sleep(simulatedDelay)

			order.CompletedAt = order.PickStartedAt.Add(serviceDuration)
			order.Status = StatusCompleted

			simServiceNs := serviceDuration.Nanoseconds()
			simSystemNs := order.CompletedAt.Sub(order.QueuedAt).Nanoseconds()
			order.TotalSystemTime = time.Duration(simSystemNs)

			p.store.DecrPickingAndIncrCompleted(simServiceNs, simSystemNs)
		}
	}
}
