package fulfillment

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestStore_ComputeLoadFormula(t *testing.T) {
	s := NewStore("store-test", 28.6, 77.2, 10, 1.0, 0.5, 2)
	assert.Equal(t, 0.0, s.ComputeLoad())

	// Enqueue 5 orders
	for i := 0; i < 5; i++ {
		s.EnqueueOrder(&Order{ID: "o"})
	}
	assert.Equal(t, 0.5, s.ComputeLoad())

	// Simulate 2 active pickers
	s.IncrPicking()
	s.IncrPicking()
	assert.Equal(t, 0.7, s.ComputeLoad(), "(5 in queue + 2 in picking) / 10 capacity = 0.7")
}

func TestLoadMonitor_ThresholdBreachDetection(t *testing.T) {
	s1 := NewStore("store-1", 28.6, 77.2, 10, 1.0, 0.5, 2)
	stores := []*Store{s1}

	var totalCreated int64
	costCfg := DefaultCostModelConfig()
	engine := NewDecisionEngine(ModeCostGated, costCfg, nil, nil)

	monitor := NewLoadMonitor(stores, 0.85, 0.70, 10*time.Millisecond, 10.0, engine, func() int64 {
		return atomic.LoadInt64(&totalCreated)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	monitor.Start(ctx)
	defer monitor.Stop()

	// Push 9 orders -> 90% load
	for i := 0; i < 9; i++ {
		s1.EnqueueOrder(&Order{ID: "o"})
		s1.IncrReceived()
		atomic.AddInt64(&totalCreated, 1)
	}

	time.Sleep(50 * time.Millisecond)

	summary := monitor.GetBreachSummary(200 * time.Millisecond)
	assert.GreaterOrEqual(t, summary.TotalEpisodes, int64(1))
}

func TestLoadMonitor_LowTimeScale_NoSimTimeOverflow(t *testing.T) {
	s1 := NewStore("store-1", 28.6, 77.2, 10, 1.0, 0.5, 2)
	stores := []*Store{s1}

	var totalCreated int64
	costCfg := DefaultCostModelConfig()
	engine := NewDecisionEngine(ModeFullOrchestration, costCfg, nil, nil)

	// Low time scale: 5.0x
	monitor := NewLoadMonitor(stores, 0.85, 0.70, 20*time.Millisecond, 5.0, engine, func() int64 {
		return atomic.LoadInt64(&totalCreated)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	monitor.Start(ctx)

	for i := 0; i < 9; i++ {
		s1.EnqueueOrder(&Order{ID: "o"})
		s1.IncrReceived()
		atomic.AddInt64(&totalCreated, 1)
	}

	time.Sleep(50 * time.Millisecond)
	monitor.Stop()

	// Verify DecisionEngine event stream has non-negative SimTimeNs
	select {
	case ev := <-engine.EventStream():
		assert.GreaterOrEqual(t, ev.SimTimeNs, int64(0), "SimTimeNs should never overflow to negative")
	default:
	}
}
