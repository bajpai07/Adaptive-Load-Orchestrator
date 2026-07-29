package fulfillment

import (
	"context"
	"log"
	"sync"
	"time"
)

type ThresholdBreachedEvent struct {
	StoreID     string    `json:"store_id"`
	CurrentLoad float64   `json:"current_load"`
	Threshold   float64   `json:"threshold"`
	QueueLength int       `json:"queue_length"`
	Timestamp   time.Time `json:"timestamp"`
}

type StoreEpisodeState struct {
	InBreachedState     bool  `json:"in_breached_state"`
	EpisodeStartSimNs   int64 `json:"episode_start_sim_ns"`
	TotalEpisodes       int64 `json:"total_episodes"`
	TotalDurationSimNs int64 `json:"total_duration_sim_ns"`
	MaxDurationSimNs   int64 `json:"max_duration_sim_ns"`
	RawTickBreaches     int64 `json:"raw_tick_breaches"`
}

type NetworkBreachSummary struct {
	TotalEpisodes        int64   `json:"total_episodes"`
	AvgDurationSec       float64 `json:"avg_duration_sec"`
	MaxDurationSec       float64 `json:"max_duration_sec"`
	TotalRawTickBreaches int64   `json:"total_raw_tick_breaches"`
}

// LoadMonitor periodically monitors dark store load and triggers DecisionEngine evaluation.
type LoadMonitor struct {
	stores            []*Store
	threshold         float64
	recoveryThreshold float64
	interval          time.Duration
	timeScale         float64
	startTime         time.Time
	decisionEngine    *DecisionEngine
	createdCountFn    func() int64

	mu            sync.Mutex
	episodeStates map[string]*StoreEpisodeState
	cancel        context.CancelFunc
	wg            sync.WaitGroup
}

// NewLoadMonitor initializes a LoadMonitor.
func NewLoadMonitor(stores []*Store, threshold, recoveryThreshold float64, interval time.Duration, timeScale float64, de *DecisionEngine, createdCountFn func() int64) *LoadMonitor {
	if threshold <= 0 {
		threshold = 0.85 // Default 85%
	}
	if recoveryThreshold <= 0 {
		recoveryThreshold = 0.70 // Default 70%
	}
	if timeScale <= 0 {
		timeScale = 1.0
	}

	states := make(map[string]*StoreEpisodeState)
	for _, s := range stores {
		states[s.ID] = &StoreEpisodeState{}
	}

	return &LoadMonitor{
		stores:            stores,
		threshold:         threshold,
		recoveryThreshold: recoveryThreshold,
		interval:          interval,
		timeScale:         timeScale,
		startTime:         time.Now(),
		decisionEngine:    de,
		createdCountFn:    createdCountFn,
		episodeStates:     states,
	}
}

// Start runs the periodic load monitoring loop.
func (m *LoadMonitor) Start(ctx context.Context) {
	m.startTime = time.Now()
	ctx, m.cancel = context.WithCancel(ctx)
	m.wg.Add(1)
	go m.loop(ctx)
}

// Stop halts monitoring.
func (m *LoadMonitor) Stop() {
	if m.cancel != nil {
		m.cancel()
	}
	m.wg.Wait()
}

func (m *LoadMonitor) GetBreachSummary(simDuration time.Duration) NetworkBreachSummary {
	m.mu.Lock()
	defer m.mu.Unlock()

	var totalEp int64
	var totalDurSimNs int64
	var maxDurSimNs int64
	var totalRawBreaches int64

	endSimNs := simDuration.Nanoseconds()

	for _, state := range m.episodeStates {
		epCount := state.TotalEpisodes
		durSimNs := state.TotalDurationSimNs
		maxDur := state.MaxDurationSimNs

		if state.InBreachedState {
			epCount++
			activeDurSimNs := endSimNs - state.EpisodeStartSimNs
			durSimNs += activeDurSimNs
			if activeDurSimNs > maxDur {
				maxDur = activeDurSimNs
			}
		}

		totalEp += epCount
		totalDurSimNs += durSimNs
		if maxDur > maxDurSimNs {
			maxDurSimNs = maxDur
		}
		totalRawBreaches += state.RawTickBreaches
	}

	var avgDurSec float64
	if totalEp > 0 {
		avgDurSec = (float64(totalDurSimNs) / float64(totalEp)) / 1e9
	}
	maxDurSec := float64(maxDurSimNs) / 1e9

	return NetworkBreachSummary{
		TotalEpisodes:        totalEp,
		AvgDurationSec:       avgDurSec,
		MaxDurationSec:       maxDurSec,
		TotalRawTickBreaches: totalRawBreaches,
	}
}

func (m *LoadMonitor) loop(ctx context.Context) {
	defer m.wg.Done()
	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			elapsedReal := time.Since(m.startTime)
			// elapsedReal is a time.Duration (integer nanoseconds). Multiply by timeScale for simulated nanoseconds.
			simTimeNs := int64(float64(elapsedReal) * m.timeScale)

			for _, s := range m.stores {
				load := s.ComputeLoad()

				m.mu.Lock()
				state := m.episodeStates[s.ID]

				if load > m.threshold {
					state.RawTickBreaches++
					if !state.InBreachedState {
						state.InBreachedState = true
						state.EpisodeStartSimNs = simTimeNs
						state.TotalEpisodes++
					}

					// Evaluate DecisionEngine on load breach
					if m.decisionEngine != nil {
						m.decisionEngine.EvaluateBreach(ctx, s, m.stores, simTimeNs)
					}
				} else if load < m.recoveryThreshold {
					if state.InBreachedState {
						state.InBreachedState = false
						durSimNs := simTimeNs - state.EpisodeStartSimNs
						state.TotalDurationSimNs += durSimNs
						if durSimNs > state.MaxDurationSimNs {
							state.MaxDurationSimNs = durSimNs
						}
					}
				}
				m.mu.Unlock()
			}

			// Invariant Check against real-time enqueued orders count
			if m.createdCountFn != nil {
				createdTotal := m.createdCountFn()
				if createdTotal > 0 {
					var totMerged, nudgeCount int64
					if m.decisionEngine != nil {
						sum := m.decisionEngine.GetSummary()
						totMerged = sum.TotalOrdersMerged
						nudgeCount = sum.BatchingNudgeCount
					}
					_, err := VerifyNetworkAccountingInvariant(m.stores, createdTotal, totMerged, nudgeCount)
					if err != nil {
						log.Printf("[ACCOUNTING ERROR] %v", err)
					}
				}
			}
		}
	}
}
