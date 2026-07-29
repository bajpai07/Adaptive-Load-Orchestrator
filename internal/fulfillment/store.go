package fulfillment

import (
	"context"
	"sync/atomic"
	"time"

	"adaptive-load-orchestrator/internal/simulation"
)

type Store struct {
	ID                   string
	Lat                  float64
	Lng                  float64
	Location             simulation.Location
	Capacity             int
	ArrivalRate          float64 // lambda (orders/sec)
	ServiceRate          float64 // mu (orders/sec per picker)
	PickerCount          int
	CurrentQueue         *OrderQueue
	pickerPool           *PickerPool
	receivedCount        int64
	completedCount       int64
	reRoutedInCount      int64
	reRoutedOutCount     int64
	activePickingCount   int64
	cumulativeServiceNs  int64
	cumulativeSystemNs   int64
	lastReRouteSimTimeNs int64
	startTime            time.Time
	timeScale            float64
}

func NewStore(id string, lat, lng float64, capacity int, arrivalRate, serviceRate float64, pickerCount int) *Store {
	q := NewOrderQueue()
	loc := simulation.Location{Lat: lat, Lng: lng}
	s := &Store{
		ID:           id,
		Lat:          lat,
		Lng:          lng,
		Location:     loc,
		Capacity:     capacity,
		ArrivalRate:  arrivalRate,
		ServiceRate:  serviceRate,
		PickerCount:  pickerCount,
		CurrentQueue: q,
		timeScale:    1.0,
	}
	s.pickerPool = NewPickerPool(s, 1, 1.0)
	return s
}

func (s *Store) SetPickerParams(seed int64, timeScale float64) {
	s.timeScale = timeScale
	s.pickerPool = NewPickerPool(s, seed, timeScale)
}

func (s *Store) SetStartTime(t time.Time, timeScale float64) {
	s.startTime = t
	if timeScale > 0 {
		s.timeScale = timeScale
	}
}

func (s *Store) SimNow() time.Time {
	if s.startTime.IsZero() {
		return time.Now()
	}
	elapsedReal := time.Since(s.startTime)
	return s.startTime.Add(time.Duration(float64(elapsedReal) * s.timeScale))
}

func (s *Store) Start(ctx context.Context) {
	s.pickerPool.Start(ctx)
}

func (s *Store) Stop() {
	s.pickerPool.Stop()
}

func (s *Store) Queue() *OrderQueue {
	return s.CurrentQueue
}

func (s *Store) WaitingOrdersCount() int {
	return s.CurrentQueue.Len()
}

func (s *Store) EnqueueOrder(order *Order) {
	s.CurrentQueue.Push(order)
}

func (s *Store) IncrReceived() {
	atomic.AddInt64(&s.receivedCount, 1)
}

func (s *Store) IncrReRoutedIn() {
	atomic.AddInt64(&s.reRoutedInCount, 1)
}

func (s *Store) IncrReRoutedOut() {
	atomic.AddInt64(&s.reRoutedOutCount, 1)
}

func (s *Store) LastReRouteSimTimeNs() int64 {
	return atomic.LoadInt64(&s.lastReRouteSimTimeNs)
}

func (s *Store) SetLastReRouteSimTimeNs(val int64) {
	atomic.StoreInt64(&s.lastReRouteSimTimeNs, val)
}

func (s *Store) IncrPicking() {
	atomic.AddInt64(&s.activePickingCount, 1)
}

func (s *Store) DecrPickingAndIncrCompleted(serviceNs, systemNs int64) {
	atomic.AddInt64(&s.activePickingCount, -1)
	atomic.AddInt64(&s.completedCount, 1)
	atomic.AddInt64(&s.cumulativeServiceNs, serviceNs)
	atomic.AddInt64(&s.cumulativeSystemNs, systemNs)
}

func (s *Store) CompletedCount() int64 {
	return atomic.LoadInt64(&s.completedCount)
}

func (s *Store) ActivePickingCount() int64 {
	return atomic.LoadInt64(&s.activePickingCount)
}

func (s *Store) ComputeLoad() float64 {
	queueLen := float64(s.CurrentQueue.Len())
	activePicking := float64(atomic.LoadInt64(&s.activePickingCount))
	currentItems := queueLen + activePicking
	return currentItems / float64(s.Capacity)
}

func (s *Store) Stats() (received, completed, reRoutedIn, reRoutedOut, inQueue, inPicking int64, avgServiceSec, avgSystemSec float64) {
	received = atomic.LoadInt64(&s.receivedCount)
	completed = atomic.LoadInt64(&s.completedCount)
	reRoutedIn = atomic.LoadInt64(&s.reRoutedInCount)
	reRoutedOut = atomic.LoadInt64(&s.reRoutedOutCount)
	inQueue = int64(s.CurrentQueue.Len())
	inPicking = atomic.LoadInt64(&s.activePickingCount)

	if completed > 0 {
		totServiceNs := atomic.LoadInt64(&s.cumulativeServiceNs)
		totSystemNs := atomic.LoadInt64(&s.cumulativeSystemNs)
		avgServiceSec = (float64(totServiceNs) / float64(completed)) / 1e9
		avgSystemSec = (float64(totSystemNs) / float64(completed)) / 1e9
	}
	return
}

func (s *Store) StuckQueueStats(now time.Time) (minWaitSec, maxWaitSec, avgWaitSec float64, count int) {
	count, minWaitSec, maxWaitSec, avgWaitSec = s.CurrentQueue.WaitingOrdersStats(now)
	return minWaitSec, maxWaitSec, avgWaitSec, count
}
