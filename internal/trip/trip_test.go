package trip

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func setupTestTripStore(t *testing.T) (*RedisTripStore, *miniredis.Miniredis, func()) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("Failed to start miniredis: %v", err)
	}

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	store := NewRedisTripStore(rdb)

	cleanup := func() {
		rdb.Close()
		mr.Close()
	}

	return store, mr, cleanup
}

func TestDeliveryFeeCalculation(t *testing.T) {
	baseFee := int64(3500) // ₹35.00

	// 1 Member: Full fee
	fee, disc := CalculateDeliveryFee(baseFee, 1)
	if fee != 3500 || disc != 0 {
		t.Errorf("Expected 1 member fee=3500 disc=0, got fee=%d disc=%d", fee, disc)
	}

	// 2 Members: 50% split (₹17.50 each)
	fee, disc = CalculateDeliveryFee(baseFee, 2)
	if fee != 1750 || disc != 1750 {
		t.Errorf("Expected 2 member fee=1750 disc=1750, got fee=%d disc=%d", fee, disc)
	}

	// 3+ Members: 100% waived (₹0.00 fee)
	fee, disc = CalculateDeliveryFee(baseFee, 3)
	if fee != 0 || disc != 3500 {
		t.Errorf("Expected 3 member fee=0 disc=3500, got fee=%d disc=%d", fee, disc)
	}
}

func TestRiderMovementAndProximityDetection(t *testing.T) {
	store, _, cleanup := setupTestTripStore(t)
	defer cleanup()

	sim := NewRiderSimulator(store, 0.8) // 0.8 km = 800m threshold

	rider := &Rider{
		ID:                    "rider-1",
		CurrentLat:            28.6315,
		CurrentLng:            77.2167,
		PickupLat:             28.6315,
		PickupLng:             77.2167,
		DestinationGeofenceID: "geofence-store-1",
		DestinationLat:        28.6450,
		DestinationLng:        77.2167,
		AssignedOrderIDs:      []string{"ord-1"},
		Status:                RiderStatusEnRoute,
		SpeedKmH:              30.0,
	}

	sim.RegisterRider(rider)
	ctx := context.Background()

	// Initial check: > 800m away, no trip created
	events, err := sim.Tick(ctx, 1.0)
	if err != nil {
		t.Fatalf("Tick failed: %v", err)
	}
	if len(events) != 0 {
		t.Errorf("Expected 0 events when far away, got %d", len(events))
	}

	// Tick forward 120 seconds (120s at 30km/h = 1.0 km traveled => 0.5 km remaining <= 0.8 km threshold)
	events, err = sim.Tick(ctx, 120.0)
	if err != nil {
		t.Fatalf("Tick failed: %v", err)
	}

	if len(events) != 1 {
		t.Fatalf("Expected 1 TRIP_AVAILABLE event on proximity threshold crossing, got %d", len(events))
	}

	ev := events[0]
	if ev.Type != EventTripAvailable || ev.RiderID != "rider-1" {
		t.Errorf("Unexpected event payload: %+v", ev)
	}

	// Verify trip saved in Redis
	trip, err := store.GetTrip(ctx, "trip-rider-1")
	if err != nil || trip == nil {
		t.Fatalf("Failed to fetch created trip from Redis: %v", err)
	}

	if trip.Status != TripStatusAvailable && trip.Status != TripStatusPooled {
		t.Errorf("Expected TripStatusAvailable or TripStatusPooled, got %s", trip.Status)
	}
}

func TestRedisTripStore_ConcurrentJoins(t *testing.T) {
	store, _, cleanup := setupTestTripStore(t)
	defer cleanup()

	ctx := context.Background()
	tripID := "trip-concurrent-test"

	initialTrip := &Trip{
		ID:                     tripID,
		RiderID:                "rider-test",
		GeofenceID:             "geofence-test",
		MemberOrderIDs:         []string{"ord-initial"},
		BaseDeliveryFeePaise:   3500,
		CurrentDeliveryFeePaise: 3500,
		DiscountPaise:          0,
		Status:                 TripStatusAvailable,
		CreatedAt:              time.Now(),
	}

	if err := store.CreateOrUpdateTrip(ctx, initialTrip); err != nil {
		t.Fatalf("Failed to create initial trip: %v", err)
	}

	concurrentClients := 20
	var wg sync.WaitGroup
	errChan := make(chan error, concurrentClients)

	for i := 1; i <= concurrentClients; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			orderID := fmt.Sprintf("ord-concurrent-%d", idx)
			memberObj := &TripMember{
				OrderID:      orderID,
				MemberID:     fmt.Sprintf("mem-concurrent-%d", idx),
				DisplayName:  fmt.Sprintf("Member %d", idx),
				FlatLocation: fmt.Sprintf("Flat %d01", idx),
			}
			_, err := store.JoinTripAtomic(ctx, tripID, orderID, 3500, memberObj)
			if err != nil {
				errChan <- err
			}
		}(i)
	}

	wg.Wait()
	close(errChan)

	for err := range errChan {
		t.Errorf("Concurrent join error: %v", err)
	}

	finalTrip, err := store.GetTrip(ctx, tripID)
	if err != nil {
		t.Fatalf("Failed to fetch final trip: %v", err)
	}

	// 1 initial order + 20 concurrent joins = 21 orders total
	expectedOrderCount := concurrentClients + 1
	if len(finalTrip.MemberOrderIDs) != expectedOrderCount {
		t.Errorf("CONCURRENCY FAILURE: Expected %d member orders, got %d", expectedOrderCount, len(finalTrip.MemberOrderIDs))
	}

	// 21 orders >= 3 members => 100% Fee Waived (0 Paise fee, 3500 Paise discount)
	if finalTrip.CurrentDeliveryFeePaise != 0 {
		t.Errorf("Expected final fee = 0 Paise (FREE), got %d Paise", finalTrip.CurrentDeliveryFeePaise)
	}

	if finalTrip.DiscountPaise != 3500 {
		t.Errorf("Expected final discount = 3500 Paise (₹35.00), got %d Paise", finalTrip.DiscountPaise)
	}
}

func TestJoinTripAtomic_IdempotencyDeduplication_3DistinctMembers(t *testing.T) {
	store, _, cleanup := setupTestTripStore(t)
	defer cleanup()

	ctx := context.Background()
	tripID := "trip-idempotency-test"

	initialMembers := []TripMember{
		{
			OrderID:         "ord-mem-1",
			MemberID:        "mem-1",
			DisplayName:     "Aarav Mehta",
			FlatLocation:    "Flat 402, Tower B",
			ItemsSummary:    "Amul Taaza Milk (1L), Brown Bread",
			OrderTotalPaise: 11500,
			AvatarColor:     "#8B5CF6",
		},
		{
			OrderID:         "ord-mem-2",
			MemberID:        "mem-2",
			DisplayName:     "Priya Sharma",
			FlatLocation:    "Flat 201, Tower B",
			ItemsSummary:    "Lay's Chips (52g), Coca-Cola (750ml)",
			OrderTotalPaise: 6000,
			AvatarColor:     "#EC4899",
		},
	}

	initialTrip := &Trip{
		ID:                     tripID,
		RiderID:                "rider-101",
		RiderName:              "Rahul Sharma",
		GeofenceID:             "geofence-aravali",
		GeofenceName:           "Aravali Heights, Tower B",
		AssignedOrderCount:     4,
		MemberOrderIDs:         []string{"ord-mem-1", "ord-mem-2"},
		Members:                initialMembers,
		BaseDeliveryFeePaise:   3500,
		CurrentDeliveryFeePaise: 1750,
		DiscountPaise:          1750,
		Status:                 TripStatusPooled,
		CreatedAt:              time.Now(),
	}

	if err := store.CreateOrUpdateTrip(ctx, initialTrip); err != nil {
		t.Fatalf("Failed to create initial trip: %v", err)
	}

	newMember3 := &TripMember{
		OrderID:         "ord-web-3",
		MemberID:        "mem-web-3",
		DisplayName:     "Vikram Kumar",
		FlatLocation:    "Flat 304, Tower B",
		ItemsSummary:    "Organic Eggs (6-pack), Greek Yogurt",
		OrderTotalPaise: 22000,
		AvatarColor:     "#10B981",
	}

	// First join attempt for 3rd member
	updatedTrip, err := store.JoinTripAtomic(ctx, tripID, "ord-web-3", 3500, newMember3)
	if err != nil {
		t.Fatalf("EXPECTED SUCCESS: 3rd member join failed: %v", err)
	}

	if len(updatedTrip.Members) != 3 {
		t.Fatalf("Expected 3 distinct members in Members array, got %d", len(updatedTrip.Members))
	}

	// Verify all 3 member names are DISTINCT and preserved!
	names := []string{updatedTrip.Members[0].DisplayName, updatedTrip.Members[1].DisplayName, updatedTrip.Members[2].DisplayName}
	if names[0] != "Aarav Mehta" || names[1] != "Priya Sharma" || names[2] != "Vikram Kumar" {
		t.Errorf("MEMBER CORRUPTION DETECTED! Expected [Aarav Mehta, Priya Sharma, Vikram Kumar], got %v", names)
	}

	// 3 members => 100% Fee Waived (0 Paise fee, 3500 Paise discount)
	if updatedTrip.CurrentDeliveryFeePaise != 0 {
		t.Errorf("Expected fee waived (0 Paise), got %d Paise", updatedTrip.CurrentDeliveryFeePaise)
	}

	// RETRY TEST: Perform 5 repeated retries with the SAME member_id and order_id!
	for i := 0; i < 5; i++ {
		retriedTrip, err := store.JoinTripAtomic(ctx, tripID, "ord-web-3", 3500, newMember3)
		if err != nil {
			t.Fatalf("Retry %d failed: %v", i+1, err)
		}
		if len(retriedTrip.Members) != 3 {
			t.Errorf("IDEMPOTENCY FAILURE: Expected 3 members after retry, got %d (Duplicate inserted!)", len(retriedTrip.Members))
		}
	}
}
