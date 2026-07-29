package trip

import (
	"context"
	"fmt"
	"sync"
	"time"

	"adaptive-load-orchestrator/internal/simulation"
)

type RiderSimulator struct {
	mu                 sync.RWMutex
	riders             map[string]*Rider
	store              *RedisTripStore
	proximityThreshold float64 // in kilometers (e.g. 0.8 km = 800 meters)
}

func NewRiderSimulator(store *RedisTripStore, proximityThresholdKm float64) *RiderSimulator {
	if proximityThresholdKm <= 0 {
		proximityThresholdKm = 0.8 // default 800m
	}
	return &RiderSimulator{
		riders:             make(map[string]*Rider),
		store:              store,
		proximityThreshold: proximityThresholdKm,
	}
}

func (sim *RiderSimulator) RegisterRider(r *Rider) {
	sim.mu.Lock()
	defer sim.mu.Unlock()
	if r.SpeedKmH <= 0 {
		r.SpeedKmH = 20.0 // Default 20 km/h rider speed
	}
	if r.Status == "" {
		r.Status = RiderStatusEnRoute
	}
	sim.riders[r.ID] = r
}

func (sim *RiderSimulator) GetRider(id string) (*Rider, bool) {
	sim.mu.RLock()
	defer sim.mu.RUnlock()
	r, ok := sim.riders[id]
	return r, ok
}

func (sim *RiderSimulator) Tick(ctx context.Context, deltaSimSec float64) ([]*TripEvent, error) {
	sim.mu.Lock()
	defer sim.mu.Unlock()

	var triggeredEvents []*TripEvent

	for _, r := range sim.riders {
		if r.Status != RiderStatusEnRoute {
			continue
		}

		totalDistKm := simulation.HaversineDistance(r.PickupLat, r.PickupLng, r.DestinationLat, r.DestinationLng)
		if totalDistKm <= 0.001 {
			r.Status = RiderStatusArrived
			continue
		}

		currentDistKm := simulation.HaversineDistance(r.CurrentLat, r.CurrentLng, r.DestinationLat, r.DestinationLng)
		travelStepKm := (r.SpeedKmH / 3600.0) * deltaSimSec

		if currentDistKm <= travelStepKm {
			r.CurrentLat = r.DestinationLat
			r.CurrentLng = r.DestinationLng
			r.Status = RiderStatusArrived
			currentDistKm = 0.0
		} else {
			fraction := travelStepKm / currentDistKm
			r.CurrentLat = r.CurrentLat + (r.DestinationLat-r.CurrentLat)*fraction
			r.CurrentLng = r.CurrentLng + (r.DestinationLng-r.CurrentLng)*fraction
			currentDistKm = simulation.HaversineDistance(r.CurrentLat, r.CurrentLng, r.DestinationLat, r.DestinationLng)
		}

		r.UpdatedAt = time.Now()
		etaSec := (currentDistKm / r.SpeedKmH) * 3600.0

		// Proximity check: Trigger Trip creation when within proximity threshold (e.g. <= 0.8 km / 800m)
		if currentDistKm <= sim.proximityThreshold {
			tripID := fmt.Sprintf("trip-%s", r.ID)

			existingTrip, err := sim.store.GetTrip(ctx, tripID)
			if err != nil || existingTrip == nil {
				riderName := r.Name
				if riderName == "" {
					riderName = "Rahul Sharma"
				}

				initialMembers := []TripMember{
					{
						OrderID:         "ord-mem-1",
						MemberID:        "mem-1",
						DisplayName:     "Aarav Mehta",
						FlatLocation:    "Flat 402, Tower B",
						ItemsSummary:    "Amul Taaza Milk (1L), Brown Bread",
						OrderTotalPaise: 18500,
						AvatarColor:     "#8B5CF6",
					},
					{
						OrderID:         "ord-mem-2",
						MemberID:        "mem-2",
						DisplayName:     "Priya Sharma",
						FlatLocation:    "Flat 201, Tower B",
						ItemsSummary:    "Lay's Chips (52g), Coca-Cola (750ml)",
						OrderTotalPaise: 14000,
						AvatarColor:     "#EC4899",
					},
				}

				newTrip := &Trip{
					ID:                     tripID,
					RiderID:                r.ID,
					RiderName:              riderName,
					GeofenceID:             r.DestinationGeofenceID,
					GeofenceName:           "Aravali Heights, Tower B",
					AssignedOrderCount:     len(r.AssignedOrderIDs),
					MemberOrderIDs:         []string{"ord-mem-1", "ord-mem-2"},
					Members:                initialMembers,
					BaseDeliveryFeePaise:   3500, // ₹35.00 Base Delivery Fee
					CurrentDeliveryFeePaise: 1750, // 50% split for 2 members
					DiscountPaise:          1750,
					ETASeconds:             etaSec,
					Status:                 TripStatusPooled,
					CreatedAt:              time.Now(),
					UpdatedAt:              time.Now(),
				}

				if err := sim.store.CreateOrUpdateTrip(ctx, newTrip); err == nil {
					triggeredEvents = append(triggeredEvents, &TripEvent{
						Type:                   EventTripAvailable,
						TripID:                 newTrip.ID,
						RiderID:                r.ID,
						RiderName:              riderName,
						GeofenceID:             r.DestinationGeofenceID,
						GeofenceName:           "Aravali Heights, Tower B",
						MemberCount:            len(newTrip.MemberOrderIDs),
						AssignedOrderCount:     len(r.AssignedOrderIDs),
						BaseDeliveryFeePaise:   newTrip.BaseDeliveryFeePaise,
						CurrentDeliveryFeePaise: newTrip.CurrentDeliveryFeePaise,
						DiscountPaise:          newTrip.DiscountPaise,
						ETASeconds:             etaSec,
						Timestamp:              time.Now(),
					})
				}
			} else {
				// Update ETA for active trip
				existingTrip.ETASeconds = etaSec
				_ = sim.store.CreateOrUpdateTrip(ctx, existingTrip)
			}
		}
	}

	return triggeredEvents, nil
}
