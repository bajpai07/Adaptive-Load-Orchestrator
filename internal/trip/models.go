package trip

import (
	"time"
)

type Location struct {
	Lat float64 `json:"lat"`
	Lng float64 `json:"lng"`
}

type RiderStatus string

const (
	RiderStatusPicking   RiderStatus = "PICKING"
	RiderStatusEnRoute   RiderStatus = "EN_ROUTE"
	RiderStatusArrived   RiderStatus = "ARRIVED"
	RiderStatusCompleted RiderStatus = "COMPLETED"
)

type Rider struct {
	ID                    string      `json:"id"`
	CurrentLat            float64     `json:"current_lat"`
	CurrentLng            float64     `json:"current_lng"`
	PickupLat             float64     `json:"pickup_lat"`
	PickupLng             float64     `json:"pickup_lng"`
	DestinationGeofenceID string      `json:"destination_geofence_id"`
	DestinationLat        float64     `json:"destination_lat"`
	DestinationLng        float64     `json:"destination_lng"`
	AssignedOrderIDs      []string    `json:"assigned_order_ids"`
	Status                RiderStatus `json:"status"`
	SpeedKmH              float64     `json:"speed_kmh"`
	UpdatedAt             time.Time   `json:"updated_at"`
}

type TripStatus string

const (
	TripStatusAvailable  TripStatus = "AVAILABLE"
	TripStatusPooled     TripStatus = "POOLED"
	TripStatusDispatched TripStatus = "DISPATCHED"
	TripStatusCompleted  TripStatus = "COMPLETED"
)

type Trip struct {
	ID                     string     `json:"id"`
	RiderID                string     `json:"rider_id"`
	GeofenceID             string     `json:"geofence_id"`
	MemberOrderIDs         []string   `json:"member_order_ids"`
	BaseDeliveryFeePaise   int64      `json:"base_delivery_fee_paise"` // 3500 Paise = ₹35.00
	CurrentDeliveryFeePaise int64      `json:"current_delivery_fee_paise"`
	DiscountPaise          int64      `json:"discount_paise"`
	ETASeconds             float64    `json:"eta_seconds"`
	Status                 TripStatus `json:"status"`
	CreatedAt              time.Time  `json:"created_at"`
	UpdatedAt              time.Time  `json:"updated_at"`
}

type TripEventType string

const (
	EventTripAvailable       TripEventType = "TRIP_AVAILABLE"
	EventTripUpdated         TripEventType = "TRIP_UPDATED"
	EventTripDiscountChanged TripEventType = "TRIP_DISCOUNT_CHANGED"
	EventTripCompleted       TripEventType = "TRIP_COMPLETED"
)

type TripEvent struct {
	Type                   TripEventType `json:"type"`
	TripID                 string        `json:"trip_id"`
	RiderID                string        `json:"rider_id"`
	GeofenceID             string        `json:"geofence_id"`
	MemberCount            int           `json:"member_count"`
	BaseDeliveryFeePaise   int64         `json:"base_delivery_fee_paise"`
	CurrentDeliveryFeePaise int64         `json:"current_delivery_fee_paise"`
	DiscountPaise          int64         `json:"discount_paise"`
	ETASeconds             float64       `json:"eta_seconds"`
	Timestamp              time.Time     `json:"timestamp"`
}

// CalculateDeliveryFee computes the pooled delivery fee per member based on total pooled order count.
// Formula:
//   1 Order  => Full fee (BaseDeliveryFeePaise, e.g. 3,500 Paise = ₹35.00)
//   2 Orders => Half fee (BaseDeliveryFeePaise / 2, e.g. 1,750 Paise = ₹17.50)
//   3+ Orders => Free (0 Paise = ₹0.00, 100% Waived)
func CalculateDeliveryFee(baseFeePaise int64, memberCount int) (currentFeePaise int64, discountPaise int64) {
	if memberCount <= 1 {
		return baseFeePaise, 0
	} else if memberCount == 2 {
		currentFeePaise = baseFeePaise / 2
		discountPaise = baseFeePaise - currentFeePaise
		return currentFeePaise, discountPaise
	} else {
		return 0, baseFeePaise
	}
}
