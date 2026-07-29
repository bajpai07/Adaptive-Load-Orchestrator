package trip

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

const joinTripLuaScript = `
local tripKey = KEYS[1]
local orderID = ARGV[1]
local baseFeePaise = tonumber(ARGV[2])

local tripData = redis.call('GET', tripKey)
if not tripData then
    return redis.error_reply("TRIP_NOT_FOUND")
end

local trip = cjson.decode(tripData)
local exists = false
for _, existingID in ipairs(trip.member_order_ids) do
    if existingID == orderID then
        exists = true
        break
    end
end

if not exists then
    table.insert(trip.member_order_ids, orderID)
    local count = #trip.member_order_ids
    local fee = baseFeePaise
    local discount = 0
    if count == 2 then
        fee = math.floor(baseFeePaise / 2)
        discount = baseFeePaise - fee
    elseif count >= 3 then
        fee = 0
        discount = baseFeePaise
    end
    trip.current_delivery_fee_paise = fee
    trip.discount_paise = discount
    trip.status = "POOLED"
    
    local updatedData = cjson.encode(trip)
    redis.call('SET', tripKey, updatedData)
    return updatedData
end

return tripData
`

type RedisTripStore struct {
	rdb *redis.Client
}

func NewRedisTripStore(rdb *redis.Client) *RedisTripStore {
	return &RedisTripStore{rdb: rdb}
}

func (s *RedisTripStore) CreateOrUpdateTrip(ctx context.Context, t *Trip) error {
	data, err := json.Marshal(t)
	if err != nil {
		return fmt.Errorf("failed to marshal trip: %w", err)
	}

	key := fmt.Sprintf("trip:%s", t.ID)
	geofenceKey := fmt.Sprintf("geofence_trip:%s", t.GeofenceID)

	pipe := s.rdb.Pipeline()
	pipe.Set(ctx, key, data, 1*time.Hour)
	pipe.Set(ctx, geofenceKey, t.ID, 1*time.Hour)
	_, err = pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to save trip to Redis: %w", err)
	}

	// Publish TRIP_AVAILABLE event
	ev := TripEvent{
		Type:                   EventTripAvailable,
		TripID:                 t.ID,
		RiderID:                t.RiderID,
		GeofenceID:             t.GeofenceID,
		MemberCount:            len(t.MemberOrderIDs),
		BaseDeliveryFeePaise:   t.BaseDeliveryFeePaise,
		CurrentDeliveryFeePaise: t.CurrentDeliveryFeePaise,
		DiscountPaise:          t.DiscountPaise,
		ETASeconds:             t.ETASeconds,
		Timestamp:              time.Now(),
	}

	evData, _ := json.Marshal(ev)
	s.rdb.Publish(ctx, "trip_events", evData)

	return nil
}

func (s *RedisTripStore) GetTrip(ctx context.Context, tripID string) (*Trip, error) {
	key := fmt.Sprintf("trip:%s", tripID)
	val, err := s.rdb.Get(ctx, key).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get trip %s: %w", tripID, err)
	}

	var t Trip
	if err := json.Unmarshal([]byte(val), &t); err != nil {
		return nil, fmt.Errorf("failed to unmarshal trip: %w", err)
	}

	return &t, nil
}

func (s *RedisTripStore) GetActiveTripForGeofence(ctx context.Context, geofenceID string) (*Trip, error) {
	geofenceKey := fmt.Sprintf("geofence_trip:%s", geofenceID)
	tripID, err := s.rdb.Get(ctx, geofenceKey).Result()
	if err != nil {
		return nil, err
	}
	return s.GetTrip(ctx, tripID)
}

func (s *RedisTripStore) JoinTripAtomic(ctx context.Context, tripID string, orderID string, baseFeePaise int64) (*Trip, error) {
	key := fmt.Sprintf("trip:%s", tripID)

	res, err := s.rdb.Eval(ctx, joinTripLuaScript, []string{key}, orderID, baseFeePaise).Result()
	if err != nil {
		return nil, fmt.Errorf("atomic join trip failed: %w", err)
	}

	strVal, ok := res.(string)
	if !ok {
		return nil, fmt.Errorf("unexpected script return type: %T", res)
	}

	var updatedTrip Trip
	if err := json.Unmarshal([]byte(strVal), &updatedTrip); err != nil {
		return nil, fmt.Errorf("failed to parse updated trip JSON: %w", err)
	}

	// Publish TRIP_UPDATED event
	ev := TripEvent{
		Type:                   EventTripUpdated,
		TripID:                 updatedTrip.ID,
		RiderID:                updatedTrip.RiderID,
		GeofenceID:             updatedTrip.GeofenceID,
		MemberCount:            len(updatedTrip.MemberOrderIDs),
		BaseDeliveryFeePaise:   updatedTrip.BaseDeliveryFeePaise,
		CurrentDeliveryFeePaise: updatedTrip.CurrentDeliveryFeePaise,
		DiscountPaise:          updatedTrip.DiscountPaise,
		ETASeconds:             updatedTrip.ETASeconds,
		Timestamp:              time.Now(),
	}

	evData, _ := json.Marshal(ev)
	s.rdb.Publish(ctx, "trip_events", evData)

	return &updatedTrip, nil
}

func (s *RedisTripStore) SubscribeTripEvents(ctx context.Context) <-chan *TripEvent {
	pubsub := s.rdb.Subscribe(ctx, "trip_events")
	out := make(chan *TripEvent, 100)

	go func() {
		defer pubsub.Close()
		defer close(out)
		ch := pubsub.Channel()
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-ch:
				if !ok {
					return
				}
				var ev TripEvent
				if err := json.Unmarshal([]byte(msg.Payload), &ev); err == nil {
					select {
					case out <- &ev:
					default:
					}
				}
			}
		}
	}()

	return out
}
