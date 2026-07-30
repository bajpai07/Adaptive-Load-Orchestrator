package groupcart

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type CartStore interface {
	CreateOrGetActiveCart(ctx context.Context, geofenceID string, hostMember Member, centroid Location, radiusMeters float64, ttl time.Duration, unlockThresholdPaise int64) (*GroupCart, error)
	GetCart(ctx context.Context, cartID string) (*GroupCart, error)
	AddItemAtomic(ctx context.Context, cartID string, item CartItem) (*GroupCart, bool, error)
	RemoveItemAtomic(ctx context.Context, cartID string, itemID string) (*GroupCart, error)
	FinalizeCart(ctx context.Context, cartID string) (*GroupCart, error)
	SubscribeCartEvents(ctx context.Context, cartID string) <-chan *CartEvent
}

type RedisCartStore struct {
	client *redis.Client
}

func NewRedisCartStore(client *redis.Client) *RedisCartStore {
	return &RedisCartStore{client: client}
}

func (s *RedisCartStore) Client() *redis.Client {
	return s.client
}

func (s *RedisCartStore) SubscribeCartEvents(ctx context.Context, cartID string) <-chan *CartEvent {
	ch := make(chan *CartEvent, 100)
	pubsub := s.client.Subscribe(ctx, fmt.Sprintf("cart_events:%s", cartID))

	go func() {
		defer close(ch)
		defer pubsub.Close()

		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-pubsub.Channel():
				if !ok {
					return
				}
				var ev CartEvent
				if err := json.Unmarshal([]byte(msg.Payload), &ev); err == nil {
					ch <- &ev
				}
			}
		}
	}()

	return ch
}

func (s *RedisCartStore) CreateOrGetActiveCart(ctx context.Context, geofenceID string, hostMember Member, centroid Location, radiusMeters float64, ttl time.Duration, unlockThresholdPaise int64) (*GroupCart, error) {
	activeCartKey := fmt.Sprintf("geofence_cart:%s", geofenceID)
	existingCartID, err := s.client.Get(ctx, activeCartKey).Result()
	if err == nil && existingCartID != "" {
		cart, err := s.GetCart(ctx, existingCartID)
		if err == nil && cart != nil && cart.Status == CartStatusActive {
			memberExists := false
			for _, m := range cart.Members {
				if m.ID == hostMember.ID {
					memberExists = true
					break
				}
			}
			if !memberExists {
				cart.Members = append(cart.Members, hostMember)
				data, _ := json.Marshal(cart)
				_ = s.client.Set(ctx, fmt.Sprintf("cart:%s", cart.ID), data, 0).Err()
			}
			return cart, nil
		}
	}

	cartID := fmt.Sprintf("cart-%s-%d", geofenceID, time.Now().UnixNano())
	now := time.Now()

	members := []Member{hostMember}
	demoItems := make([]CartItem, 0)
	var totalPaise int64 = 0

	// Pre-seed demo members and items ONLY for the live demo geofence ("geofence-aravali")
	if geofenceID == "geofence-aravali" {
		demoMember1 := Member{ID: "mem-1", DisplayName: "Aarav Mehta (Flat 402)"}
		demoMember2 := Member{ID: "mem-2", DisplayName: "Priya Sharma (Flat 201)"}

		if hostMember.ID != demoMember1.ID {
			members = append(members, demoMember1)
		}
		if hostMember.ID != demoMember2.ID {
			members = append(members, demoMember2)
		}

		demoItems = []CartItem{
			{ID: "item-seed-1", SKU: "sku-milk-1", Name: "Amul Taaza Milk (1L)", PricePaise: 7500, AddedByMemberID: "mem-1", AddedAt: now},
			{ID: "item-seed-2", SKU: "sku-bread-1", Name: "Brown Bread", PricePaise: 4000, AddedByMemberID: "mem-1", AddedAt: now},
			{ID: "item-seed-3", SKU: "sku-chips-1", Name: "Lay's Magic Masala (52g)", PricePaise: 2000, AddedByMemberID: "mem-2", AddedAt: now},
			{ID: "item-seed-4", SKU: "sku-cola-1", Name: "Coca-Cola Bottle (750ml)", PricePaise: 4000, AddedByMemberID: "mem-2", AddedAt: now},
		}
		totalPaise = 17500
	}

	unlocked := totalPaise >= unlockThresholdPaise

	cart := &GroupCart{
		ID:                   cartID,
		GeofenceID:           geofenceID,
		GeofenceCentroid:     centroid,
		GeofenceRadiusMeters: radiusMeters,
		Members:              members,
		Items:                demoItems,
		CreatedAt:            now,
		ExpiresAt:            now.Add(ttl),
		UnlockThresholdPaise: unlockThresholdPaise,
		Unlocked:             unlocked,
		TotalPaise:           totalPaise,
		Status:               CartStatusActive,
	}

	data, err := json.Marshal(cart)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal cart: %w", err)
	}

	pipe := s.client.Pipeline()
	pipe.Set(ctx, fmt.Sprintf("cart:%s", cartID), data, ttl)
	pipe.Set(ctx, activeCartKey, cartID, ttl)
	_, err = pipe.Exec(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create cart in Redis: %w", err)
	}

	return cart, nil
}

func (s *RedisCartStore) GetCart(ctx context.Context, cartID string) (*GroupCart, error) {
	key := fmt.Sprintf("cart:%s", cartID)
	data, err := s.client.Get(ctx, key).Result()
	if err == redis.Nil {
		return nil, fmt.Errorf("cart not found: %s", cartID)
	} else if err != nil {
		return nil, err
	}

	var cart GroupCart
	if err := json.Unmarshal([]byte(data), &cart); err != nil {
		return nil, fmt.Errorf("failed to unmarshal cart: %w", err)
	}
	return &cart, nil
}

const addItemLuaScript = `
local cartKey = KEYS[1]
local itemData = ARGV[1]

local data = redis.call("GET", cartKey)
if not data then
    return redis.error_reply("CART_NOT_FOUND")
end

local cart = cjson.decode(data)
if cart.status ~= "ACTIVE" then
    return redis.error_reply("CART_NOT_ACTIVE")
end

local newItem = cjson.decode(itemData)
local itemPrice = tonumber(newItem.price_paise) or 0

table.insert(cart.items, newItem)
cart.total_paise = cart.total_paise + itemPrice

local justUnlocked = false
if cart.total_paise >= cart.unlock_threshold_paise and not cart.unlocked then
    cart.unlocked = true
    justUnlocked = true
end

local updatedData = cjson.encode(cart)
redis.call("SET", cartKey, updatedData)

local result = {}
result[1] = updatedData
if justUnlocked then
    result[2] = 1
else
    result[2] = 0
end

return result
`

func (s *RedisCartStore) AddItemAtomic(ctx context.Context, cartID string, item CartItem) (*GroupCart, bool, error) {
	itemBytes, err := json.Marshal(item)
	if err != nil {
		return nil, false, fmt.Errorf("failed to marshal item: %w", err)
	}

	cartKey := fmt.Sprintf("cart:%s", cartID)
	res, err := s.client.Eval(ctx, addItemLuaScript, []string{cartKey}, string(itemBytes)).Result()
	if err != nil {
		return nil, false, err
	}

	resSlice, ok := res.([]interface{})
	if !ok || len(resSlice) < 2 {
		return nil, false, fmt.Errorf("invalid Lua script return format")
	}

	cartJSON, ok := resSlice[0].(string)
	if !ok {
		return nil, false, fmt.Errorf("invalid cart JSON format in Lua result")
	}

	unlockedFlag, _ := resSlice[1].(int64)
	justUnlocked := unlockedFlag == 1

	var cart GroupCart
	if err := json.Unmarshal([]byte(cartJSON), &cart); err != nil {
		return nil, false, fmt.Errorf("failed to unmarshal updated cart: %w", err)
	}

	channel := fmt.Sprintf("cart_events:%s", cartID)
	eventPayload, _ := json.Marshal(&CartEvent{
		Type:                 EventCartUpdated,
		CartID:               cartID,
		MemberID:             item.AddedByMemberID,
		Item:                 &item,
		TotalPaise:           cart.TotalPaise,
		UnlockThresholdPaise: cart.UnlockThresholdPaise,
		Unlocked:             cart.Unlocked,
		Status:               cart.Status,
		Timestamp:            time.Now(),
	})
	_ = s.client.Publish(ctx, channel, string(eventPayload)).Err()

	if justUnlocked {
		unlockPayload, _ := json.Marshal(&CartEvent{
			Type:                 EventCartUnlocked,
			CartID:               cartID,
			TotalPaise:           cart.TotalPaise,
			UnlockThresholdPaise: cart.UnlockThresholdPaise,
			Unlocked:             true,
			Timestamp:            time.Now(),
		})
		_ = s.client.Publish(ctx, channel, string(unlockPayload)).Err()
	}

	return &cart, justUnlocked, nil
}

func (s *RedisCartStore) RemoveItemAtomic(ctx context.Context, cartID string, itemID string) (*GroupCart, error) {
	cart, err := s.GetCart(ctx, cartID)
	if err != nil {
		return nil, err
	}

	if cart.Status != CartStatusActive {
		return nil, fmt.Errorf("cart is not active: %s", cart.Status)
	}

	newItems := make([]CartItem, 0, len(cart.Items))
	var removedCost int64

	for _, item := range cart.Items {
		if item.ID == itemID {
			removedCost += item.PricePaise
		} else {
			newItems = append(newItems, item)
		}
	}

	cart.Items = newItems
	cart.TotalPaise -= removedCost

	if cart.TotalPaise < cart.UnlockThresholdPaise {
		cart.Unlocked = false
	}

	data, err := json.Marshal(cart)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal cart: %w", err)
	}

	key := fmt.Sprintf("cart:%s", cartID)
	if err := s.client.Set(ctx, key, data, 0).Err(); err != nil {
		return nil, err
	}

	channel := fmt.Sprintf("cart_events:%s", cartID)
	eventPayload, _ := json.Marshal(&CartEvent{
		Type:                 EventCartUpdated,
		CartID:               cartID,
		TotalPaise:           cart.TotalPaise,
		UnlockThresholdPaise: cart.UnlockThresholdPaise,
		Unlocked:             cart.Unlocked,
		Status:               cart.Status,
		Timestamp:            time.Now(),
	})
	_ = s.client.Publish(ctx, channel, string(eventPayload)).Err()

	return cart, nil
}

func (s *RedisCartStore) FinalizeCart(ctx context.Context, cartID string) (*GroupCart, error) {
	cart, err := s.GetCart(ctx, cartID)
	if err != nil {
		return nil, err
	}

	cart.Status = CartStatusFinalized
	data, err := json.Marshal(cart)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal cart: %w", err)
	}

	key := fmt.Sprintf("cart:%s", cartID)
	if err := s.client.Set(ctx, key, data, 0).Err(); err != nil {
		return nil, err
	}

	channel := fmt.Sprintf("cart_events:%s", cartID)
	eventPayload, _ := json.Marshal(&CartEvent{
		Type:                 EventCartFinalized,
		CartID:               cartID,
		TotalPaise:           cart.TotalPaise,
		UnlockThresholdPaise: cart.UnlockThresholdPaise,
		Unlocked:             cart.Unlocked,
		Status:               cart.Status,
		Timestamp:            time.Now(),
	})
	_ = s.client.Publish(ctx, channel, string(eventPayload)).Err()

	return cart, nil
}
