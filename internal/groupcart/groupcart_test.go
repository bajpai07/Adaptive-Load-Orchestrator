package groupcart

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTestStore(t *testing.T) (*RedisCartStore, *miniredis.Miniredis, func()) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})
	store := NewRedisCartStore(rdb)

	cleanup := func() {
		rdb.Close()
		mr.Close()
	}
	return store, mr, cleanup
}

func TestGroupCart_ConcurrentItemAdds(t *testing.T) {
	store, _, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()
	member := Member{ID: "mem-1", DisplayName: "Alice"}
	cart, err := store.CreateOrGetActiveCart(ctx, "geofence-test", member, Location{28.6, 77.2}, 500, 30*time.Minute, 20000)
	require.NoError(t, err)

	numWriters := 50
	pricePerItem := int64(150) // 150 Paise per item
	var wg sync.WaitGroup

	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			item := CartItem{
				ID:              fmt.Sprintf("item-concurrent-%d", workerID),
				SKU:             fmt.Sprintf("SKU-%d", workerID),
				Name:            fmt.Sprintf("Item %d", workerID),
				PricePaise:      pricePerItem,
				AddedByMemberID: "mem-1",
				AddedAt:         time.Now(),
			}
			_, _, err := store.AddItemAtomic(ctx, cart.ID, item)
			assert.NoError(t, err)
		}(i)
	}

	wg.Wait()

	// Verify cart state
	finalCart, err := store.GetCart(ctx, cart.ID)
	require.NoError(t, err)

	expectedItemsCount := numWriters
	expectedTotalPaise := int64(numWriters) * pricePerItem

	assert.Equal(t, expectedItemsCount, len(finalCart.Items), "Final item count must equal number of concurrent writes attempted (no lost updates)")
	assert.Equal(t, expectedTotalPaise, finalCart.TotalPaise, "Final running total price must match exact sum of item prices")
}

func TestGroupCart_ConcurrentItemAdds_20RunsLoop(t *testing.T) {
	// Execute the 50-concurrent-writer test 20 consecutive times to prove race-condition freedom
	for run := 1; run <= 20; run++ {
		t.Run(fmt.Sprintf("Run_%d", run), func(t *testing.T) {
			TestGroupCart_ConcurrentItemAdds(t)
		})
	}
}

func TestGroupCart_UnlockEventFiresExactlyOnce(t *testing.T) {
	store, _, cleanup := setupTestStore(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	member := Member{ID: "mem-1", DisplayName: "Alice"}
	// Cart threshold = ₹200.00 (20000 Paise)
	cart, err := store.CreateOrGetActiveCart(ctx, "geofence-unlock", member, Location{28.6, 77.2}, 500, 30*time.Minute, 20000)
	require.NoError(t, err)

	ch := store.SubscribeCartEvents(ctx, cart.ID)

	// Step 1: Add item ₹150.00 (15000 Paise) -> Total 15000 < 20000 -> Unlocked = false
	item1 := CartItem{ID: "item-1", PricePaise: 15000, AddedByMemberID: "mem-1"}
	_, newlyUnlocked1, err := store.AddItemAtomic(ctx, cart.ID, item1)
	require.NoError(t, err)
	assert.False(t, newlyUnlocked1)

	// Step 2: Add item ₹60.00 (6000 Paise) -> Total 21000 >= 20000 -> Unlocked = true (newlyUnlocked = true)
	item2 := CartItem{ID: "item-2", PricePaise: 6000, AddedByMemberID: "mem-1"}
	_, newlyUnlocked2, err := store.AddItemAtomic(ctx, cart.ID, item2)
	require.NoError(t, err)
	assert.True(t, newlyUnlocked2)

	// Step 3: Add item ₹50.00 (5000 Paise) -> Total 26000 >= 20000 -> Unlocked = true (newlyUnlocked = false!)
	item3 := CartItem{ID: "item-3", PricePaise: 5000, AddedByMemberID: "mem-1"}
	_, newlyUnlocked3, err := store.AddItemAtomic(ctx, cart.ID, item3)
	require.NoError(t, err)
	assert.False(t, newlyUnlocked3, "Subsequent items added post-unlock must NOT set newlyUnlocked=true")

	// Read events from channel and count UNLOCKED events
	unlockEventCount := 0
	readDone := time.After(300 * time.Millisecond)

Loop:
	for {
		select {
		case ev, ok := <-ch:
			if !ok {
				break Loop
			}
			if ev.Type == EventCartUnlocked {
				unlockEventCount++
			}
		case <-readDone:
			break Loop
		}
	}

	assert.Equal(t, 1, unlockEventCount, "CART_UNLOCKED event must fire EXACTLY ONCE when crossing threshold")
}

func TestGroupCart_BillSplitCorrectness(t *testing.T) {
	cart := &GroupCart{
		ID: "cart-split",
		Members: []Member{
			{ID: "mem-A", DisplayName: "Alice"},
			{ID: "mem-B", DisplayName: "Bob"},
			{ID: "mem-C", DisplayName: "Charlie"},
		},
		Items: []CartItem{
			{ID: "1", PricePaise: 5000, AddedByMemberID: "mem-A"}, // ₹50.00
			{ID: "2", PricePaise: 10000, AddedByMemberID: "mem-B"}, // ₹100.00
			{ID: "3", PricePaise: 3000, AddedByMemberID: "mem-B"}, // ₹30.00
			{ID: "4", PricePaise: 7500, AddedByMemberID: "mem-C"}, // ₹75.00
		},
		TotalPaise: 25500, // ₹255.00
	}

	split, err := ComputeBillSplit(cart)
	require.NoError(t, err)

	assert.Equal(t, int64(5000), split.MemberTotals["mem-A"])
	assert.Equal(t, int64(13000), split.MemberTotals["mem-B"])
	assert.Equal(t, int64(7500), split.MemberTotals["mem-C"])

	sumSplit := split.MemberTotals["mem-A"] + split.MemberTotals["mem-B"] + split.MemberTotals["mem-C"]
	assert.Equal(t, cart.TotalPaise, sumSplit, "Sum of individual member totals must equal cart grand total")
}

func TestGroupCart_TTLExpiryReaper(t *testing.T) {
	store, mr, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()
	member := Member{ID: "mem-1", DisplayName: "Alice"}
	// Short TTL = 50ms
	cart, err := store.CreateOrGetActiveCart(ctx, "geofence-ttl", member, Location{28.6, 77.2}, 500, 50*time.Millisecond, 20000)
	require.NoError(t, err)

	reaper := NewTTLReaper(store, store.client, 20*time.Millisecond)
	reaper.Start(ctx)

	// Sleep past TTL expiration
	time.Sleep(100 * time.Millisecond)

	// Fast-forward miniredis time if needed
	mr.FastForward(200 * time.Millisecond)

	reaper.reapExpiredCarts(ctx)
	reaper.Stop()

	finalizedCart, err := store.GetCart(ctx, cart.ID)
	require.NoError(t, err)

	assert.Equal(t, CartStatusFinalized, finalizedCart.Status, "Cart must be finalized after TTL expiration")
}

func TestGroupCart_WebSocketRealtimePropagation(t *testing.T) {
	store, _, cleanup := setupTestStore(t)
	defer cleanup()

	server := NewServer(store)
	httptestServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/ws/cart" {
			server.HandleWebSocket(w, r)
		} else if r.URL.Path == "/api/carts/item" {
			server.HandleAddItem(w, r)
		}
	}))
	defer httptestServer.Close()

	ctx := context.Background()
	cart, err := store.CreateOrGetActiveCart(ctx, "geofence-ws", Member{ID: "mem-1"}, Location{28.6, 77.2}, 500, 30*time.Minute, 20000)
	require.NoError(t, err)

	// Connect 3 virtual WS clients
	wsURL, _ := url.Parse(httptestServer.URL)
	wsURL.Scheme = "ws"
	wsURL.Path = "/ws/cart"

	conns := make([]*websocket.Conn, 3)
	for i := 0; i < 3; i++ {
		u := fmt.Sprintf("%s?cart_id=%s&member_id=mem-%d", wsURL.String(), cart.ID, i+1)
		conn, _, err := websocket.DefaultDialer.Dial(u, nil)
		require.NoError(t, err)
		defer conn.Close()
		conns[i] = conn
	}

	// Brief 30ms sleep for WS client registration and Pub/Sub channel connection to complete
	time.Sleep(30 * time.Millisecond)

	// Member 1 adds item ₹50.00
	item := CartItem{
		ID:              "item-ws-1",
		SKU:             "SKU-WS",
		Name:            "WS Test Item",
		PricePaise:      5000,
		AddedByMemberID: "mem-1",
	}

	startWrite := time.Now()
	_, _, err = store.AddItemAtomic(ctx, cart.ID, item)
	require.NoError(t, err)

	// Assert Member 1, Member 2, and Member 3 receive CART_UPDATED event within 200ms bound
	for i := 0; i < 3; i++ {
		conns[i].SetReadDeadline(time.Now().Add(1 * time.Second))
		_, message, err := conns[i].ReadMessage()
		latency := time.Since(startWrite)
		require.NoError(t, err, "WS Client %d must receive update", i+1)

		var event CartEvent
		err = json.Unmarshal(message, &event)
		require.NoError(t, err)
		assert.Equal(t, EventCartUpdated, event.Type)
		assert.Equal(t, cart.ID, event.CartID)

		assert.Less(t, latency, 200*time.Millisecond, "Propagation latency must be under 200ms bound (actual latency: %v)", latency)
	}
}
