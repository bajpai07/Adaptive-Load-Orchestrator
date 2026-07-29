package groupcart

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true // Allow all origins for simulation & testing
	},
}

type ClientConnection struct {
	MemberID string
	CartID   string
	Conn     *websocket.Conn
	Send     chan []byte
}

type WSHub struct {
	store         *RedisCartStore
	mu            sync.RWMutex
	connections   map[string]map[string]*ClientConnection // cartID -> memberID -> ClientConnection
	subscriptions map[string]context.CancelFunc          // cartID -> cancelFunc
}

func NewWSHub(store *RedisCartStore) *WSHub {
	return &WSHub{
		store:         store,
		connections:   make(map[string]map[string]*ClientConnection),
		subscriptions: make(map[string]context.CancelFunc),
	}
}

func (h *WSHub) Register(ctx context.Context, cartID, memberID string, conn *websocket.Conn) *ClientConnection {
	client := &ClientConnection{
		MemberID: memberID,
		CartID:   cartID,
		Conn:     conn,
		Send:     make(chan []byte, 256),
	}

	h.mu.Lock()
	if _, exists := h.connections[cartID]; !exists {
		h.connections[cartID] = make(map[string]*ClientConnection)

		subCtx, cancel := context.WithCancel(context.Background())
		h.subscriptions[cartID] = cancel

		// Synchronously establish Redis Pub/Sub channel confirmation before completing registration
		ch := h.store.SubscribeCartEvents(subCtx, cartID)
		go h.listenPubSubChannel(subCtx, cartID, ch)
	}
	h.connections[cartID][memberID] = client
	h.mu.Unlock()

	return client
}

func (h *WSHub) Unregister(cartID, memberID string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if members, exists := h.connections[cartID]; exists {
		if client, memberExists := members[memberID]; memberExists {
			close(client.Send)
			delete(members, memberID)
		}
		if len(members) == 0 {
			delete(h.connections, cartID)
			if cancel, subExists := h.subscriptions[cartID]; subExists {
				cancel()
				delete(h.subscriptions, cartID)
			}
		}
	}
}

func (h *WSHub) listenPubSubChannel(ctx context.Context, cartID string, ch <-chan *CartEvent) {
	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-ch:
			if !ok {
				return
			}
			eventRaw, err := json.Marshal(event)
			if err != nil {
				continue
			}

			h.mu.RLock()
			if members, exists := h.connections[cartID]; exists {
				for _, client := range members {
					select {
					case client.Send <- eventRaw:
					default:
						// Buffer full, drop frame to prevent slow consumer blocking
					}
				}
			}
			h.mu.RUnlock()
		}
	}
}

func (c *ClientConnection) WritePump(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer func() {
		ticker.Stop()
		c.Conn.Close()
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case message, ok := <-c.Send:
			c.Conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
			if !ok {
				c.Conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			w, err := c.Conn.NextWriter(websocket.TextMessage)
			if err != nil {
				return
			}
			w.Write(message)
			if err := w.Close(); err != nil {
				return
			}
		case <-ticker.C:
			c.Conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
			if err := c.Conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

// Server handles HTTP APIs and WebSocket upgrades.
type Server struct {
	store *RedisCartStore
	hub   *WSHub
}

func NewServer(store *RedisCartStore) *Server {
	return &Server{
		store: store,
		hub:   NewWSHub(store),
	}
}

func (s *Server) HandleJoinCart(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		GeofenceID           string  `json:"geofence_id"`
		MemberID             string  `json:"member_id"`
		DisplayName          string  `json:"display_name"`
		Lat                  float64 `json:"lat"`
		Lng                  float64 `json:"lng"`
		RadiusMeters         float64 `json:"radius_meters"`
		UnlockThresholdPaise int64   `json:"unlock_threshold_paise"`
		TTLSeconds           int64   `json:"ttl_seconds"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	member := Member{
		ID:          req.MemberID,
		DisplayName: req.DisplayName,
		JoinedAt:    time.Now(),
	}

	ttl := time.Duration(req.TTLSeconds) * time.Second
	if ttl <= 0 {
		ttl = 30 * time.Minute
	}

	cart, err := s.store.CreateOrGetActiveCart(r.Context(), req.GeofenceID, member, Location{Lat: req.Lat, Lng: req.Lng}, req.RadiusMeters, ttl, req.UnlockThresholdPaise)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(cart)
}

func (s *Server) HandleAddItem(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		CartID          string `json:"cart_id"`
		SKU             string `json:"sku"`
		Name            string `json:"name"`
		PricePaise      int64  `json:"price_paise"`
		AddedByMemberID string `json:"added_by_member_id"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	item := CartItem{
		ID:              fmt.Sprintf("item-%d", time.Now().UnixNano()),
		SKU:             req.SKU,
		Name:            req.Name,
		PricePaise:      req.PricePaise,
		AddedByMemberID: req.AddedByMemberID,
		AddedAt:         time.Now(),
	}

	updatedCart, newlyUnlocked, err := s.store.AddItemAtomic(r.Context(), req.CartID, item)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"cart":           updatedCart,
		"newly_unlocked": newlyUnlocked,
	})
}

func (s *Server) HandleWebSocket(w http.ResponseWriter, r *http.Request) {
	cartID := r.URL.Query().Get("cart_id")
	memberID := r.URL.Query().Get("member_id")

	if cartID == "" || memberID == "" {
		http.Error(w, "Missing cart_id or member_id", http.StatusBadRequest)
		return
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("WS Upgrade error: %v", err)
		return
	}

	clientCtx, clientCancel := context.WithCancel(context.Background())
	client := s.hub.Register(clientCtx, cartID, memberID, conn)
	go client.WritePump(clientCtx)

	// Read pump to handle disconnection
	defer func() {
		clientCancel()
		s.hub.Unregister(cartID, memberID)
		conn.Close()
	}()

	for {
		_, _, err := conn.ReadMessage()
		if err != nil {
			break
		}
	}
}
