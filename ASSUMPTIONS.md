# Simplification Assumptions (ASSUMPTIONS.md)

This document tracks all explicit simplifying assumptions made throughout the architecture and simulation of the Adaptive Load Orchestrator. It acts as the single running source of truth for system models, queueing dynamics, parameter choices, and design trade-offs.

---

## Phase 1 — Fulfillment Engine Core

1. **Queueing Model (M/M/c)**:
   - Order arrivals follow a memoryless **Poisson process** with rate $\lambda$ (orders/sec).
   - Picker service times follow an **exponential distribution** with mean rate $\mu$ (orders/sec per picker).
   - *Real-world divergence*: Real picking is subject to aisle physical congestion, item weight variations, and inventory search times which create heavy-tailed service distributions (e.g. Weibull or Log-Normal). Phase 1 uses an M/M/c queueing system as a standard baseline model.

2. **Distance & Spatial Calculation**:
   - Store locations use 2D GPS coordinates (lat/lng).
   - Proximity is calculated using the **Haversine formula** (great-circle distance).
   - *Real-world divergence*: Real-world road network topology, traffic signals, turn restrictions, and delivery dispatch routes are not modeled; 1.5km haversine distance is used as a strict proximity threshold.

3. **In-Process Event Bus**:
   - Phase 1 uses an in-process Go channel-based event bus for load threshold breaches and re-routing.
   - *Real-world divergence*: Redis Pub/Sub and Streams will be introduced in Phase 2 & 3 when Redis state persistence and multi-service orchestration are integrated.

4. **Order Composition & Naive Re-routing**:
   - Orders consist of item count and a perishable flag (`IsPerishable`).
   - In Phase 1 naive re-routing, when a store's load crosses 85%, eligible non-perishable order items are transferred directly to the nearest store under threshold within 1.5km.
   - *Real-world divergence*: Inventory availability is assumed unlimited in Phase 1 (distributed multi-node inventory reservations via Redis Lua scripts will be enforced in Phase 3).

5. **Re-Route Cooldown Duration (`reRouteCooldown = 30s` simulated time)**:
   - *Rationale*: Once a dark store triggers or receives a re-route event, it enters a 30-second simulated time cooldown window. At baseline picking rates ($\mu = 0.15$ orders/sec per picker across $c=5$ pickers $\rightarrow 0.75$ orders/sec combined), 30 seconds allows the pickers to process ~22.5 orders, giving the queue sufficient time to drain before re-evaluating re-route eligibility. This prevents per-tick ping-pong oscillation while ensuring stores do not remain locked out during genuinely sustained network surges.

6. **Hysteresis Recovery Threshold (`recoveryThreshold = 70%` load)**:
   - *Rationale & Trade-off*: Re-route triggering occurs at `upperThreshold = 85%` load, but a candidate target store can ONLY accept re-routed orders if its current load is `< 70%`.
   - *Trade-off*: Setting recovery threshold too close to 85% (e.g. 80%) causes target stores to immediately cross 85% upon receiving a burst of orders and re-route them back. Setting recovery threshold too low (e.g. 50%) keeps nearby candidate stores ineligible for receiving re-routes for too long during sustained demand spikes, artificially restricting network capacity. 70% load provides a guaranteed 15% headroom buffer.

7. **Customer Latency Proxy (`avg_time_in_system`)**:
   - `avg_service_time = CompletedAt - PickStartedAt` measures only active picker execution duration.
   - `avg_time_in_system = CompletedAt - QueuedAt` measures true customer-facing end-to-end latency (queue wait time + picker service time combined).
   - *Phase 3 Consumption*: `avg_time_in_system` will serve as the explicit input for the SLA-breach penalty calculation $f(\text{predicted\_delay\_minutes})$ in Phase 3 cost-gated decision logic.

---

## Phase 2 — Group Cart Engine Core

1. **WebSocket Framework Selection (`github.com/gorilla/websocket`)**:
   - *Rationale*: `gorilla/websocket` is chosen for its widespread adoption, robust connection upgrade handling, thread-safe write loop mechanics via dedicated per-connection write channels, and low-overhead binary/text frame parsing.

2. **Geofencing Simplification ("Fake Geofencing")**:
   - *Rationale*: Geofenced areas are modeled deterministically using hardcoded named geofences with a fixed centroid coordinate (lat/lng) and static radius in meters (e.g., `geofence-aravali` at `[28.6315, 77.2167]` with 500m radius). Real-world polygon building footprints, GPS noise, indoor elevation, and cell-tower/WiFi positioning are omitted.

3. **Redis as External Source of Truth**:
   - *Rationale*: All group cart state (members, items, totals, unlock status) is persisted in Redis rather than Go in-memory maps. This architectural choice ensures that web application instances remain stateless, allowing group cart state and WebSocket updates to scale horizontally across multiple application nodes.

4. **Money Representation (Integer Paise)**:
   - *Rationale*: All item prices, cart totals, unlock thresholds, and bill-split shares are stored as 64-bit signed integers (`int64`) in Paise (where 100 Paise = ₹1.00; e.g. 20000 Paise = ₹200.00). This eliminates floating-point rounding errors and precision leakage during bill splitting arithmetic.

5. **Atomic Operations via Redis Lua Scripting**:
   - *Rationale*: Item additions, removals, and running total re-computations execute atomically inside a single Redis Lua script (`EVAL`). This prevents Go-side check-then-write race conditions when multiple virtual members add or remove items simultaneously.

6. **Single-Fire Discount Unlock Event**:
   - *Rationale*: The discount unlock condition (`TotalPaise >= UnlockThresholdPaise`) is evaluated inside the atomic Lua script. The script checks whether `Unlocked` transitions from `0` to `1` during the current write and publishes an `UNLOCKED` event **exactly once** upon crossing. Subsequent item additions while above threshold do not re-trigger unlock events.

7. **Decoupled Architecture**:
   - *Rationale*: Phase 2 operates independently of Phase 1 (`internal/fulfillment`). No imports exist between Phase 1 and Phase 2. Integration will occur strictly in Phase 4.

---

## Phase 3 — Cost-Gated Decision Engine & Distributed Reservation Safety

1. **Explicit Cost Model Formulas & Constants**:
   - **`reRouteCost(order, distKm)`**:
     $$\text{reRouteCost} = \text{BaseFee} + (\text{PerKmRate} \times \text{distKm})$$
     - $\text{BaseFee} = ₹25.00$ ($2,500\text{ Paise}$) — fixed operational cost for dispatching a second delivery leg.
     - $\text{PerKmRate} = ₹10.00/\text{km}$ ($1,000\text{ Paise/km}$) — distance-scaled fuel & rider payment rate.
   - **`slaBreachPenalty(order, predictedDelayMin)`**:
     $$\text{slaBreachPenalty} = \begin{cases} 0 & \text{if } \text{predictedDelayMin} \le W_{\text{acceptable}} \\ (\text{predictedDelayMin} - W_{\text{acceptable}}) \times \text{PenaltyPerMin} & \text{if } \text{predictedDelayMin} > W_{\text{acceptable}} \end{cases}$$
     - $W_{\text{acceptable}} = 1.0\text{ min}$ ($60\text{ seconds}$) — acceptable queue backlog wait time buffer.
     - $\text{PenaltyPerMin} = ₹15.00/\text{min}$ ($1,500\text{ Paise/min}$) — customer churn, support ticket, and goodwill voucher cost per minute of delay beyond acceptable window.

2. **Cost-Gated Trigger Rule**:
   - A re-route occurs **ONLY IF**: $\text{reRouteCost} < \text{slaBreachPenalty}$.
   - *Trade-off*: If $\text{reRouteCost} \ge \text{slaBreachPenalty}$, the re-route is blocked to prevent margin-negative transfers. The order remains in the congested store's local queue and is logged as `RE_ROUTE_REJECTED_ON_COST`. This improves order margin at the cost of higher customer queue wait time during severe surges.

3. **Distributed Stock Reservation Safety**:
   - Before committing a re-route to a destination store, stock availability for the requested SKU(s) is checked and reserved atomically using a single Redis Lua script (`reserveStockLuaScript`).
   - If destination store stock $\ge \text{requestedQty}$, stock is decremented and reservation succeeds (`RE_ROUTE_EXECUTED`).
   - If stock is insufficient, reservation fails (`RE_ROUTE_FAILED_NO_STOCK`), preventing double-reservations under high concurrent re-route requests. The order falls back to waiting in its original local store queue.

4. **Structured Decision Outcomes**:
   - Every load monitor decision evaluation yields one of four explicit, mutually exclusive outcomes:
     1. `NO_ACTION`: Store load below trigger threshold ($\le 85\%$).
     2. `RE_ROUTE_REJECTED_ON_COST`: Store load $>85\%$, but second delivery leg cost exceeds SLA breach penalty.
     3. `RE_ROUTE_EXECUTED`: Store load $>85\%$, cost-gate passed, and Redis stock reservation succeeded.
     4. `RE_ROUTE_FAILED_NO_STOCK`: Store load $>85\%$, cost-gate passed, but destination store stock was unavailable.

5. **Carried-Over Open Item Rationale (Phase 2 WebSocket Client Ramp)**:
   - *Observation*: In Phase 2, `--clients=250` achieved 219 active concurrent WebSocket connections.
   - *Root Cause*: On Windows OS, opening 250 simultaneous TCP loopback socket connections in an unthrottled parallel burst hits the operating system's default TCP loopback connection backlog queue limit (`SOMAXCONN` / Winsock backlog), dropping ~31 socket handshake attempts. This is an OS loopback burst limitation, not a application logic bug.

---

## Phase 4 — Unified Decision Engine & Demand Shaping

1. **Order-to-GroupCart-Member Linkage Implementation**:
   - Orders generated in dark store geofences optionally carry `MemberID`, `GroupCartID`, and `GeofenceID` fields. This establishes a direct, traceable link between simulated customer orders and active Redis Group Carts created in Phase 2.

2. **Dynamic Queue Order Consolidation Mechanics**:
   - When `DecisionEngine` detects a store load breach ($>85\%$), it queries Redis for active Group Carts matching the store's geofences.
   - If matching carts exist, `DecisionEngine` calls `ConsolidateOrdersByCart(cartID)` on the store's `OrderQueue`.
   - Unpicked queued orders belonging to the same Group Cart are merged into a single consolidated picking pass (`IsConsolidated = true`). The merged order's service time equals a single picking pass rather than $N$ independent passes, reducing total queue wait depth and picker iteration overhead.

3. **Batching Nudge Grace Window (`nudgeGraceNs = 30s` simulated time)**:
   - *Rationale*: When a batching nudge is issued (`BATCHING_NUDGE_ISSUED`), the store enters a 30-second simulated grace window before fallback re-routing can be evaluated. This gives queue consolidation time to reduce store load, preventing premature fallback re-routing.

4. **Structured Five Decision Outcomes**:
   - The unified `DecisionEngine` evaluates and logs 5 distinct outcome types:
     1. `NO_ACTION`: Load $\le 85\%$.
     2. `BATCHING_NUDGE_ISSUED`: Load $> 85\%$, active Group Carts found, queue consolidation applied.
     3. `RE_ROUTE_REJECTED_ON_COST`: Load $> 85\%$, post-grace window, $\text{reRouteCost} \ge \text{slaBreachPenalty}$.
     4. `RE_ROUTE_EXECUTED`: Load $> 85\%$, post-grace window, $\text{reRouteCost} < \text{slaBreachPenalty}$, stock reserved in Redis.
     5. `RE_ROUTE_FAILED_NO_STOCK`: Load $> 85\%$, post-grace window, cost-gate passed, but Redis stock unavailable.
