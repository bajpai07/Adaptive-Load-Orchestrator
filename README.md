# Adaptive Load Orchestrator

[![Go Version](https://img.shields.io/badge/Go-1.25.6-blue.svg)](https://golang.org)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

A portfolio-grade simulation project exploring **cost-aware, demand-shaping-first load orchestration** for quick-commerce (dark-store grocery delivery) operations.

---

## Framing Statement & Project Scope

> [!NOTE]
> **Framing Statement**: This project is an architectural simulation exploring how demand-side order consolidation (`internal/groupcart`) interacts with cost-gated fulfillment re-routing (`internal/fulfillment`) under realistic delivery logistics fees. It is an engineering demonstration of systems design, queueing math, distributed Redis state management, and seed-controlled empirical benchmarking — **not a claim to have solved a real quick-commerce company's production problem**.

---

## Core Problem & Architecture

In quick-commerce, when a dark store experiences a demand surge, the naive response is to re-route excess orders to nearby dark stores. However, **cross-store re-routing incurs a second delivery leg fee** ($\text{reRouteCost} = \text{BaseFee} + \text{PerKmRate} \times \text{distKm}$). When delivery cost exceeds the SLA breach penalty ($\text{slaPenalty} = (\text{predictedDelay} - 1.0) \times \text{PenaltyPerMin}$), re-routing loses money on every transfer.

This project unifies **demand-side Group Carts** with **cost-gated fulfillment** to prioritize **demand-side order consolidation** first before falling back to cross-store re-routing.

```
                         ┌─────────────────────────────┐
                         │   Load Monitor (85% Load)   │
                         └──────────────┬──────────────┘
                                        │
                                        ▼
                         ┌─────────────────────────────┐
                         │ Step 1: Query Redis Carts   │
                         │ Active Carts in Geofence?   │
                         └──────┬───────────────┬──────┘
                            YES │               │ NO / Post-Grace (30s)
             ┌──────────────────┘               └─────────────────┐
             ▼                                                    ▼
┌───────────────────────────────┐               ┌──────────────────────────────────┐
│    BATCHING_NUDGE_ISSUED      │               │ Step 2: Cost Gate Evaluation     │
│ (Queue Order Consolidation)   │               │   reRouteCost < slaPenalty?      │
└───────────────────────────────┘               └──────┬───────────────────┬───────┘
                                                    NO │                   │ YES
                                                       ▼                   ▼
                                       ┌───────────────────────┐ ┌───────────────────┐
                                       │RE_ROUTE_REJECTED_COST │ │ Redis Lua Stock   │
                                       └───────────────────────┘ └─────────┬─┬───────┘
                                                                      FAIL │ │ SUCCESS
                                                                           ▼ ▼
                                                                 ┌───────────────────┐
                                                                 │ RE_ROUTE_EXECUTED │
                                                                 └───────────────────┘
```

---

## Four Architectural Phases

- **Phase 1 — Fulfillment Engine Core**: M/M/c queueing simulation math, store load monitoring with hysteresis state machine (>85% breach, <70% recovery), 30s cooldown window, `avg_time_in_system` latency metric, and cumulative network accounting invariant verification.
- **Phase 2 — Group Cart Engine Core**: Decoupled `internal/groupcart` package, Redis-backed atomic Lua scripts (`addItemLuaScript`), single-fire discount unlock mechanic (`CART_UNLOCKED`), exact integer Paise currency representation (`int64`), background `TTLReaper`, and WebSocket Pub/Sub event streaming.
- **Phase 3 — Cost-Gated Decision Engine & Distributed Reservation Safety**: Economic cost gate ($\text{reRouteCost} < \text{slaBreachPenalty}$), atomic Redis Lua stock reservation (`reserveStockLuaScript`), 20-run loop concurrency tests, and structured 4-outcome event logs.
- **Phase 4 — Unified Decision Engine & Operations Dashboard**: Unified `DecisionEngine`, order-to-cart member linkage, dynamic queue consolidation (`ConsolidateOrdersByCart`), 30s nudge grace window, real-time WebSocket Operations Console (`dashboard/ops_console.html`), and 3-way seed-controlled benchmark comparison.

---

## Empirical Three-Way Seed-Controlled Benchmark Results

All metrics below come from compiled code execution runs under **identical seed (`--seed=42`)**, generating **6,405 network orders** across identical demand arrival patterns.

| Metric | (a) Naive Re-Routing (Phase 1) | (b) Cost-Gated Only (Phase 3) | (c) Full Orchestration (Phase 4) | Engineering Impact & Interpretation |
| :--- | :--- | :--- | :--- | :--- |
| **Total Orders Created** | **6,405** | **6,405** | **6,405** | **100% Exact Match (6,405 = 6,405 = 6,405)** |
| **Total Orders Completed** | 3,434 | 3,521 | 3,520 | Stable processing throughput |
| **Re-Routes Executed** | **34** | **0** | **22** | Cost gate blocked non-viable transfers |
| **`RE_ROUTE_REJECTED_ON_COST`** | 0 | **2,700** | **2,512** | Blocked margin-negative transfers |
| **`BATCHING_NUDGE_ISSUED`** | 0 | 0 | **830** | **Demand-shaping consolidation triggered** |
| **Total Orders Merged** | 0 | 0 | **2,263** | **2,263 orders consolidated into single passes** |
| **Store-1 Avg System Time** | 185.36s | 182.54s | **96.81s** | **47.0% Latency Reduction!** |
| **Store-1 Queue Backlog Count** | 996 orders | 992 orders | **684 orders** | **31.0% Queue Depth Reduction!** |
| **Direct Logistics Margin Saved** | -₹1,183.88 (Loss) | ₹0.00 (Neutral) | **+₹417.84** | **Avoids naive re-route delivery fee waste** |
| **Accounting Invariant Status** | **PASSED** | **PASSED** | **PASSED** | **100% Verified Order Accounting** |

---

## Reproduction & Benchmark Commands

### 1. Run Unit Tests & Concurrency Verification (Race Detector)
```powershell
$env:PATH="C:\msys64\mingw64\bin;" + $env:PATH; $env:CGO_ENABLED="1"; go test -count=1 -race -v ./internal/fulfillment/... ./internal/groupcart/...
```

### 2. Run Three-Way Benchmark Comparison
```powershell
# (a) Naive Re-Routing (Phase 1)
go run ./cmd/sim/main.go --mode=naive --duration=10m --surge-store=store-1 --surge-factor=3.0 --surge-duration=5m --surge-start=2m --seed=42

# (b) Cost-Gated Only (Phase 3)
go run ./cmd/sim/main.go --mode=cost-gated --duration=10m --surge-store=store-1 --surge-factor=3.0 --surge-duration=5m --surge-start=2m --seed=42

# (c) Full Orchestration (Phase 4)
go run ./cmd/sim/main.go --mode=full-orchestration --duration=10m --surge-store=store-1 --surge-factor=3.0 --surge-duration=5m --surge-start=2m --seed=42
```

### 3. Launch Operations Console Dashboard
1. Start Full Orchestration simulation:
   ```powershell
   go run ./cmd/sim/main.go --mode=full-orchestration --port=8081
   ```
2. Open browser to: `http://localhost:8081/ops_console.html`

---

## Production Deployment Guide (Railway / Render / Docker)

The application binary is fully cloud-ready and supports automatic environment variable configuration for platforms like **Railway**, **Render**, or custom Docker containers.

### Environment Variables

| Variable | Required? | Default | Description |
| :--- | :--- | :--- | :--- |
| `PORT` | Optional | `8081` | HTTP server port (automatically assigned by Railway/Render). |
| `REDIS_URL` | Optional | — | Connection string for external Redis (e.g. `redis://default:pass@redis-host:6379/0`). |
| `REDIS_ADDR` | Optional | — | Alternative Redis host:port address string (e.g. `redis-service:6379`). |
| `MODE` | Optional | `full-orchestration` | Engine mode (`full-orchestration` \| `cost-gated` \| `naive`). |
| `TIME_SCALE` | Optional | `100.0` | Simulation speedup scale factor. |
| `AUTO_RESTART` | Optional | `true` (in cloud) | Continuous simulation loop for live demos (`true` \| `false`). |

> [!TIP]
> **Zero-Dependency Local Mode**: If neither `REDIS_URL` nor `REDIS_ADDR` is provided, the application automatically starts an embedded, in-memory Miniredis instance.

### Deployment on Railway

1. **New Service**: Select **Deploy from GitHub repo**.
2. **Add Redis Database**: Add a Redis database service in Railway.
3. **Environment Variables**: Add `REDIS_URL` referencing your Railway Redis service string `${{Redis.REDIS_URL}}`.
4. **Start Command**:
   ```bash
   go run ./cmd/sim/main.go
   ```

---

## Honest Portfolio Limitations

1. **Aisle Congestion & Physical Layout**: Picker service times follow an M/M/c memoryless exponential distribution; physical aisle congestion and item weight variations are omitted.
2. **Synthetic Cost Parameters**: Cost constants ($\text{BaseFee} = ₹25$, $\text{PerKmRate} = ₹10/\text{km}$, $\text{PenaltyPerMin} = ₹15/\text{min}$) are synthetic parameters derived from industry delivery benchmarks.
3. **No Real Company Data**: The simulation operates entirely on synthetic Poisson demand streams and lat/lng dark store grids.

