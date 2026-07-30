# Mathematical Queueing Architecture & Load Balancing Model

This document presents the formal mathematical foundations, queueing theory formulations, and algorithmic mechanics governing the **Adaptive Load Orchestrator**.

---

## 1. Kendall Notation & Dark Store Queueing System

Each dark store micro-fulfillment center is modeled as an **$M/M/c$ queueing node**:

$$\text{Queueing Model: } M/M/c$$

### Key Parameters:
- **Arrival Rate ($\lambda$)**: Order arrival process follows a memoryless **Poisson Distribution** with rate $\lambda$ (orders/second).
- **Service Rate ($\mu$)**: Individual picker execution duration follows an **Exponential Distribution** with mean rate $\mu$ (orders/second per picker).
- **Server Capacity ($c$)**: Parallel human picker workforce count per store.
- **Traffic Intensity / Utilization ($\rho$)**:
  $$\rho = \frac{\lambda}{c \cdot \mu}$$

---

## 2. Erlang-C Queueing Probability & Wait Times

When a dark store experiences a demand surge where $\rho \to 1.0$, incoming orders buffer in queue.

### Erlang-C Formula ($P_q$):
The probability that an arriving order finds all $c$ pickers busy and must wait in queue is:

$$P_q = P(W > 0) = \frac{\frac{(c\rho)^c}{c!} \cdot \frac{1}{1-\rho}}{\sum_{k=0}^{c-1} \frac{(c\rho)^k}{k!} + \frac{(c\rho)^c}{c!} \cdot \frac{1}{1-\rho}}$$

### Mean Queue Length ($L_q$) & Queue Wait Time ($W_q$):
$$L_q = P_q \cdot \frac{\rho}{1-\rho}$$

$$W_q = \frac{L_q}{\lambda} = P_q \cdot \frac{1}{c\mu - \lambda}$$

---

## 3. Little's Law Application

Applied to dark store queue management and customer SLA compliance:

$$L = \lambda W$$

Where:
- $L$ = Total order volume in the system (queued + active picking).
- $\lambda$ = Effective order throughput (orders/sec).
- $W$ = Customer residence time ($W = W_q + W_s$, where $W_s = \frac{1}{\mu}$).

---

## 4. Cost-Gated Decision Engine & Penalty Formulation

Re-routing decisions undergo strict cost-gating to prevent unprofitable transfer costs during local store overload:

### SLA Breach Penalty Function:
$$f(\Delta t) = \text{BaseSLAFee} + \alpha \cdot \left( \max(0, T_{\text{est}} - T_{\text{SLA}}) \right)^2$$

Where:
- $T_{\text{est}}$ = Predicted customer end-to-end residence time ($W_q + W_s$).
- $T_{\text{SLA}}$ = SLA delivery target threshold (e.g. 10 minutes = 600s).
- $\alpha$ = Exponential SLA penalty multiplier ($\text{Paise/sec}^2$).

### Re-Route Cost Gate Equation:
$$\text{ReRouteDecision} = \begin{cases} 
\text{EXECUTE} & \text{if } \text{ReRouteCost} < f(\Delta t) \text{ and } \text{TargetLoad} < 70\% \\ 
\text{REJECT} & \text{if } \text{ReRouteCost} \ge f(\Delta t) 
\end{cases}$$

---

## 5. Proactive Rider Trip Pooling Mechanics

Fulfillment batching extends to delivery rider dispatch:

### Distance-Based Haversine Proximity Triggering:
$$d = 2R \arcsin \left( \sqrt{\sin^2\left(\frac{\Delta \phi}{2}\right) + \cos(\phi_1)\cos(\phi_2)\sin^2\left(\frac{\Delta \lambda}{2}\right)} \right)$$

When rider Haversine distance $d \le 0.8\text{ km}$ to geofence centroid, a `TRIP_AVAILABLE` event is dispatched.

### Tiered Pooled Delivery Fee Split Formula:
$$\text{DeliveryFeePaise}(N) = \begin{cases} 
3,500\text{ Paise (₹35.00)} & N = 1 \\ 
1,750\text{ Paise (₹17.50, 50\% split)} & N = 2 \\ 
0\text{ Paise (₹0.00, 100\% waived)} & N \ge 3 
\end{cases}$$

$$\text{DiscountPaise}(N) = 3,500 - \text{DeliveryFeePaise}(N)$$

---

## 6. Concurrency & Atomic Redis Lua Execution

All state changes for Group Carts and Rider Trips execute via atomic Redis Lua scripts (`EVAL`), eliminating race conditions without distributed locks.

$$\text{Isolation Guarantee: Serialized Single-Threaded Lua Execution in Redis Event Loop}$$
