# Low-Latency Concurrent Event Broker

A high-performance, microsecond-optimized systems infrastructure component designed to handle multi-producer, multi-consumer (MPMC) telemetry streaming without the overhead of traditional thread locking mechanisms.

## Core Architectural Impact

* Built high-throughput event broker
* Engineered in modern C++
* Designed custom fixed-size memory pool
* Eliminated runtime heap allocation latency
* Implemented lock-free ring buffer
* Utilized atomic memory operations (`std::atomic`)
* Managed concurrent producer-consumer threads
* Prevented thread contention and deadlocks
* Achieved nanosecond-level message routing
* Handled high-throughput mock operations
* Optimized data structure alignment
* Minimized CPU cache misses
* Enforced strict RAII memory management

## Low-Level Optimization Mechanics

1. **Lock-Free MPMC Ring Buffer:** Implements Dmitry Vyukov's array-based queue configuration. Uses atomic Compare-and-Swap (CAS) state tracking to allow concurrent thread operations without explicit mutex constraints.
2. **Cache-Line Alignment (`alignas(64)`):** Enforces 64-byte boundaries on data elements to eradicate false sharing across different CPU cores.
3. **Pre-allocated Memory Footprint:** Erases kernel-level heap allocation latencies during runtime execution by provisioning memory blocks purely at initialization.

## Quick Start & Verification

This project executes natively on systems supporting standard C++17 threading libraries.

```bash
# Clone the repository
git clone [https://github.com/YOUR_USERNAME/Low-Latency-Concurrent-Event-Broker.git](https://github.com/YOUR_USERNAME/Low-Latency-Concurrent-Event-Broker.git)
cd Low-Latency-Concurrent-Event-Broker

# Compile with full hardware optimizations (-O3 flag)
g++ -O3 -std=c++17 -pthread main.cpp -o event_broker

# Run the performance benchmark suite
./event_broker
