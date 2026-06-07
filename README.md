#include <iostream>
#include <atomic>
#include <thread>
#include <vector>
#include <chrono>
#include <cstring>
#include <cassert>

// ============================================================================
// LAYER 1: DATA STRUCTURE & CACHE ALIGNMENT
// ============================================================================

// alignas(64) prevents false sharing by forcing each Event packet to fit 
// perfectly into a standard CPU cache line size.
struct alignas(64) Event {
    uint64_t id;
    uint64_t timestamp_ns;
    char payload[32];
};

// ============================================================================
// LAYER 2: PRE-ALLOCATED LOCK-FREE ENGINE
// ============================================================================

template<typename T, size_t Capacity>
class LockFreeConcurrentBroker {
    static_assert((Capacity & (Capacity - 1)) == 0, "Capacity must be a power of 2 for bitwise masking.");

private:
    struct Cell {
        std::atomic<size_t> sequence;
        T data;
    };

    static constexpr size_t BufferMask = Capacity - 1;
    
    // Explicit padding prevents the enqueue and dequeue counters from sharing cache lines
    alignas(64) Cell buffer_[Capacity];
    alignas(64) std::atomic<size_t> enqueue_pos_{0};
    alignas(64) std::atomic<size_t> dequeue_pos_{0};

public:
    LockFreeConcurrentBroker() {
        for (size_t i = 0; i != Capacity; ++i) {
            buffer_[i].sequence.store(i, std::memory_order_relaxed);
        }
    }

    // High-performance lock-free enqueue mechanism using atomic compare-and-swap
    bool publish(T const& data) {
        Cell* cell;
        size_t pos = enqueue_pos_.load(std::memory_order_relaxed);
        while (true) {
            cell = &buffer_[pos & BufferMask];
            size_t seq = cell->sequence.load(std::memory_order_acquire);
            intptr_t dif = static_cast<intptr_t>(seq) - static_cast<intptr_t>(pos);
            
            if (dif == 0) {
                if (enqueue_pos_.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed)) {
                    break;
                }
            } else if (dif < 0) {
                return false; // Short-circuit: Buffer is fully saturated (backpressure management)
            } else {
                pos = enqueue_pos_.load(std::memory_order_relaxed);
            }
        }
        
        cell->data = data;
        cell->sequence.store(pos + 1, std::memory_order_release);
        return true;
    }

    // High-performance lock-free dequeue mechanism
    bool consume(T& data) {
        Cell* cell;
        size_t pos = dequeue_pos_.load(std::memory_order_relaxed);
        while (true) {
            cell = &buffer_[pos & BufferMask];
            size_t seq = cell->sequence.load(std::memory_order_acquire);
            intptr_t dif = static_cast<intptr_t>(seq) - static_cast<intptr_t>(pos + 1);
            
            if (dif == 0) {
                if (dequeue_pos_.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed)) {
                    break;
                }
            } else if (dif < 0) {
                return false; // Short-circuit: Broker buffer is completely empty
            } else {
                pos = dequeue_pos_.load(std::memory_order_relaxed);
            }
        }
        
        data = cell->data;
        cell->sequence.store(pos + BufferMask + 1, std::memory_order_release);
        return true;
    }
};

// ============================================================================
// LAYER 3: REAL-TIME BENCHMARK SUITE
// ============================================================================

constexpr size_t OPERATIONS_PER_PRODUCER = 250000;
constexpr size_t NUM_PRODUCERS = 4;
constexpr size_t TOTAL_EVENTS = OPERATIONS_PER_PRODUCER * NUM_PRODUCERS;
constexpr size_t BROKER_CAPACITY = 1024; // Must be power of 2

void producer_thread(size_t id, LockFreeConcurrentBroker<Event, BROKER_CAPACITY>& broker) {
    for (size_t i = 0; i < OPERATIONS_PER_PRODUCER; ++i) {
        Event ev;
        ev.id = (id * OPERATIONS_PER_PRODUCER) + i;
        std::string payload_str = "TELEMETRY_PACKET_" + std::to_string(i % 100);
        std::strncpy(ev.payload, payload_str.c_str(), sizeof(ev.payload));
        
        // Sample timestamp immediately before execution
        ev.timestamp_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch()
        ).count();

        // Spin-wait if the ring buffer experiences temporary backpressure saturation
        while (!broker.publish(ev)) {
            std::this_thread::yield();
        }
    }
}

void consumer_thread(LockFreeConcurrentBroker<Event, BROKER_CAPACITY>& broker, 
                     std::atomic<size_t>& consumed_count, 
                     std::atomic<uint64_t>& total_latency_ns) {
    size_t local_consumed = 0;
    uint64_t local_latency = 0;

    while (consumed_count.load(std::memory_order_relaxed) < TOTAL_EVENTS) {
        Event ev;
        if (broker.consume(ev)) {
            uint64_t current_time = std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::steady_clock::now().time_since_epoch()
            ).count();
            
            if (current_time >= ev.timestamp_ns) {
                local_latency += (current_time - ev.timestamp_ns);
            }
            local_consumed++;
            
            // Periodically flush tracking data to shared global memory metrics
            if (local_consumed >= 5000) {
                consumed_count.fetch_add(local_consumed, std::memory_order_relaxed);
                total_latency_ns.fetch_add(local_latency, std::memory_order_relaxed);
                local_consumed = 0;
                local_latency = 0;
            }
        } else {
            std::this_thread::yield(); // Yield CPU when broker queue is momentarily drained
        }
    }
    // Final flush of remaining tracking allocations
    if (local_consumed > 0) {
        consumed_count.fetch_add(local_consumed, std::memory_order_relaxed);
        total_latency_ns.fetch_add(local_latency, std::memory_order_relaxed);
    }
}

int main() {
    std::cout << "========================================================\n";
    std::cout << "INITIALIZING LOW-LATENCY CONCURRENT EVENT BROKER\n";
    std::cout << "Target Event Operations: " << TOTAL_EVENTS << "\n";
    std::cout << "========================================================\n\n";

    LockFreeConcurrentBroker<Event, BROKER_CAPACITY> broker;
    std::atomic<size_t> consumed_count{0};
    std::atomic<uint64_t> total_latency_ns{0};

    auto start_time = std::chrono::high_resolution_clock::now();

    // Spawn 1 Consumer and 4 concurrent Producer execution tracks
    std::thread consumer(consumer_thread, std::ref(broker), std::ref(consumed_count), std::ref(total_latency_ns));
    
    std::vector<std::thread> producers;
    for (size_t i = 0; i < NUM_PRODUCERS; ++i) {
        producers.emplace_back(producer_thread, i, std::ref(broker));
    }

    // Synchronize thread completion paths
    for (auto& p : producers) {
        p.join();
    }
    consumer.join();

    auto end_time = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> total_duration = end_time - start_time;

    double throughput_ops_sec = TOTAL_EVENTS / total_duration.count();
    double average_latency_ns = static_cast<double>(total_latency_ns.load()) / TOTAL_EVENTS;

    std::cout << "--- PERFORMANCE BENCHMARK REPORT ---\n";
    std::cout << "Execution Duration : " << total_duration.count() << " seconds\n";
    std::cout << "System Throughput  : " << throughput_ops_sec << " ops/sec\n";
    std::cout << "Mean In-Flight Latetcy : " << average_latency_ns << " nanoseconds\n";
    std::cout << "------------------------------------\n";

    return 0;
}
