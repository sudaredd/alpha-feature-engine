# Sub-Microsecond Alpha Feature Engine

A bare-metal, high-frequency quantitative market data feature calculation engine built targeting Java 21/25 with zero-allocation constraints, lock-free primitives, Apache Arrow off-heap columnar memory layouts, and SIMD vector math.

---

## ⚡ Performance Targets & Objectives

* **Throughput Target:** > 1,000,000 events / second.
* **Latency Profile:** Sub-microsecond execution path ($p99.9 < 5\mu s$).
* **Memory Footprint:** Zero garbage collection ($0$ allocations in ingestion and calculation hot paths).

---

## 🛡 Strict Low-Latency Technical Rules

1. **Zero Object Allocation:** The `new` keyword is strictly prohibited in execution loops, ingestion handlers, or indicator metrics calculations. All structures are pre-allocated at startup or backed by off-heap `MemorySegment` buffers.
2. **No Enterprise Frameworks:** No Spring, REST controllers, or dynamic DI proxies. Bare-metal execution only.
3. **No Standard Java Collections in Hot Path:** Standard collections (`java.util.List`, `java.util.Map`, wrapper types like `Long`, `Double`) are barred from hot paths. Hot data structures leverage primitive arrays or off-heap Arrow vector structures.
4. **Lock-Free Concurrency:** Zero lock primitives (`synchronized`, `ReentrantLock`). Thread communication utilizes `VarHandle`, atomic CAS loops, and memory ordering fences (`Acquire`/`Release`).
5. **False Sharing Elimination:** Highly-contended sequences and counters feature 64-byte boundary padding to isolate fields into distinct L1 CPU cache lines.
6. **Fixed-Point Arithmetic:** Financial price and volume values use fixed-point scaled primitive `long` types (e.g. `price * 10^4`) to avoid IEEE-754 floating-point inaccuracies and speed up calculation cycles.

---

## 📁 Repository Structure

```
alpha-feature-engine/
├── pom.xml
├── README.md
└── src/
    ├── main/
    │   └── java/
    │       └── com/
    │           └── quant/
    │               └── engine/
    │                   └── ringbuffer/
    │                       └── PaddedAtomicSequence.java
    ├── test/
    │   └── java/
    │       └── com/
    │           └── quant/
    │               └── engine/
    │                   └── ringbuffer/
    │                       └── PaddedAtomicSequenceTest.java
    └── benchmarks/
        └── java/
            └── com/
                └── quant/
                    └── engine/
                        └── ringbuffer/
                            └── RingBufferBenchmark.java
```

---

## 🛠 Build & Execution Instructions

### Prerequisites
* **Java 25** (or Java 21+ with FFM & Preview features enabled)
* **Maven 3.9+**

### Compilation & Unit Tests

To compile the codebase with Java 25 preview options:

```bash
mvn clean compile
```

To run JUnit 5 unit tests:

```bash
mvn test
```

### Building & Executing JMH Microbenchmarks

To package the JMH executable benchmark uber-JAR:

```bash
mvn clean package
```

To execute the JMH tail-latency benchmarks:

```bash
java --enable-preview -jar target/benchmarks.jar
```

Optional flags for CPU pinning and profilers:
```bash
java --enable-preview -jar target/benchmarks.jar -prof gc -prof stack
```
