# lsm-embedded-engine
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT) [![CI](https://github.com/sohamshewani/lsm-embedded-engine/actions/workflows/ci.yml/badge.svg)](https://github.com/sohamshewani/lsm-embedded-engine/actions) [![Security: Hardened](https://img.shields.io/badge/Security-Hardened-blue.svg)](https://github.com/sohamshewani/lsm-embedded-engine) [![Stars](https://img.shields.io/github/stars/sohamshewani/lsm-embedded-engine?style=flat)](https://github.com/sohamshewani/lsm-embedded-engine)

## Executive Summary & Problem Space

`lsm-embedded-engine` is a high-performance, zero-allocation-on-hot-path Log-Structured Merge-tree (LSM) embedded storage engine engineered in C99. Modern high-throughput applications running on non-volatile memory and NVMe arrays frequently bottleneck on random disk I/O, lock contention, and garbage collection pauses inherent in traditional B-Tree architectures. 

`lsm-embedded-engine` resolves these constraints by transforming random write workloads into sequential disk appends through an append-only Write-Ahead Log (WAL), an in-memory concurrent SkipList memtable, and immutable leveled Sorted String Tables (SSTables) on disk. Designed for deterministic latency profiles and hardened execution safety, the engine provides predictable $O(1)$ write amplification bounds and robust crash-recovery semantics, making it ideal for edge databases, distributed consensus storage layers, and resource-constrained environments requiring strict memory determinism.

---

## Structural ASCII Architecture / Dataflow Diagram

The engine isolates write paths, memory buffering, durability barriers, and background compaction through a strict layered architecture:

```
 +-----------------------------------------------------------------------+
 |                        Client Application                             |
 +-----------------------------------+-----------------------------------+
                                     |
                       [ Write / Read / Scan API ]
                                     v
 +-----------------------------------------------------------------------+
 |                     LSM Engine Core Orchestrator                      |
 +-------------------+-------------------------------+-------------------+
                     |                               |
        (Append Log) |                               | (Insert / Lookup)
                     v                               v
         +-----------------------+       +-----------------------+
         | Write-Ahead Log (WAL) |       |   Memtable SkipList   |
         | (src/lsm_wal.c)       |       | (src/lsm_skiplist.c)  |
         +-----------------------+       +-----------------------+
                                                     |
                                        (Flush / Freeze / Swap)
                                                     v
                                         +-----------------------+
                                         |    Immutable L0/Ln    |
                                         |    SSTables (Disk)    |
                                         |  (src/lsm_sstable.c)  |
                                         +-----------------------+
                                                     ^
                                                     |
                                         +-----------------------+
                                         |   Background Worker   |
                                         | Compaction Engine     |
                                         | (src/lsm_compaction.c)|
                                         +-----------------------+
```

---

## Concurrency Model & Memory Invariants

`lsm-embedded-engine` utilizes a fine-grained concurrency model optimized for lock-free reads and synchronized mutations:

* **Thread Safety**: The active memtable (`src/lsm_skiplist.c`) and WAL handle (`src/lsm_wal.c`) are protected via re-entrant read-write primitives and atomic synchronization barriers. Readers acquire shared locks to scan active memtable nodes without blocking concurrent writers.
* **Memory Ownership & Allocation (`src/lsm_alloc.c`)**: All internal node allocations utilize a bounded arena and pool allocator design. Hot-path operations bypass standard `malloc`/`free`syscalls, eliminating heap fragmentation and guaranteeing deterministic latency.
* **Compaction Isolation (`src/lsm_compaction.c`)**: Background compaction threads operate on immutable snapshot views of L0/Ln SSTables. Writers swap active and immutable memtables atomically via pointer flipping, preventing cascading lock contention during heavy I/O merge operations.

---

## Performance Characteristics & Algorithmic Complexity Table

| Operation | Average Case | Worst Case | Latency Estimate (NVMe) | Algorithmic Bottleneck |
| :--- | :--- | :--- | :--- | :--- |
| **Point Lookup (`Get`)** | $O(\log N)$ | $O(L \cdot \log N)$ | $15\mu\text{s} - 45\mu\text{s}$ | Bloom filter checks and SSTable index traversal |
| **Sequential Write (`Put`)** | $O(1)$ | $O(\log N)$ | $< 5\mu\text{s}$ | WAL fsync barrier and Memtable SkipList insertion |
| **Range Scan (`Scan`)** | $O(K + \log N)$ | $O(K + L \cdot \log N)$ | $20\mu\text{s} + 1\mu\text{s}/\text{item}$ | Multi-way iterator heap merging across levels |
| **Compaction (`Merge`)** | $O(N)$ | $O(N \log N)$ | Background Async | Disk bandwidth saturation and block decompression |

*Where $N$ is total key-count, $L$ is compaction level depth, and $K$ is keys returned in range.*

---

## Security Architecture & Threat Model

`lsm-embedded-engine` is built from the ground up to operate securely within untrusted or hostile execution environments:

* **Input Boundaries & Sanitization**: All disk-read vectors (SSTable headers, index blocks, WAL frames) are subjected to strict structural validation and magic-number verification before memory mapping or parsing.
* **Memory Corruption Protections**: Employs rigorous bounds-checking, explicit null-pointer validations, and CRC32 checksum verification on all WAL records and SSTable data blocks to detect bit-rot or malicious file tampering.
* **Buffer Overflow Mitigations**: Fixed-size stack buffers are strictly prohibited. Dynamic allocations enforce calculated length bounds with explicit integer-overflow guard clauses across all serialization and deserialization routines.
* **Resource Exhaustion Defense**: Bounded arenas and configurable memtable ceilings prevent memory exhaustion attacks (`CWE-400`), while file descriptor limits protect against exhaustion through unclosed SSTable handles.

---

## Build, Verification & Benchmarking Recipes

Ensure your environment has a C99-compliant compiler (`gcc` or `clang`), `make`, and standard POSIX development utilities installed.

### 1. Compile the Engine and Test Suite
```bash
make clean
make all
```

### 2. Run Invariant and Unit Tests
Execute the test suite to verify memory safety, thread concurrency limits, and LSM structural invariants (`tests/test_invariants.c`):
```bash
make test
```

### 3. Execute Performance Benchmarks
Run the high-throughput performance harness (`benchmarks/bench_engine.c`) to measure raw throughput, latency distributions, and compaction overhead:
```bash
make bench
```

### 4. Comprehensive Clean Build & Verification Pipeline
```bash
make clean && make all && make test && make bench