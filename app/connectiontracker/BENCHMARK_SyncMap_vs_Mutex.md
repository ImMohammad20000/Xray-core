# Connection Tracker: Sharded RWMutex vs `sync.Map` vs single `sync.Mutex`

This document compares three designs for the per-user connection registry
(`app/connectiontracker`) and records the move to the sharded design plus the
readability cleanups that came with it.

> Scratch document — safe to delete. Generated on branch `UserConnTracker`.

## TL;DR

The **sharded RWMutex** design is the production choice. It wins the realistic
mixed workload and both registration paths, has the fewest allocations, leads
`ListConnections` outright, and beats `sync.Map` on `CancelAll` — the operation
the feature exists for. It only trails the single mutex on single-hot-key churn
and bulk `CancelAll`, and trails `sync.Map` on single-user count reads (where an
atomic counter wins). Connection IDs are `uint64`.

## The three designs

| | Indexing | Locking |
|---|---|---|
| **Mutex** (original) | nested `map[email]map[id]` + flat `map[id]` | one `sync.Mutex` for everything |
| **SyncMap** | `sync.Map` byID + `sync.Map` byEmail of buckets | lock-free, atomic count mirror |
| **Sharded** (now) | 64 id-shards + 64 email-shards, each a plain map | per-shard `sync.RWMutex` |

The production `Tracker`:

```go
const shardCount = 64 // power of two, masked

type idShard struct {
    mu   sync.RWMutex
    byID map[uint64]*ConnEntry
}
type emailShard struct {
    mu      sync.RWMutex
    byEmail map[string]map[uint64]*ConnEntry
}
type Tracker struct {
    manager *Manager
    ids     [shardCount]idShard    // sharded by id
    emails  [shardCount]emailShard // sharded by maphash(email)
}
```

Why this beats the alternatives:

- vs **single mutex**: independent connections fall on different shards, so
  Register/Unregister rarely contend; reads take `RLock`.
- vs **`sync.Map`**: plain `map[uint64]` keys are **not boxed into `any`**
  (`sync.Map` boxes every key — that was the 7 allocs/op), bulk `CancelAll`
  walks one plain map instead of ranging a `sync.Map`, and `ListConnections`
  can presize because plain maps have `len`.

## Readability changes

- **`addEntry` / `takeID` / `removeFromEmail` helpers.** The "both indexes move
  together" invariant lived inline in four methods; it now lives in one place.
  `takeID` is the single exactly-once guard (its map delete decides who cancels).
- **`idShard(id)` / `emailShard(email)` helpers** hide the shard math.
- **No more `any` type assertions** sprinkled through every method.
- **No atomic `count` mirror.** `GetConnCount` is `len(map)` under an `RLock`,
  deleting the previous drift hazard.
- **Empty email buckets are reclaimed again** (the `sync.Map` version leaked
  them). Under the email shard lock, add and delete are mutually exclusive, so
  reclaiming is race-free — the `ponytail` caveat is gone.
- **`disconnectInfo` renamed to `snapshot`** (it builds info for connects too).

Behavior is unchanged: all 28 unit tests pass against the sharded tracker.

## Methodology

- **Mutex** and **SyncMap** are faithful in-test copies (`mutexTracker`,
  `syncMapTracker`) that store the same `*ConnEntry` and share an identical
  `emit` overhead (one lock + copy of an empty subscriber slice).
- **Sharded** is the **real production** `*connectiontracker.Tracker`.
- Concurrent benchmarks use `b.RunParallel`. `ManyUsers` gives each goroutine a
  distinct email; `HotUser` funnels all goroutines through one email.

```
goos: windows  goarch: amd64
cpu:  AMD Ryzen 9 7945HX (GOMAXPROCS=8)
go test -bench . -benchmem -benchtime=1s -count=6 -cpu=8
```

Figures are the mean of 6 runs. **Bold** = best of the three.

## Results (ns/op)

| Benchmark | Mutex | SyncMap | Sharded |
|---|---:|---:|---:|
| `MixedWorkload` (60/30/10, many users) | 401 | 185 | **124** |
| `Register` (many users) | 486 | 265 | **178** |
| `RegisterUnregister` (many users) | 262 | 334 | **216** |
| `ListConnections` (1k conns, parallel) | 80,700 | 71,100 | **29,600** |
| `RegisterUnregister` (hot key) | **193** | 371 | 339 |
| `CancelAll` (100 conns) | **6,074** | 12,995 | 9,020 |
| `GetConnCount` (1k conns, single user) | 29.3 | **2.3** | 33.1 |

## Allocations (B/op, allocs/op)

| Benchmark | Mutex | SyncMap | Sharded |
|---|---|---|---|
| `MixedWorkload` | 140 B, 0 | 271 B, 4 | **142 B, 0** |
| `Register` | **244 B, 1** | 428 B, 7 | 259 B, 1 |
| `RegisterUnregister` (many users) | **320 B, 3** | 335 B, 7 | 320 B, 3 |
| `ListConnections` | 122.9 KB, 1 | 401.2 KB, 12 | **122.9 KB, 1** |
| `CancelAll` | 0 B, 0 | 0 B, 0 | 0 B, 0 |
| `GetConnCount` | 0 B, 0 | 0 B, 0 | 0 B, 0 |

## Analysis

- **Realistic traffic favors sharding decisively.** `MixedWorkload` (many users,
  mostly register/unregister with some reads) is **3.2x faster than the mutex**
  and **1.5x faster than `sync.Map`**, with zero allocations — versus 4 for
  `sync.Map`.
- **Registration scales.** `Register` is **2.7x** the mutex; spreading IDs
  across shards removes the single-lock bottleneck while keeping one allocation
  (the `ConnEntry`) — `sync.Map` pays 7 because it boxes the key and bucket.
- **`ListConnections` now leads** after presizing the result from a cheap
  length pass: parallel readers take `RLock`s on different shards instead of
  serializing on one mutex, and it allocates once (123 KB) where `sync.Map`
  allocates twelve times (401 KB).
- **Where sharding isn't first:**
  - **`CancelAll`** — the single mutex wins (one lock, walk one map), but
    sharded is still **1.4x faster than `sync.Map`**. In absolute terms ~9 µs to
    drop 100 connections, off any hot path (runs once per user removal).
  - **Hot-key churn** — all operations on one email hit one email-shard, so
    sharding can't help and the extra hashing makes it trail the single mutex.
    Real traffic spreads across users, where sharded is fastest.
  - **`GetConnCount` for one hammered user** — `sync.Map`'s atomic counter reads
    in ~2 ns; sharded pays the `RLock`. Across many users the reads hit
    different shards and scale; the single-user case is the worst case. Adding a
    per-shard atomic counter would close this but reintroduces a drift-prone
    mirror, so it was left out.

## Conclusion

Sharded RWMutex is the best general-purpose fit: it wins the workloads that
actually run hot (registration and mixed concurrent traffic), keeps allocations
minimal, leads connection listing, and beats `sync.Map` on the forced-disconnect
path the feature is named after. The only clear `sync.Map` win is single-user
count polling; the only clear single-mutex win is one-hot-key churn — neither is
the dominant production pattern.

Possible follow-ups if profiling ever flags them:
- Per-shard atomic connection counters for ~2 ns `GetConnCount` (at the cost of
  a counter that must be kept in sync with the maps).
- A coarse activity clock in `TrackedConn.Read/Write` — `time.Now()` runs per
  buffer, far hotter than register; trading timestamp resolution would cut
  per-I/O cost. Left out here because it changes observable `LastActivity`
  granularity.

## Reproduce

```bash
cd app/connectiontracker
go test -run '^$' -bench . -benchmem -benchtime=1s -count=6 -cpu=8
go test ./...   # behavior: all unit tests
```
