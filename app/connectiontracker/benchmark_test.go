package connectiontracker_test

import (
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/xtls/xray-core/app/connectiontracker"
)

// This file compares three implementations of the connection registry:
//
//   - Mutex:   a single sync.Mutex guarding a nested map (the original design),
//              reproduced here as mutexTracker.
//   - SyncMap: the sync.Map-based design, reproduced here as syncMapTracker.
//   - Sharded: the current production *connectiontracker.Tracker, which shards
//              two RWMutex-guarded maps.
//
// All three store the same *connectiontracker.ConnEntry value and perform an
// equivalent "emit" (one mutex lock + copy of an empty subscriber slice) on
// every mutating call, so the only variable measured is the data/locking
// strategy itself.

var noopCancel = func() {}

// benchTracker is the minimal surface all implementations share.
type benchTracker interface {
	register(email string) uint64
	unregister(email string, id uint64)
	cancelAll(email string)
	closeConn(id uint64) bool
	connCount(email string) int
	listLen() int
}

// emitOverhead reproduces Manager.emit's cost with zero subscribers: lock,
// snapshot the (empty) subscriber slice, fan out. Shared by the two test-local
// trackers so their overhead matches the production tracker's.
type emitOverhead struct {
	mu   sync.Mutex
	subs []chan struct{}
}

func (e *emitOverhead) emit() {
	e.mu.Lock()
	subs := make([]chan struct{}, len(e.subs))
	copy(subs, e.subs)
	e.mu.Unlock()
	for _, ch := range subs {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

// --- Sharded side: thin adapter over the real production Tracker ---

type shardedAdapter struct{ t *connectiontracker.Tracker }

func (a shardedAdapter) register(email string) uint64       { return a.t.Register(email, noopCancel) }
func (a shardedAdapter) unregister(email string, id uint64) { a.t.Unregister(email, id) }
func (a shardedAdapter) cancelAll(email string)             { a.t.CancelAll(email) }
func (a shardedAdapter) closeConn(id uint64) bool           { return a.t.CloseConn(id) }
func (a shardedAdapter) connCount(email string) int         { return a.t.GetConnCount(email) }
func (a shardedAdapter) listLen() int                       { return len(a.t.ListConnections()) }

// --- Mutex side: faithful copy of the original map+mutex design ---

type mutexTracker struct {
	emitOverhead
	globalNext uint64

	mu    sync.Mutex
	conns map[string]map[uint64]*connectiontracker.ConnEntry
	byID  map[uint64]*connectiontracker.ConnEntry
}

func newMutexTracker() *mutexTracker {
	return &mutexTracker{
		conns: make(map[string]map[uint64]*connectiontracker.ConnEntry),
		byID:  make(map[uint64]*connectiontracker.ConnEntry),
	}
}

func (t *mutexTracker) register(email string) uint64 {
	id := atomic.AddUint64(&t.globalNext, 1)
	entry := &connectiontracker.ConnEntry{Email: email, Cancel: noopCancel}
	t.mu.Lock()
	if t.conns[email] == nil {
		t.conns[email] = make(map[uint64]*connectiontracker.ConnEntry)
	}
	t.conns[email][id] = entry
	t.byID[id] = entry
	t.mu.Unlock()
	t.emit()
	return id
}

func (t *mutexTracker) unregister(email string, id uint64) {
	t.mu.Lock()
	entry := t.byID[id]
	delete(t.byID, id)
	if m := t.conns[email]; m != nil {
		delete(m, id)
		if len(m) == 0 {
			delete(t.conns, email)
		}
	}
	t.mu.Unlock()
	if entry != nil {
		t.emit()
	}
}

func (t *mutexTracker) cancelAll(email string) {
	t.mu.Lock()
	entries := t.conns[email]
	delete(t.conns, email)
	for id := range entries {
		delete(t.byID, id)
	}
	t.mu.Unlock()

	for _, entry := range entries {
		t.emit()
		if entry.Cancel != nil {
			entry.Cancel()
		}
	}
}

func (t *mutexTracker) closeConn(id uint64) bool {
	t.mu.Lock()
	entry, ok := t.byID[id]
	if ok {
		delete(t.byID, id)
		if m := t.conns[entry.Email]; m != nil {
			delete(m, id)
			if len(m) == 0 {
				delete(t.conns, entry.Email)
			}
		}
	}
	t.mu.Unlock()
	if ok {
		t.emit()
		if entry.Cancel != nil {
			entry.Cancel()
		}
	}
	return ok
}

func (t *mutexTracker) connCount(email string) int {
	t.mu.Lock()
	n := len(t.conns[email])
	t.mu.Unlock()
	return n
}

func (t *mutexTracker) listLen() int {
	t.mu.Lock()
	result := make([]connectiontracker.ConnectionInfo, 0, len(t.byID))
	for id, entry := range t.byID {
		result = append(result, connectiontracker.ConnectionInfo{
			ID:         id,
			Email:      entry.Email,
			InboundTag: entry.InboundTag,
			Protocol:   entry.Protocol,
			StartTime:  entry.StartTime,
		})
	}
	t.mu.Unlock()
	return len(result)
}

// --- SyncMap side: faithful copy of the sync.Map design ---

type syncEmailBucket struct {
	conns sync.Map // uint64 (id) -> *ConnEntry
	count int64    // atomic
}

type syncMapTracker struct {
	emitOverhead
	globalNext uint64
	byID       sync.Map // uint64 -> *ConnEntry
	byEmail    sync.Map // string -> *syncEmailBucket
}

func newSyncMapTracker() *syncMapTracker { return &syncMapTracker{} }

func (t *syncMapTracker) register(email string) uint64 {
	id := atomic.AddUint64(&t.globalNext, 1)
	entry := &connectiontracker.ConnEntry{Email: email, Cancel: noopCancel}
	t.byID.Store(id, entry)
	bi, _ := t.byEmail.LoadOrStore(email, &syncEmailBucket{})
	b := bi.(*syncEmailBucket)
	b.conns.Store(id, entry)
	atomic.AddInt64(&b.count, 1)
	t.emit()
	return id
}

func (t *syncMapTracker) removeFromBucket(email string, id uint64) {
	if bi, ok := t.byEmail.Load(email); ok {
		b := bi.(*syncEmailBucket)
		if _, ok := b.conns.LoadAndDelete(id); ok {
			atomic.AddInt64(&b.count, -1)
		}
	}
}

func (t *syncMapTracker) unregister(email string, id uint64) {
	if _, ok := t.byID.LoadAndDelete(id); !ok {
		return
	}
	t.removeFromBucket(email, id)
	t.emit()
}

func (t *syncMapTracker) cancelAll(email string) {
	bi, ok := t.byEmail.Load(email)
	if !ok {
		return
	}
	b := bi.(*syncEmailBucket)
	b.conns.Range(func(k, v any) bool {
		id := k.(uint64)
		if _, won := t.byID.LoadAndDelete(id); won {
			b.conns.Delete(id)
			atomic.AddInt64(&b.count, -1)
			entry := v.(*connectiontracker.ConnEntry)
			t.emit()
			if entry.Cancel != nil {
				entry.Cancel()
			}
		}
		return true
	})
}

func (t *syncMapTracker) closeConn(id uint64) bool {
	ei, ok := t.byID.LoadAndDelete(id)
	if !ok {
		return false
	}
	entry := ei.(*connectiontracker.ConnEntry)
	t.removeFromBucket(entry.Email, id)
	t.emit()
	if entry.Cancel != nil {
		entry.Cancel()
	}
	return true
}

func (t *syncMapTracker) connCount(email string) int {
	if bi, ok := t.byEmail.Load(email); ok {
		return int(atomic.LoadInt64(&bi.(*syncEmailBucket).count))
	}
	return 0
}

func (t *syncMapTracker) listLen() int {
	var result []connectiontracker.ConnectionInfo
	t.byID.Range(func(k, v any) bool {
		entry := v.(*connectiontracker.ConnEntry)
		result = append(result, connectiontracker.ConnectionInfo{
			ID:         k.(uint64),
			Email:      entry.Email,
			InboundTag: entry.InboundTag,
			Protocol:   entry.Protocol,
			StartTime:  entry.StartTime,
		})
		return true
	})
	return len(result)
}

// impls is the matrix benchmarked under identical workloads.
var impls = []struct {
	name string
	new  func() benchTracker
}{
	{"Mutex", func() benchTracker { return newMutexTracker() }},
	{"SyncMap", func() benchTracker { return newSyncMapTracker() }},
	{"Sharded", func() benchTracker { return shardedAdapter{connectiontracker.New()} }},
}

func emailFor(hot bool, gid int64) string {
	if hot {
		return "shared@example.com"
	}
	return "user-" + strconv.FormatInt(gid, 10) + "@example.com"
}

// BenchmarkRegisterUnregister measures the per-connection lifecycle (the
// hottest path: every accepted connection registers on open and unregisters on
// close). ManyUsers gives each goroutine its own email; HotUser funnels every
// goroutine through a single email to maximize key contention.
func BenchmarkRegisterUnregister(b *testing.B) {
	for _, impl := range impls {
		b.Run(impl.name, func(b *testing.B) {
			for _, hot := range []bool{false, true} {
				name := "ManyUsers"
				if hot {
					name = "HotUser"
				}
				b.Run(name, func(b *testing.B) {
					t := impl.new()
					var gid int64
					b.ReportAllocs()
					b.ResetTimer()
					b.RunParallel(func(pb *testing.PB) {
						email := emailFor(hot, atomic.AddInt64(&gid, 1))
						for pb.Next() {
							id := t.register(email)
							t.unregister(email, id)
						}
					})
				})
			}
		})
	}
}

// BenchmarkRegister measures registration only (monotonic growth), the
// allocation-heavy half of the lifecycle.
func BenchmarkRegister(b *testing.B) {
	for _, impl := range impls {
		b.Run(impl.name, func(b *testing.B) {
			t := impl.new()
			var gid int64
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				email := emailFor(false, atomic.AddInt64(&gid, 1))
				for pb.Next() {
					t.register(email)
				}
			})
		})
	}
}

// BenchmarkCancelAll measures forced disconnection of every connection for one
// user (the gRPC "remove user" path) with a bucket of connsPerUser entries.
func BenchmarkCancelAll(b *testing.B) {
	const connsPerUser = 100
	for _, impl := range impls {
		b.Run(impl.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				t := impl.new()
				for j := 0; j < connsPerUser; j++ {
					t.register("victim@example.com")
				}
				b.StartTimer()
				t.cancelAll("victim@example.com")
			}
		})
	}
}

// BenchmarkGetConnCount measures concurrent count reads against a populated
// single-user bucket.
func BenchmarkGetConnCount(b *testing.B) {
	const conns = 1000
	for _, impl := range impls {
		b.Run(impl.name, func(b *testing.B) {
			t := impl.new()
			for i := 0; i < conns; i++ {
				t.register("user@example.com")
			}
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					_ = t.connCount("user@example.com")
				}
			})
		})
	}
}

// BenchmarkListConnections measures snapshotting all active connections while a
// population of conns entries is present.
func BenchmarkListConnections(b *testing.B) {
	const conns = 1000
	for _, impl := range impls {
		b.Run(impl.name, func(b *testing.B) {
			t := impl.new()
			for i := 0; i < conns; i++ {
				t.register(emailFor(false, int64(i%50)))
			}
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					_ = t.listLen()
				}
			})
		})
	}
}

// BenchmarkMixedWorkload models realistic concurrent traffic: ~60% register,
// ~30% unregister, ~10% reads, against many users.
func BenchmarkMixedWorkload(b *testing.B) {
	for _, impl := range impls {
		b.Run(impl.name, func(b *testing.B) {
			t := impl.new()
			var gid int64
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				id := atomic.AddInt64(&gid, 1)
				email := emailFor(false, id)
				rng := rand.New(rand.NewSource(id))
				active := make([]uint64, 0, 16)
				for pb.Next() {
					switch rng.Intn(10) {
					case 0, 1, 2, 3, 4, 5: // register
						active = append(active, t.register(email))
					case 6, 7, 8: // unregister oldest, if any
						if len(active) > 0 {
							t.unregister(email, active[0])
							active = active[1:]
						}
					default: // read
						_ = t.connCount(email)
					}
				}
			})
		})
	}
}
