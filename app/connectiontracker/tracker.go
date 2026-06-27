// Package connectiontracker provides a thread-safe registry of active proxy
// connections. It enables forced per-user disconnection and exposes real-time
// per-connection metadata and traffic statistics for API consumers.
package connectiontracker

import (
	"context"
	"hash/maphash"
	"io"
	"sync"
	"sync/atomic"
	"time"

	B "github.com/sagernet/sing/common/buf"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/xtls/xray-core/transport/internet/stat"
)

// ConnEntry holds metadata and traffic state for a single tracked connection.
type ConnEntry struct {
	Email        string
	InboundTag   string
	Protocol     string
	Cancel       context.CancelFunc
	StartTime    time.Time
	lastActivity int64 // atomic, Unix nanosecond timestamp
	uplink       int64 // atomic, bytes received from client
	downlink     int64 // atomic, bytes sent to client
	closerMu     sync.Mutex
	closer       io.Closer
}

// ConnectionInfo is a read-only snapshot of an active connection's state.
type ConnectionInfo struct {
	ID           uint64
	Email        string
	InboundTag   string
	Protocol     string
	StartTime    time.Time
	LastActivity time.Time
	Uplink       int64
	Downlink     int64
}

// Manager holds the shared connection registry and subscription fan-out for
// a single Xray instance.
type Manager struct {
	globalNext uint64

	globalMu sync.Mutex
	trackers []*Tracker

	subMu       sync.Mutex
	subscribers []chan WatchEvent
}

// shardCount is the number of lock shards for each index. A power of two so
// the shard can be selected with a mask. 64 spreads contention well across the
// many goroutines a busy proxy runs without wasting much memory per Tracker.
const (
	shardCount = 64
	shardMask  = shardCount - 1
)

// emailHashSeed seeds the email->shard hash. Process-local; it only affects
// shard distribution, never correctness.
var emailHashSeed = maphash.MakeSeed()

// idShard holds one slice of the flat id->entry index under its own lock.
// Sharding by connection ID spreads Register/Unregister/CloseConn contention.
type idShard struct {
	mu   sync.RWMutex
	byID map[uint64]*ConnEntry
}

// emailShard holds one slice of the per-email grouping under its own lock. The
// grouping powers CancelAll, GetConnCount, and per-user stats.
type emailShard struct {
	mu      sync.RWMutex
	byEmail map[string]map[uint64]*ConnEntry // email -> id -> entry
}

// Tracker tracks active connections per user, enabling forced disconnection
// and real-time connection inspection.
//
// It keeps two sharded indexes that always move together:
//   - ids:    id -> entry, sharded by id, for O(1) lookup/removal by id.
//   - emails: email -> {id -> entry}, sharded by email, for per-user ops.
//
// Sharded RWMutexes were chosen over a single mutex (serializes every op) and
// over sync.Map (boxes keys, slower bulk CancelAll): reads take read locks and
// independent connections fall on different shards, so the hot paths rarely
// contend. A Tracker must not be copied after first use.
type Tracker struct {
	manager *Manager

	ids    [shardCount]idShard
	emails [shardCount]emailShard
}

// WatchEvent is delivered to subscribers whenever a connection opens or closes.
type WatchEvent struct {
	Connected bool // true = opened, false = closed
	Info      ConnectionInfo
}

// NewManager creates an empty tracker manager.
func NewManager() *Manager {
	return &Manager{}
}

// Close clears manager-owned registries.
func (m *Manager) Close() error {
	m.globalMu.Lock()
	m.trackers = nil
	m.globalMu.Unlock()

	m.subMu.Lock()
	m.subscribers = nil
	m.subMu.Unlock()

	return nil
}

func (m *Manager) snapshotTrackers() []*Tracker {
	m.globalMu.Lock()
	trackers := make([]*Tracker, len(m.trackers))
	copy(trackers, m.trackers)
	m.globalMu.Unlock()
	return trackers
}

// Subscribe returns a channel that receives WatchEvents. Call Unsubscribe when
// done to avoid a goroutine / channel leak.
func (m *Manager) Subscribe() chan WatchEvent {
	ch := make(chan WatchEvent, 64)
	m.subMu.Lock()
	m.subscribers = append(m.subscribers, ch)
	m.subMu.Unlock()
	return ch
}

// Unsubscribe removes a channel returned by Subscribe.
func (m *Manager) Unsubscribe(ch chan WatchEvent) {
	m.subMu.Lock()
	defer m.subMu.Unlock()

	for i, s := range m.subscribers {
		if s == ch {
			m.subscribers[i] = m.subscribers[len(m.subscribers)-1]
			m.subscribers = m.subscribers[:len(m.subscribers)-1]
			return
		}
	}
}

func (m *Manager) emit(ev WatchEvent) {
	m.subMu.Lock()
	subs := make([]chan WatchEvent, len(m.subscribers))
	copy(subs, m.subscribers)
	m.subMu.Unlock()
	for _, ch := range subs {
		select {
		case ch <- ev:
		default: // drop if subscriber is too slow
		}
	}
}

// NewTracker creates a new, empty Tracker and registers it in the manager so
// that ListAllConnections and CloseGlobalConn can see its connections.
func (m *Manager) NewTracker() *Tracker {
	t := &Tracker{manager: m}
	for i := range t.ids {
		t.ids[i].byID = make(map[uint64]*ConnEntry)
	}
	for i := range t.emails {
		t.emails[i].byEmail = make(map[string]map[uint64]*ConnEntry)
	}
	m.globalMu.Lock()
	m.trackers = append(m.trackers, t)
	m.globalMu.Unlock()
	return t
}

// New creates a new Tracker with its own Manager.
func New() *Tracker {
	return NewManager().NewTracker()
}

func (t *Tracker) idShard(id uint64) *idShard {
	return &t.ids[id&shardMask]
}

func (t *Tracker) emailShard(email string) *emailShard {
	return &t.emails[maphash.String(emailHashSeed, email)&shardMask]
}

// addEntry inserts entry into both indexes. The two indexes always move
// together; this is the single place that establishes that invariant.
func (t *Tracker) addEntry(id uint64, email string, entry *ConnEntry) {
	is := t.idShard(id)
	is.mu.Lock()
	is.byID[id] = entry
	is.mu.Unlock()

	es := t.emailShard(email)
	es.mu.Lock()
	bucket := es.byEmail[email]
	if bucket == nil {
		bucket = make(map[uint64]*ConnEntry)
		es.byEmail[email] = bucket
	}
	bucket[id] = entry
	es.mu.Unlock()
}

// takeID removes id from the flat index and reports whether this call is the
// one that removed it. It is the exactly-once guard shared by Unregister,
// CloseConn, and CancelAll: only the winner emits and cancels, even when they
// race for the same connection.
func (t *Tracker) takeID(id uint64) (*ConnEntry, bool) {
	is := t.idShard(id)
	is.mu.Lock()
	entry, ok := is.byID[id]
	if ok {
		delete(is.byID, id)
	}
	is.mu.Unlock()
	return entry, ok
}

// removeFromEmail drops id from email's grouping and reclaims the bucket once
// it empties. Under the email shard lock add and remove are mutually exclusive,
// so reclaiming here is race-free.
func (t *Tracker) removeFromEmail(email string, id uint64) {
	es := t.emailShard(email)
	es.mu.Lock()
	if bucket := es.byEmail[email]; bucket != nil {
		delete(bucket, id)
		if len(bucket) == 0 {
			delete(es.byEmail, email)
		}
	}
	es.mu.Unlock()
}

// snapshot builds a read-only ConnectionInfo for an event or a listing.
func snapshot(id uint64, entry *ConnEntry) ConnectionInfo {
	return ConnectionInfo{
		ID:           id,
		Email:        entry.Email,
		InboundTag:   entry.InboundTag,
		Protocol:     entry.Protocol,
		StartTime:    entry.StartTime,
		LastActivity: time.Unix(0, atomic.LoadInt64(&entry.lastActivity)),
		Uplink:       atomic.LoadInt64(&entry.uplink),
		Downlink:     atomic.LoadInt64(&entry.downlink),
	}
}

// ListAllConnections returns a snapshot of every active connection across all
// Tracker instances that were created by NewTracker.
func (m *Manager) ListAllConnections() []ConnectionInfo {
	ts := m.snapshotTrackers()
	var all []ConnectionInfo
	for _, t := range ts {
		all = append(all, t.ListConnections()...)
	}
	return all
}

// GetUserStats returns the aggregate uplink bytes, downlink bytes, and active
// connection count for email across all registered Trackers.
func (m *Manager) GetUserStats(email string) (uplink, downlink int64, connCount int32) {
	ts := m.snapshotTrackers()
	for _, t := range ts {
		u, d, c := t.userStats(email)
		uplink += u
		downlink += d
		connCount += c
	}
	return
}

// userStats sums the traffic counters and counts the live connections for
// email within this Tracker.
func (t *Tracker) userStats(email string) (uplink, downlink int64, connCount int32) {
	es := t.emailShard(email)
	es.mu.RLock()
	for _, e := range es.byEmail[email] {
		uplink += atomic.LoadInt64(&e.uplink)
		downlink += atomic.LoadInt64(&e.downlink)
		connCount++
	}
	es.mu.RUnlock()
	return
}

// CloseGlobalConn closes the connection with the given ID in whichever Tracker
// owns it. Returns true if the connection was found and cancelled.
func (m *Manager) CloseGlobalConn(id uint64) bool {
	ts := m.snapshotTrackers()
	for _, t := range ts {
		if t.CloseConn(id) {
			return true
		}
	}
	return false
}

// Register records a connection's cancel function under email and returns its
// ID. Use RegisterWithMeta for richer per-connection tracking.
func (t *Tracker) Register(email string, cancel context.CancelFunc) uint64 {
	id, _ := t.RegisterWithMeta(email, cancel, "", "")
	return id
}

// RegisterWithMeta records a connection with full metadata and returns the
// connection ID and a *ConnEntry whose traffic counters can be updated by
// passing it to WrapConn.
func (t *Tracker) RegisterWithMeta(email string, cancel context.CancelFunc, inboundTag, protocol string) (uint64, *ConnEntry) {
	now := time.Now()
	entry := &ConnEntry{
		Email:      email,
		InboundTag: inboundTag,
		Protocol:   protocol,
		Cancel:     cancel,
		StartTime:  now,
	}
	atomic.StoreInt64(&entry.lastActivity, now.UnixNano())
	id := atomic.AddUint64(&t.manager.globalNext, 1)
	t.addEntry(id, email, entry)
	t.manager.emit(WatchEvent{Connected: true, Info: ConnectionInfo{
		ID:           id,
		Email:        email,
		InboundTag:   inboundTag,
		Protocol:     protocol,
		StartTime:    now,
		LastActivity: now,
	}})
	return id, entry
}

// Unregister removes a connection from the tracker when it closes naturally.
func (t *Tracker) Unregister(email string, id uint64) {
	entry, ok := t.takeID(id)
	if !ok {
		return
	}
	t.removeFromEmail(email, id)
	t.manager.emit(WatchEvent{Connected: false, Info: snapshot(id, entry)})
}

// CancelAll cancels every active connection belonging to email.
func (t *Tracker) CancelAll(email string) {
	es := t.emailShard(email)
	es.mu.Lock()
	bucket := es.byEmail[email]
	delete(es.byEmail, email)
	es.mu.Unlock()

	for id, entry := range bucket {
		// takeID is the exactly-once guard against a racing CloseConn.
		if _, won := t.takeID(id); won {
			t.manager.emit(WatchEvent{Connected: false, Info: snapshot(id, entry)})
			entry.cancelAndClose()
		}
	}
}

// CloseConn cancels the connection identified by id.
// Returns true if the connection was found and cancelled.
func (t *Tracker) CloseConn(id uint64) bool {
	entry, ok := t.takeID(id)
	if !ok {
		return false
	}
	t.removeFromEmail(entry.Email, id)
	t.manager.emit(WatchEvent{Connected: false, Info: snapshot(id, entry)})
	entry.cancelAndClose()
	return true
}

// GetConnCount returns the number of active connections for email.
func (t *Tracker) GetConnCount(email string) int {
	es := t.emailShard(email)
	es.mu.RLock()
	n := len(es.byEmail[email])
	es.mu.RUnlock()
	return n
}

// ListConnections returns a snapshot of all currently active connections.
func (t *Tracker) ListConnections() []ConnectionInfo {
	total := 0
	for i := range t.ids {
		t.ids[i].mu.RLock()
		total += len(t.ids[i].byID)
		t.ids[i].mu.RUnlock()
	}
	result := make([]ConnectionInfo, 0, total)
	for i := range t.ids {
		s := &t.ids[i]
		s.mu.RLock()
		for id, entry := range s.byID {
			result = append(result, snapshot(id, entry))
		}
		s.mu.RUnlock()
	}
	return result
}

// TrackedConn wraps a stat.Connection and records per-connection traffic
// counters into the associated ConnEntry. Obtain one via WrapConn.
type TrackedConn struct {
	stat.Connection
	entry *ConnEntry
}

func (c *TrackedConn) updateActivity(uplink, downlink int64) {
	now := time.Now().UnixNano()
	if uplink > 0 {
		atomic.AddInt64(&c.entry.uplink, uplink)
	}
	if downlink > 0 {
		atomic.AddInt64(&c.entry.downlink, downlink)
	}
	if uplink > 0 || downlink > 0 {
		atomic.StoreInt64(&c.entry.lastActivity, now)
	}
}

func (c *TrackedConn) Read(b []byte) (int, error) {
	n, err := c.Connection.Read(b)
	if n > 0 {
		c.updateActivity(int64(n), 0)
	}
	return n, err
}

func (c *TrackedConn) Write(b []byte) (int, error) {
	n, err := c.Connection.Write(b)
	if n > 0 {
		c.updateActivity(0, int64(n))
	}
	return n, err
}

// WrapConn wraps conn so that every Read and Write updates the traffic
// counters in entry. Call after RegisterWithMeta to enable byte-level tracking.
func WrapConn(conn stat.Connection, entry *ConnEntry) stat.Connection {
	if entry != nil {
		entry.setCloser(conn)
	}
	return &TrackedConn{Connection: conn, entry: entry}
}

// TrackedPacketConn wraps an N.PacketConn (UDP) and records per-connection
// traffic counters into the associated ConnEntry.
type TrackedPacketConn struct {
	N.PacketConn
	entry *ConnEntry
}

func (c *TrackedPacketConn) updateActivity(uplink, downlink int64) {
	now := time.Now().UnixNano()
	if uplink > 0 {
		atomic.AddInt64(&c.entry.uplink, uplink)
	}
	if downlink > 0 {
		atomic.AddInt64(&c.entry.downlink, downlink)
	}
	atomic.StoreInt64(&c.entry.lastActivity, now)
}

func (c *TrackedPacketConn) ReadPacket(buffer *B.Buffer) (M.Socksaddr, error) {
	addr, err := c.PacketConn.ReadPacket(buffer)
	if err == nil && buffer.Len() > 0 {
		c.updateActivity(int64(buffer.Len()), 0)
	}
	return addr, err
}

func (c *TrackedPacketConn) WritePacket(buffer *B.Buffer, destination M.Socksaddr) error {
	n := buffer.Len()
	err := c.PacketConn.WritePacket(buffer, destination)
	if err == nil && n > 0 {
		c.updateActivity(0, int64(n))
	}
	return err
}

// WrapPacketConn wraps a UDP PacketConn so that every ReadPacket and WritePacket
// updates the traffic counters in entry. Call after RegisterWithMeta to enable
// byte-level tracking for UDP connections.
func WrapPacketConn(conn N.PacketConn, entry *ConnEntry) N.PacketConn {
	if entry != nil {
		entry.setCloser(conn)
	}
	return &TrackedPacketConn{PacketConn: conn, entry: entry}
}

func (e *ConnEntry) setCloser(c io.Closer) {
	if e == nil || c == nil {
		return
	}
	e.closerMu.Lock()
	e.closer = c
	e.closerMu.Unlock()
}

func (e *ConnEntry) closeCloser() {
	if e == nil {
		return
	}
	e.closerMu.Lock()
	closer := e.closer
	e.closerMu.Unlock()
	if closer == nil {
		return
	}
	_ = closer.Close()
}

func (e *ConnEntry) cancelAndClose() {
	if e == nil {
		return
	}
	if e.Cancel != nil {
		e.Cancel()
	}
	e.closeCloser()
}
