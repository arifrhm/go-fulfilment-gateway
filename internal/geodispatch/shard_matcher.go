package geodispatch

import (
	"errors"
	"hash/fnv"
	"sync"
)

var ErrNoDriverAvailable = errors.New("no idle driver available in geo cell")

type Driver struct {
	ID     string
	CellID string
	Busy   bool
}

type shard struct {
	mu      sync.Mutex
	drivers map[string][]*Driver // cellID -> drivers in cell
}

// Matcher implements a sharded in-memory geo-cell driver allocation engine.
// Partitioning by geo-cell eliminates global lock contention during rush-hour surges
// and guarantees zero double-dispatch per driver.
type Matcher struct {
	shards []shard
}

// NewMatcher creates a sharded geo-dispatch matcher with shardCount partitions.
func NewMatcher(shardCount int) *Matcher {
	if shardCount <= 0 {
		shardCount = 64
	}
	shards := make([]shard, shardCount)
	for i := range shards {
		shards[i].drivers = make(map[string][]*Driver)
	}
	return &Matcher{shards: shards}
}

func (m *Matcher) shardFor(cellID string) *shard {
	h := fnv.New32a()
	_, _ = h.Write([]byte(cellID))
	idx := int(h.Sum32()) % len(m.shards)
	return &m.shards[idx]
}

// RegisterDriver places an idle driver into a geo-cell.
func (m *Matcher) RegisterDriver(driverID, cellID string) {
	sh := m.shardFor(cellID)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	sh.drivers[cellID] = append(sh.drivers[cellID], &Driver{
		ID:     driverID,
		CellID: cellID,
		Busy:   false,
	})
}

// Allocate atomically claims the first idle driver in cellID, preventing double-dispatch.
func (m *Matcher) Allocate(cellID string) (string, error) {
	sh := m.shardFor(cellID)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	for _, d := range sh.drivers[cellID] {
		if !d.Busy {
			d.Busy = true
			return d.ID, nil
		}
	}
	return "", ErrNoDriverAvailable
}

// Release marks a driver as idle again after trip completion or cancellation.
func (m *Matcher) Release(cellID, driverID string) bool {
	sh := m.shardFor(cellID)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	for _, d := range sh.drivers[cellID] {
		if d.ID == driverID {
			d.Busy = false
			return true
		}
	}
	return false
}
