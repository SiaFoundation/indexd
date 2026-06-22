package client

import (
	"cmp"
	"iter"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/indexd/hosts"
)

const (
	emaAlpha            = 0.2
	settingsPayloadSize = 270 // size of host settings in bytes

	// defaultReadThroughput is the assumed read rate before bulk reads are sampled
	defaultReadThroughput = 1 << 20 // 1 MiB/s

	// defaultWriteThroughput is the assumed write rate before bulk writes are sampled
	defaultWriteThroughput = (1 << 22) / 5 // 4 MiB sector / 5s

	// minThroughputSampleBytes is the smallest transfer that feeds the network-wide throughput estimate
	minThroughputSampleBytes = 1 << 16 // 64 KiB
)

type rpcAverage struct {
	value float64
	init  bool
}

// A Store provides a list of usable hosts.
type Store interface {
	UsableHosts() ([]hosts.HostInfo, error)
	Addresses(types.PublicKey) ([]chain.NetAddress, error)
	Usable(types.PublicKey) (bool, error)
}

// AddSample adds a new sample to the exponential moving average.
func (ra *rpcAverage) AddSample(v float64) {
	if !ra.init {
		ra.value = v
		ra.init = true
	} else {
		ra.value = emaAlpha*v + (1.0-emaAlpha)*ra.value
	}
}

// Value returns the current average and whether any samples have been
// recorded.
func (ra *rpcAverage) Value() (float64, bool) {
	return ra.value, ra.init
}

type failureRate struct {
	value       float64
	init        bool
	lastAttempt time.Time
}

// AddSample adds a new success/failure sample to the failure rate.
func (fr *failureRate) AddSample(success bool) {
	sample := 1.0
	if success {
		sample = 0.0
	}
	if !fr.init {
		fr.value = sample
		fr.init = true
	} else {
		fr.value = emaAlpha*sample + (1.0-emaAlpha)*fr.value
	}
	fr.lastAttempt = time.Now()
}

func (fr *failureRate) Value() float64 {
	if fr.init && time.Since(fr.lastAttempt) >= 5*time.Minute {
		elapsed := time.Since(fr.lastAttempt).Minutes() / 5
		fr.value *= math.Pow(1.0-emaAlpha, elapsed)
		fr.lastAttempt = time.Now()
	}
	return fr.value
}

type hostMetric struct {
	rpcWriteAverage rpcAverage
	rpcReadAverage  rpcAverage
	rpcFailRate     failureRate

	// inflight tracks the number of read/write RPCs currently in flight to
	// this host. The score divides throughput by (inflight + 1) so a
	// host already serving many shards gets demoted relative to its idle
	// peers, spreading concurrent slab work across the candidate pool
	// instead of piling onto the same top-N.
	inflightReads  atomic.Int64
	inflightWrites atomic.Int64
}

// combinedThroughput returns the average of read and write throughput
// and whether either side has been sampled.
func (hm *hostMetric) combinedThroughput() (float64, bool) {
	r, rok := hm.rpcReadAverage.Value()
	w, wok := hm.rpcWriteAverage.Value()
	switch {
	case rok && wok:
		return (r + w) / 2, true
	case rok:
		return r, true
	case wok:
		return w, true
	default:
		return 0, false
	}
}

// inflight returns the total number of read+write RPCs currently in
// flight to this host.
func (hm *hostMetric) inflight() int64 {
	return hm.inflightReads.Load() + hm.inflightWrites.Load()
}

// A HostQueue manages an ordered queue of hosts for uploading or
// downloading. It tracks per-host attempt counts so callers can
// implement progressive timeouts. It is safe for concurrent use.
type HostQueue struct {
	mu       sync.Mutex
	hosts    []types.PublicKey
	attempts map[types.PublicKey]int
}

// Available returns the number of remaining hosts.
func (q *HostQueue) Available() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.hosts)
}

// Iter returns an iterator that yields hosts and their attempt
// counts one at a time until there are no hosts left.
func (q *HostQueue) Iter() iter.Seq2[types.PublicKey, int] {
	return func(yield func(types.PublicKey, int) bool) {
		for {
			host, attempts, ok := q.Next()
			if !ok || !yield(host, attempts) {
				return
			}
		}
	}
}

// Next pops the next host from the front of the queue. The returned
// attempt count is 1-based: 1 on the first pop, 2 after one retry,
// and so on. If the queue is empty, ok is false.
func (q *HostQueue) Next() (types.PublicKey, int, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.hosts) == 0 {
		return types.PublicKey{}, 0, false
	}
	host := q.hosts[0]
	q.hosts = q.hosts[1:]
	return host, q.attempts[host] + 1, true
}

// Retry pushes the host to the back of the queue and increments
// its attempt counter.
func (q *HostQueue) Retry(host types.PublicKey) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.hosts = append(q.hosts, host)
	q.attempts[host]++
}

// NewHostQueue creates a new HostQueue with the provided hosts,
// deduplicating any repeated keys.
func NewHostQueue(hosts []types.PublicKey) *HostQueue {
	seen := make(map[types.PublicKey]struct{})
	uniqueHosts := make([]types.PublicKey, 0, len(hosts))
	for _, host := range hosts {
		if _, ok := seen[host]; !ok {
			seen[host] = struct{}{}
			uniqueHosts = append(uniqueHosts, host)
		}
	}
	return &HostQueue{
		hosts:    uniqueHosts,
		attempts: make(map[types.PublicKey]int),
	}
}

// A Provider tracks available hosts and their performance over time.
type Provider struct {
	store Store

	mu      sync.Mutex // protects the fields below
	metrics map[types.PublicKey]*hostMetric

	// globalReadThroughput and globalWriteThroughput are network-wide EMAs of
	// bulk transfer throughput in bytes/second, used by Read/WriteEstimate to
	// size adaptive racing.
	globalReadThroughput  rpcAverage
	globalWriteThroughput rpcAverage
}

func (p *Provider) sortHosts(hosts []types.PublicKey) {
	p.mu.Lock()
	defer p.mu.Unlock()
	sort.SliceStable(hosts, func(i, j int) bool {
		return p.cmpMetrics(hosts[i], hosts[j]) < 0
	})
}

// cmpMetrics returns a negative number, zero, or a positive number when
// host a sorts ahead of, equal to, or behind host b. Ordering:
//  1. Lower failure rate wins.
//  2. Unsampled hosts (no read/write samples) outrank sampled ones — the
//     "discovery" bucket so every available host eventually gets tried.
//  3. Among sampled hosts, higher throughput / (inflight + 1) wins — the
//     expected per-shard throughput if you started one more shard. A
//     saturated fast host can lose to an idle slower one, while a
//     genuinely much-faster host still wins even when serving a few
//     shards.
//  4. Among unsampled hosts (rare; only seen before any samples land),
//     lower current inflight wins so concurrent pickers spread out.
func (p *Provider) cmpMetrics(a, b types.PublicKey) int {
	am, aok := p.metrics[a]
	bm, bok := p.metrics[b]
	if !aok && !bok {
		return 0
	} else if !aok {
		return -1
	} else if !bok {
		return 1
	}
	if c := cmp.Compare(am.rpcFailRate.Value(), bm.rpcFailRate.Value()); c != 0 {
		return c
	}

	at, asampled := am.combinedThroughput()
	bt, bsampled := bm.combinedThroughput()
	if !asampled && !bsampled {
		return cmp.Compare(am.inflight(), bm.inflight())
	} else if !asampled {
		return -1
	} else if !bsampled {
		return 1
	}
	aw := at / float64(am.inflight()+1)
	bw := bt / float64(bm.inflight()+1)
	return cmp.Compare(bw, aw)
}

// metric returns the per-host metric pointer, creating it on
// first use. Caller must hold p.mu.
func (p *Provider) metric(hostKey types.PublicKey) *hostMetric {
	m, ok := p.metrics[hostKey]
	if !ok {
		m = &hostMetric{}
		p.metrics[hostKey] = m
	}
	return m
}

// AddReadSample records a successful read RPC attempt to the specified host.
// The throughput is calculated from the number of bytes transferred and the
// elapsed duration.
func (p *Provider) AddReadSample(hostKey types.PublicKey, bytes uint64, elapsed time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	metric := p.metric(hostKey)
	if elapsed > 0 {
		throughput := float64(bytes) / elapsed.Seconds()
		metric.rpcReadAverage.AddSample(throughput)
		// only bulk reads are taken into account for the global estimate
		if bytes >= minThroughputSampleBytes {
			p.globalReadThroughput.AddSample(throughput)
		}
	}
	metric.rpcFailRate.AddSample(true)
}

// ReadEstimate returns the expected time to read the given number of bytes
// based on the network-wide observed read throughput, falling back to
// defaultReadThroughput before any bulk reads have been sampled.
func (p *Provider) ReadEstimate(bytes uint64) time.Duration {
	p.mu.Lock()
	rate, ok := p.globalReadThroughput.Value()
	p.mu.Unlock()
	if !ok || rate <= 0 {
		rate = defaultReadThroughput
	}
	return time.Duration(float64(bytes) / rate * float64(time.Second))
}

// AddWriteSample records a successful write RPC attempt to the specified host.
// The throughput is calculated from the number of bytes transferred and the
// elapsed duration.
func (p *Provider) AddWriteSample(hostKey types.PublicKey, bytes uint64, elapsed time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	metric := p.metric(hostKey)
	if elapsed > 0 {
		throughput := float64(bytes) / elapsed.Seconds()
		metric.rpcWriteAverage.AddSample(throughput)
		// only bulk writes are taken into account for the global estimate
		if bytes >= minThroughputSampleBytes {
			p.globalWriteThroughput.AddSample(throughput)
		}
	}
	metric.rpcFailRate.AddSample(true)
}

// WriteEstimate returns the expected time to write the given number of bytes
// based on the network-wide observed write throughput, falling back to
// defaultWriteThroughput before any bulk writes have been sampled.
func (p *Provider) WriteEstimate(bytes uint64) time.Duration {
	p.mu.Lock()
	rate, ok := p.globalWriteThroughput.Value()
	p.mu.Unlock()
	if !ok || rate <= 0 {
		rate = defaultWriteThroughput
	}
	return time.Duration(float64(bytes) / rate * float64(time.Second))
}

// AddSettingsSample records a successful settings RPC to the specified host.
// The settings response is treated as a small read to feed the throughput
// metric.
func (p *Provider) AddSettingsSample(hostKey types.PublicKey, latency time.Duration) {
	p.AddReadSample(hostKey, settingsPayloadSize, latency)
}

// AddFailedRPC records a failed RPC attempt to the specified host.
func (p *Provider) AddFailedRPC(hostKey types.PublicKey) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.metric(hostKey).rpcFailRate.AddSample(false)
}

// TrackInflightRead increments the host's inflight read counter and
// returns a function that decrements it. Hold the returned function for
// the duration of the RPC so concurrent prioritization sees the load.
func (p *Provider) TrackInflightRead(hostKey types.PublicKey) func() {
	p.mu.Lock()
	m := p.metric(hostKey)
	p.mu.Unlock()
	m.inflightReads.Add(1)
	return func() { m.inflightReads.Add(-1) }
}

// TrackInflightWrite increments the host's inflight write counter and
// returns a function that decrements it. Prefer [Provider.PickWrite] for
// concurrent upload selection where the host comes from a shared pool;
// use this directly only when the host is already fixed.
func (p *Provider) TrackInflightWrite(hostKey types.PublicKey) func() {
	p.mu.Lock()
	m := p.metric(hostKey)
	p.mu.Unlock()
	m.inflightWrites.Add(1)
	return func() { m.inflightWrites.Add(-1) }
}

// PickWrite selects the highest-scoring host from candidates and
// atomically reserves an inflight write slot under the Provider lock,
// so concurrent pickers see each other's reservations and disperse
// across less-busy hosts instead of dogpiling the same top-N.
//
// The chosen host is swap-removed from candidates; the shortened slice
// is returned as `remaining`. Caller MUST defer `release`. Returns
// ok=false when candidates is empty.
func (p *Provider) PickWrite(candidates []types.PublicKey) (host types.PublicKey, release func(), remaining []types.PublicKey, ok bool) {
	if len(candidates) == 0 {
		return types.PublicKey{}, nil, candidates, false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	best := 0
	for i := 1; i < len(candidates); i++ {
		if p.cmpMetrics(candidates[i], candidates[best]) < 0 {
			best = i
		}
	}
	picked := candidates[best]
	m := p.metric(picked)
	m.inflightWrites.Add(1)
	candidates[best] = candidates[len(candidates)-1]
	return picked, func() { m.inflightWrites.Add(-1) }, candidates[:len(candidates)-1], true
}

// PickReads filters candidates for usable hosts, sorts them by score,
// and atomically reserves an inflight read slot on the top n — all
// under a single Provider lock so concurrent callers in a burst see
// each other's reservations and steer to less-busy hosts. Pass n=1 to
// atomically re-pick the best remaining candidate during a race loop.
//
// Returns the top n hosts as `picked` with matching `releases`, and
// the sorted-by-score tail as `remaining`. Callers MUST call all
// releases. When fewer than n usable hosts are available, returns
// picked=nil with no reservations made; the partial sorted list is
// still returned as `remaining`.
func (p *Provider) PickReads(candidates []types.PublicKey, n int) (picked []types.PublicKey, releases []func(), remaining []types.PublicKey) {
	sorted := candidates[:0]
	for _, host := range candidates {
		if u, err := p.store.Usable(host); err == nil && u {
			sorted = append(sorted, host)
		}
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	sort.SliceStable(sorted, func(i, j int) bool {
		return p.cmpMetrics(sorted[i], sorted[j]) < 0
	})
	if len(sorted) < n {
		return nil, nil, sorted
	}

	releases = make([]func(), n)
	for i := range n {
		m := p.metric(sorted[i])
		m.inflightReads.Add(1)
		releases[i] = func() { m.inflightReads.Add(-1) }
	}
	return sorted[:n], releases, sorted[n:]
}

// Addresses returns the network addresses for the specified host.
func (p *Provider) Addresses(hostKey types.PublicKey) ([]chain.NetAddress, error) {
	return p.store.Addresses(hostKey)
}

// HostQueue returns all usable hosts ordered by their historical
// performance.
func (p *Provider) HostQueue() (*HostQueue, error) {
	hosts, err := p.store.UsableHosts()
	if err != nil {
		return nil, err
	}
	hostKeys := make([]types.PublicKey, 0, len(hosts))
	for _, host := range hosts {
		hostKeys = append(hostKeys, host.PublicKey)
	}
	p.sortHosts(hostKeys)
	return &HostQueue{
		hosts:    hostKeys,
		attempts: make(map[types.PublicKey]int),
	}, nil
}

// UploadQueue returns hosts that are good for uploading, ordered
// by their historical performance.
func (p *Provider) UploadQueue() (*HostQueue, error) {
	hosts, err := p.store.UsableHosts()
	if err != nil {
		return nil, err
	}
	hostKeys := make([]types.PublicKey, 0, len(hosts))
	for _, host := range hosts {
		if host.GoodForUpload {
			hostKeys = append(hostKeys, host.PublicKey)
		}
	}
	p.sortHosts(hostKeys)
	return &HostQueue{
		hosts:    hostKeys,
		attempts: make(map[types.PublicKey]int),
	}, nil
}

// Prioritize reorders the given slice of hosts in place based
// on their historical performance. The reordered slice is returned with
// unusable hosts removed.
func (p *Provider) Prioritize(hosts []types.PublicKey) []types.PublicKey {
	filtered := hosts[:0]
	for _, host := range hosts {
		if ok, err := p.store.Usable(host); err != nil || !ok {
			continue
		}
		filtered = append(filtered, host)
	}
	p.sortHosts(filtered)
	return filtered
}

// UsableHosts returns all usable hosts in an arbitrary order.
func (p *Provider) UsableHosts() ([]hosts.HostInfo, error) {
	return p.store.UsableHosts()
}

// NewProvider creates a new Provider to track
// available hosts and their performance over time.
func NewProvider(store Store) *Provider {
	return &Provider{
		store:   store,
		metrics: make(map[types.PublicKey]*hostMetric),
	}
}
