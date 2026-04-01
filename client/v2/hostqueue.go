package client

import (
	"cmp"
	"errors"
	"iter"
	"math"
	"sort"
	"sync"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/indexd/hosts"
)

const emaAlpha = 0.2

// ErrAbortedRPC is a special error that can be used as the cancel for an RPC
// context to indicate that the RPC was interrupted and that the corresponding
// host should not have a failure recorded for that RPC.
var ErrAbortedRPC = errors.New("aborted RPC")

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

// AddSample adds a new latency sample to the average.
func (ra *rpcAverage) AddSample(latency time.Duration) {
	sample := float64(latency.Milliseconds())
	if !ra.init {
		ra.value = sample
		ra.init = true
	} else {
		ra.value = emaAlpha*sample + (1.0-emaAlpha)*ra.value
	}
}

// Value returns the current average latency in milliseconds.
func (ra *rpcAverage) Value() float64 {
	return ra.value
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
}

func (p *Provider) sortHosts(hosts []types.PublicKey) {
	p.mu.Lock()
	defer p.mu.Unlock()
	sort.SliceStable(hosts, func(i, j int) bool {
		return p.cmpMetrics(hosts[i], hosts[j]) < 0
	})
}

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
	fc := cmp.Compare(am.rpcFailRate.Value(), bm.rpcFailRate.Value())
	if fc != 0 {
		return fc
	}
	al := (am.rpcReadAverage.Value() + am.rpcWriteAverage.Value()) / 2
	bl := (bm.rpcReadAverage.Value() + bm.rpcWriteAverage.Value()) / 2
	return cmp.Compare(al, bl)
}

// AddReadSample records a successful read RPC attempt to the specified host.
func (p *Provider) AddReadSample(hostKey types.PublicKey, latency time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	metric, exists := p.metrics[hostKey]
	if !exists {
		metric = &hostMetric{}
		p.metrics[hostKey] = metric
	}
	metric.rpcReadAverage.AddSample(latency)
	metric.rpcFailRate.AddSample(true)
}

// AddWriteSample records a successful write RPC attempt to the specified host.
func (p *Provider) AddWriteSample(hostKey types.PublicKey, latency time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	metric, exists := p.metrics[hostKey]
	if !exists {
		metric = &hostMetric{}
		p.metrics[hostKey] = metric
	}
	metric.rpcWriteAverage.AddSample(latency)
	metric.rpcFailRate.AddSample(true)
}

// AddFailedRPC records a failed RPC attempt to the specified host.
func (p *Provider) AddFailedRPC(hostKey types.PublicKey, err error) {
	if errors.Is(err, ErrAbortedRPC) || errors.Is(err, proto.ErrSectorNotFound) {
		return // do not record aborted RPCs or missing sectors as failures
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	metric, exists := p.metrics[hostKey]
	if !exists {
		metric = &hostMetric{}
		p.metrics[hostKey] = metric
	}
	metric.rpcFailRate.AddSample(false)
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

// NewProvider creates a new Provider to track
// available hosts and their performance over time.
func NewProvider(store Store) *Provider {
	return &Provider{
		store:   store,
		metrics: make(map[types.PublicKey]*hostMetric),
	}
}
