package client

import (
	"cmp"
	"iter"
	"math"
	"sort"
	"sync"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/indexd/hosts"
)

const emaAlpha = 0.2

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

// Candidates manages a queue of hosts to use for uploading. It is
// safe for concurrent use.
type Candidates struct {
	mu    sync.Mutex
	hosts []types.PublicKey
}

// Available returns the number of remaining hosts.
//
// It is safe for concurrent use.
func (c *Candidates) Available() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.hosts)
}

// Iter returns an iterator that yields hosts one at a
// time until there are no hosts left.
//
// It is safe for concurrent use.
func (c *Candidates) Iter() iter.Seq[types.PublicKey] {
	return func(yield func(types.PublicKey) bool) {
		for {
			host, ok := c.Next()
			if !ok || !yield(host) {
				return
			}
		}
	}
}

// Next returns the next host candidate. If there are no more hosts, ok is false.
//
// It is safe for concurrent use.
func (c *Candidates) Next() (types.PublicKey, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.hosts) == 0 {
		return types.PublicKey{}, false
	}
	host := c.hosts[0]
	c.hosts = c.hosts[1:]
	return host, true
}

// Retry adds the host back to the queue for retrying later.
func (c *Candidates) Retry(host types.PublicKey) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.hosts = append(c.hosts, host)
}

// NewCandidates creates a new Candidates with the provided hosts.
func NewCandidates(hosts []types.PublicKey) *Candidates {
	return &Candidates{
		hosts: hosts,
	}
}

// A Provider tracks available hosts and their performance over time.
type Provider struct {
	store Store

	mu      sync.Mutex // protects the fields below
	metrics map[types.PublicKey]*hostMetric
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
func (p *Provider) AddFailedRPC(hostKey types.PublicKey) {
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

// Candidates returns all host candidates ordered by their
// historical performance.
func (p *Provider) Candidates() (*Candidates, error) {
	hosts, err := p.store.UsableHosts()
	if err != nil {
		return nil, err
	}
	hostKeys := make([]types.PublicKey, 0, len(hosts))
	for _, host := range hosts {
		hostKeys = append(hostKeys, host.PublicKey)
	}
	p.sortHosts(hostKeys)
	return &Candidates{
		hosts: hostKeys,
	}, nil
}

func (p *Provider) sortHosts(hosts []types.PublicKey) {
	sort.SliceStable(hosts, func(i, j int) bool {
		return p.cmpMetrics(hosts[i], hosts[j]) < 0
	})
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
