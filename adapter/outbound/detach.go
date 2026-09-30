package outbound

import (
	"errors"
	"os"
	"slices"

	"github.com/sagernet/sing-box/adapter"
)

var ErrOutboundReferenced = errors.New("outbound is still referenced")

// Detach removes an unreferenced, non-default outbound from lookup without
// closing it. The caller owns draining and closure after success. Unlike Remove,
// this validates the live dependency graph before modifying any manager state;
// group membership may have changed since the original reverse index was built.
// Callers must independently exclude cached route, DNS, and service references;
// those references are outside the outbound/endpoint dependency graph.
// Callers must serialize topology mutations (including group membership) with
// this operation. Concurrent manager inventory changes are rejected.
func (m *Manager) Detach(tag string) (adapter.Outbound, error) {
	m.access.RLock()
	detached, found := m.outboundByTag[tag]
	inventory := slices.Clone(m.outbounds)
	defaultOutbound := m.defaultOutbound
	m.access.RUnlock()
	if !found {
		return nil, os.ErrNotExist
	}
	if defaultOutbound == detached {
		return nil, ErrOutboundReferenced
	}
	// Never call group methods while holding the manager lock: a group dial
	// may hold its membership lock while resolving another outbound.
	dependByTag := make(map[string][]string)
	for _, outbound := range inventory {
		if outbound == detached {
			continue
		}
		dependencies := outbound.Dependencies()
		if slices.Contains(dependencies, tag) {
			return nil, ErrOutboundReferenced
		}
		for _, dependency := range dependencies {
			dependByTag[dependency] = append(dependByTag[dependency], outbound.Tag())
		}
	}
	for _, endpoint := range m.endpoint.Endpoints() {
		if slices.Contains(endpoint.Dependencies(), tag) {
			return nil, ErrOutboundReferenced
		}
	}
	m.access.Lock()
	defer m.access.Unlock()
	if !slices.Equal(inventory, m.outbounds) || m.outboundByTag[tag] != detached || m.defaultOutbound != defaultOutbound {
		return nil, errors.New("outbound inventory changed during detach")
	}
	index := slices.Index(m.outbounds, detached)
	if index < 0 {
		return nil, os.ErrInvalid
	}
	// Outbounds() hands readers the old slice. Publish a new backing array so
	// an in-flight inventory traversal does not observe mutation or nil slots.
	m.outbounds = slices.Delete(slices.Clone(m.outbounds), index, index+1)
	delete(m.outboundByTag, tag)
	m.dependByTag = dependByTag
	return detached, nil
}
