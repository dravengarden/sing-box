package outbound

import (
	"errors"
	"testing"

	"github.com/sagernet/sing-box/adapter"
)

type detachTestOutbound struct {
	adapter.Outbound
	tag          string
	dependencies []string
	closed       int
}

func (outbound *detachTestOutbound) Tag() string            { return outbound.tag }
func (outbound *detachTestOutbound) Dependencies() []string { return outbound.dependencies }
func (outbound *detachTestOutbound) Close() error {
	outbound.closed++
	return nil
}

type detachTestEndpoints struct{ adapter.EndpointManager }

func (detachTestEndpoints) Endpoints() []adapter.Endpoint { return nil }

func TestDetachValidatesLiveDependenciesBeforeMutation(t *testing.T) {
	leaf := &detachTestOutbound{tag: "leaf"}
	group := &detachTestOutbound{tag: "group", dependencies: []string{"leaf"}}
	manager := NewManager(nil, nil, detachTestEndpoints{}, "group")
	manager.defaultOutbound = group
	manager.outbounds = []adapter.Outbound{leaf, group}
	manager.outboundByTag["leaf"], manager.outboundByTag["group"] = leaf, group
	// An intentionally stale index must not authorize detachment.
	manager.dependByTag = map[string][]string{}
	if _, err := manager.Detach("leaf"); !errors.Is(err, ErrOutboundReferenced) {
		t.Fatalf("referenced leaf detached: %v", err)
	}
	if found, ok := manager.Outbound("leaf"); !ok || found != leaf || len(manager.Outbounds()) != 2 || leaf.closed != 0 {
		t.Fatal("rejected operation changed manager state")
	}
	if _, err := manager.Detach("group"); !errors.Is(err, ErrOutboundReferenced) {
		t.Fatal("default outbound detached")
	}
	group.dependencies = nil
	manager.dependByTag["leaf"] = []string{"group"}
	oldInventory := manager.Outbounds()
	value, err := manager.Detach("leaf")
	if err != nil || value != leaf || leaf.closed != 0 {
		t.Fatalf("detach did not transfer ownership: %v", err)
	}
	if len(manager.Outbounds()) != 1 || manager.Outbounds()[0] != group || oldInventory[0] != leaf || oldInventory[1] != group {
		t.Fatal("detachment changed an earlier inventory snapshot")
	}
	if len(manager.dependByTag) != 0 {
		t.Fatal("stale dependency index survived")
	}
}
