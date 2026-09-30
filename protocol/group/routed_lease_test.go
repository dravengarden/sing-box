package group

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/urltest"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/service"
)

var errRoutedLeaseRetired = errors.New("retired")

// routedLeaseLeaf follows the lease contract of a managed provider leaf: a
// retired leaf refuses direct work, while an already acquired lease still
// carries its dials.
type routedLeaseLeaf struct {
	*membershipTestLeaf
	access  sync.Mutex
	retired bool
	leases  int
	leased  int
}

func (leaf *routedLeaseLeaf) retire() {
	leaf.access.Lock()
	leaf.retired = true
	leaf.access.Unlock()
}

func (leaf *routedLeaseLeaf) counts() (leases, leased int) {
	leaf.access.Lock()
	defer leaf.access.Unlock()
	return leaf.leases, leaf.leased
}

func (leaf *routedLeaseLeaf) DialContext(context.Context, string, M.Socksaddr) (net.Conn, error) {
	leaf.access.Lock()
	defer leaf.access.Unlock()
	if leaf.retired {
		return nil, errRoutedLeaseRetired
	}
	client, server := net.Pipe()
	_ = server.Close()
	return client, nil
}

func (leaf *routedLeaseLeaf) AcquireDialer() (N.Dialer, func(), error) {
	leaf.access.Lock()
	defer leaf.access.Unlock()
	if leaf.retired {
		return nil, nil, errRoutedLeaseRetired
	}
	leaf.leases++
	var once sync.Once
	return routedLeaseDialer{leaf}, func() {
		once.Do(func() {
			leaf.access.Lock()
			leaf.leases--
			leaf.access.Unlock()
		})
	}, nil
}

type routedLeaseDialer struct{ leaf *routedLeaseLeaf }

func (dialer routedLeaseDialer) DialContext(context.Context, string, M.Socksaddr) (net.Conn, error) {
	dialer.leaf.access.Lock()
	dialer.leaf.leased++
	dialer.leaf.access.Unlock()
	client, server := net.Pipe()
	_ = server.Close()
	return client, nil
}

func (dialer routedLeaseDialer) ListenPacket(context.Context, M.Socksaddr) (net.PacketConn, error) {
	dialer.leaf.access.Lock()
	dialer.leaf.leased++
	dialer.leaf.access.Unlock()
	return net.ListenPacket("udp4", "127.0.0.1:0")
}

func routedLeaseGroups(t *testing.T, members ...adapter.Outbound) (*Selector, *URLTest) {
	t.Helper()
	manager := &membershipTestManager{members: make(map[string]adapter.Outbound)}
	var tags []string
	for _, member := range members {
		manager.members[member.Tag()] = member
		tags = append(tags, member.Tag())
	}
	ctx := service.ContextWith[adapter.OutboundManager](context.Background(), manager)
	history := urltest.NewHistoryStorage()
	service.MustRegisterPtr(ctx, history)
	for i, member := range members {
		history.StoreURLTestHistory(member.Tag(), &adapter.URLTestHistory{Time: time.Now(), Delay: uint16(i + 1)})
	}
	logger := log.NewNOPFactory().Logger()
	selectorOutbound, err := NewSelector(ctx, nil, logger, "proxy", option.SelectorOutboundOptions{Outbounds: tags})
	if err != nil {
		t.Fatal(err)
	}
	selector := selectorOutbound.(*Selector)
	if err := selector.Start(adapter.StartStateStart, adapter.NewScope(ctx, logger)); err != nil {
		t.Fatal(err)
	}
	probeOutbound, err := NewURLTest(ctx, nil, logger, "probe", option.URLTestOutboundOptions{Outbounds: tags})
	if err != nil {
		t.Fatal(err)
	}
	probe := probeOutbound.(*URLTest)
	if err := probe.Start(adapter.StartStateStart, adapter.NewScope(ctx, logger)); err != nil {
		t.Fatal(err)
	}
	probe.PerformUpdateCheck()
	return selector, probe
}

// Routing resolves a group to its member and dials the member directly. The
// lease taken under the membership lock must carry every dial attempt of that
// routed connection across a commit that removes and retires the member, while
// new routing observes the committed membership.
func TestRoutedLeaseSurvivesMembershipRemoval(t *testing.T) {
	for _, name := range []string{"selector", "URLTest"} {
		t.Run(name, func(t *testing.T) {
			a := &routedLeaseLeaf{membershipTestLeaf: newMembershipTestLeaf("a")}
			b := &routedLeaseLeaf{membershipTestLeaf: newMembershipTestLeaf("b")}
			selector, probe := routedLeaseGroups(t, a, b)
			var group adapter.OutboundGroupWithDialerLease = selector
			update := MembershipUpdate{Group: selector, Members: []adapter.Outbound{b}}
			if name == "URLTest" {
				group = probe
				update = MembershipUpdate{Group: probe, Members: []adapter.Outbound{b}}
			}
			routed, release, err := group.SelectedWithDialerLease(N.NetworkTCP)
			if err != nil || routed.Tag() != "a" {
				t.Fatalf("lease selected %v: %v", routed, err)
			}
			if leases, _ := a.counts(); leases != 1 {
				t.Fatalf("lease count %d", leases)
			}
			transaction, err := PrepareMembership([]MembershipUpdate{update})
			if err != nil {
				t.Fatal(err)
			}
			if err := transaction.Commit(context.Background()); err != nil {
				t.Fatal(err)
			}
			transaction.Release()
			a.retire()
			if _, err := a.DialContext(context.Background(), N.NetworkTCP, M.Socksaddr{}); !errors.Is(err, errRoutedLeaseRetired) {
				t.Fatal("retired member accepted a direct dial")
			}
			// Serial address fallback dials the routed member more than once.
			for range 2 {
				conn, err := routed.DialContext(context.Background(), N.NetworkTCP, M.Socksaddr{})
				if err != nil {
					t.Fatalf("leased dial after retirement: %v", err)
				}
				_ = conn.Close()
			}
			if _, leased := a.counts(); leased != 2 {
				t.Fatalf("leased dials %d", leased)
			}
			release()
			release()
			if leases, _ := a.counts(); leases != 0 {
				t.Fatalf("release left %d leases", leases)
			}
			next, nextRelease, err := group.SelectedWithDialerLease(N.NetworkTCP)
			if err != nil || next.Tag() != "b" {
				t.Fatalf("committed selection %v: %v", next, err)
			}
			nextRelease()
			if selected := group.Selected(N.NetworkTCP); selected != b {
				t.Fatalf("Selected returned %v", selected)
			}
		})
	}
}

func TestRoutedLeaseRefusesRetiredSelection(t *testing.T) {
	a := &routedLeaseLeaf{membershipTestLeaf: newMembershipTestLeaf("a")}
	selector, probe := routedLeaseGroups(t, a)
	a.retire()
	for _, group := range []adapter.OutboundGroupWithDialerLease{selector, probe} {
		if _, _, err := group.SelectedWithDialerLease(N.NetworkTCP); !errors.Is(err, errRoutedLeaseRetired) {
			t.Fatalf("%s leased a retired member: %v", group.Tag(), err)
		}
	}
}

func TestRoutedLeaseKeepsUnleasedMembersNative(t *testing.T) {
	a := newMembershipTestLeaf("a")
	selector, probe := routedLeaseGroups(t, a)
	for _, group := range []adapter.OutboundGroupWithDialerLease{selector, probe} {
		member, release, err := group.SelectedWithDialerLease(N.NetworkUDP)
		if err != nil || member != a || release == nil {
			t.Fatalf("%s wrapped a member without lease support: %v", group.Tag(), err)
		}
		release()
	}
}
