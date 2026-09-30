package group

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/outbound"
	"github.com/sagernet/sing-box/common/urltest"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/service"
)

type membershipTestLeaf struct{ outbound.Adapter }

func newMembershipTestLeaf(tag string) *membershipTestLeaf {
	return &membershipTestLeaf{outbound.NewAdapter("direct", tag, []string{"tcp", "udp"}, nil)}
}

func (*membershipTestLeaf) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	return (&net.Dialer{}).DialContext(ctx, network, destination.String())
}

func (*membershipTestLeaf) ListenPacket(context.Context, M.Socksaddr) (net.PacketConn, error) {
	return net.ListenPacket("udp4", "127.0.0.1:0")
}

type membershipTestManager struct {
	adapter.OutboundManager
	members map[string]adapter.Outbound
}

func (manager *membershipTestManager) Outbound(tag string) (adapter.Outbound, bool) {
	member, found := manager.members[tag]
	return member, found
}

func membershipTestGroups(t *testing.T) (*Selector, *URLTest, []adapter.Outbound) {
	t.Helper()
	members := []adapter.Outbound{newMembershipTestLeaf("a"), newMembershipTestLeaf("b"), newMembershipTestLeaf("c")}
	manager := &membershipTestManager{members: map[string]adapter.Outbound{"a": members[0], "b": members[1], "c": members[2]}}
	ctx := service.ContextWith[adapter.OutboundManager](context.Background(), manager)
	history := urltest.NewHistoryStorage()
	service.MustRegisterPtr(ctx, history)
	for i, member := range members {
		history.StoreURLTestHistory(member.Tag(), &adapter.URLTestHistory{Time: time.Now(), Delay: uint16(i + 1)})
	}
	logger := log.NewNOPFactory().Logger()
	selectorOutbound, err := NewSelector(ctx, nil, logger, "proxy", option.SelectorOutboundOptions{Outbounds: []string{"a", "b"}, Default: "a"})
	if err != nil {
		t.Fatal(err)
	}
	selector := selectorOutbound.(*Selector)
	if err := selector.Start(); err != nil {
		t.Fatal(err)
	}
	probeOutbound, err := NewURLTest(ctx, nil, logger, "probe", option.URLTestOutboundOptions{Outbounds: []string{"a", "b"}})
	if err != nil {
		t.Fatal(err)
	}
	probe := probeOutbound.(*URLTest)
	if err := probe.Start(); err != nil {
		t.Fatal(err)
	}
	probe.PerformUpdateCheck()
	return selector, probe, members
}

func prepareMembershipTest(t *testing.T, selector *Selector, probe *URLTest, members []adapter.Outbound) *MembershipTransaction {
	t.Helper()
	transaction, err := PrepareMembership([]MembershipUpdate{{Group: selector, Members: members}, {Group: probe, Members: members}})
	if err != nil {
		t.Fatal(err)
	}
	return transaction
}

func TestMembershipCommitRollbackPreservesCurrentSelection(t *testing.T) {
	selector, probe, members := membershipTestGroups(t)
	transaction := prepareMembershipTest(t, selector, probe, members)
	// A choice made after preparation must survive commit, even though a new
	// member is being added and the configured default is a different leaf.
	if !selector.SelectOutbound("b") {
		t.Fatal("select b")
	}
	if err := transaction.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	if selector.Now() != "b" || !slices.Equal(selector.All(), []string{"a", "b", "c"}) || !slices.Equal(probe.Dependencies(), selector.All()) {
		t.Fatal("selection or membership was not preserved")
	}
	if !selector.SelectOutbound("c") {
		t.Fatal("native selector API cannot select the added leaf")
	}
	if err := transaction.Rollback(context.Background()); err != nil {
		t.Fatal(err)
	}
	if selector.Now() != "b" || selector.SelectOutbound("c") || !slices.Equal(probe.All(), []string{"a", "b"}) {
		t.Fatal("rollback did not restore prior native groups")
	}
}

func TestMembershipCancellationDoesNotPartiallyCommit(t *testing.T) {
	selector, probe, members := membershipTestGroups(t)
	transaction := prepareMembershipTest(t, selector, probe, members[:1])
	selector.membershipAccess.RLock()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := transaction.Commit(ctx)
	selector.membershipAccess.RUnlock()
	if !errors.Is(err, context.DeadlineExceeded) || !slices.Equal(probe.All(), []string{"a", "b"}) || !slices.Equal(selector.All(), probe.All()) {
		t.Fatalf("partial commit or unexpected error: %v", err)
	}
	if err := transaction.Rollback(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestMembershipRejectsStaleCommitAndRollback(t *testing.T) {
	selector, probe, members := membershipTestGroups(t)
	first := prepareMembershipTest(t, selector, probe, members[:1])
	stale := prepareMembershipTest(t, selector, probe, members)
	if err := first.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := stale.Commit(context.Background()); !errors.Is(err, ErrMembershipConflict) {
		t.Fatalf("stale commit: %v", err)
	}
	second := prepareMembershipTest(t, selector, probe, members)
	if err := second.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := first.Rollback(context.Background()); !errors.Is(err, ErrMembershipConflict) {
		t.Fatalf("stale rollback: %v", err)
	}
	if !slices.Equal(selector.All(), []string{"a", "b", "c"}) {
		t.Fatal("stale rollback overwrote a newer generation")
	}
	second.Release()
	if err := second.Rollback(context.Background()); !errors.Is(err, ErrMembershipFinished) {
		t.Fatal("released transaction remained reversible")
	}
}

func TestMembershipRejectsInvalidGroups(t *testing.T) {
	selector, probe, members := membershipTestGroups(t)
	for name, updates := range map[string][]MembershipUpdate{
		"empty":              nil,
		"nil group":          {{Members: members}},
		"typed nil selector": {{Group: (*Selector)(nil), Members: members}},
		"typed nil URLTest":  {{Group: (*URLTest)(nil), Members: members}},
		"empty members":      {{Group: selector}},
		"duplicate group":    {{Group: selector, Members: members}, {Group: selector, Members: members}},
		"duplicate leaf":     {{Group: selector, Members: []adapter.Outbound{members[0], members[0]}}},
		"nested group":       {{Group: selector, Members: []adapter.Outbound{members[0], probe}}},
		"missing default":    {{Group: selector, Members: members[1:]}},
		"bad fallback":       {{Group: selector, Members: members, Fallback: "missing"}},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := PrepareMembership(updates); !errors.Is(err, ErrMembershipInvalid) {
				t.Fatalf("unsafe membership accepted: %v", err)
			}
		})
	}
}

func TestMembershipDependenciesDoNotAcquireGroupLocks(t *testing.T) {
	selector, probe, members := membershipTestGroups(t)
	transaction := prepareMembershipTest(t, selector, probe, members[:1])
	if err := transaction.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	selector.membershipAccess.Lock()
	probe.group.updateAccess.Lock()
	done := make(chan bool, 1)
	go func() {
		done <- slices.Equal(selector.Dependencies(), []string{"a"}) && slices.Equal(probe.Dependencies(), []string{"a"})
	}()
	select {
	case valid := <-done:
		if !valid {
			t.Error("dependency snapshot is stale")
		}
	case <-time.After(time.Second):
		t.Error("dependency inspection acquired a group lock")
	}
	probe.group.updateAccess.Unlock()
	selector.membershipAccess.Unlock()
	if err := transaction.Rollback(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := transaction.Rollback(context.Background()); err != nil {
		t.Fatal("rollback retry is not idempotent")
	}
	if !slices.Equal(selector.Dependencies(), []string{"a", "b"}) || !slices.Equal(probe.Dependencies(), []string{"a", "b"}) {
		t.Fatal("rollback retained candidate dependencies")
	}
}

func TestMembershipReplacementRevalidatesNetworkSupport(t *testing.T) {
	_, probe, members := membershipTestGroups(t)
	// The preferred TCP/UDP leaf is replaced by a same-tag TCP-only leaf.
	if probe.group.selectedOutboundUDP != members[0] {
		t.Fatal("fixture did not select a")
	}
	replacement := &membershipTestLeaf{outbound.NewAdapter("direct", "a", []string{"tcp"}, nil)}
	transaction, err := PrepareMembership([]MembershipUpdate{{Group: probe, Members: []adapter.Outbound{replacement, members[1]}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := transaction.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	if probe.group.selectedOutboundTCP != replacement || probe.group.selectedOutboundUDP != members[1] {
		t.Fatal("replacement retained unsupported UDP selection")
	}
	probe.PerformUpdateCheck()
	if probe.group.selectedOutboundUDP != members[1] {
		t.Fatal("probe reselected unsupported UDP leaf")
	}
}

func TestMembershipURLTestUsesVerifiedFallbackWithoutPreemption(t *testing.T) {
	_, probe, members := membershipTestGroups(t)
	transaction, err := PrepareMembership([]MembershipUpdate{{Group: probe, Members: members[1:], FallbackTCP: "c", FallbackUDP: "c"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := transaction.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	if probe.group.selectedOutboundTCP != members[2] || probe.group.selectedOutboundUDP != members[2] {
		t.Fatal("stale history overrode the verified fallback")
	}
	transaction.Release()
	restore, err := PrepareMembership([]MembershipUpdate{{Group: probe, Members: members, FallbackTCP: "b", FallbackUDP: "b"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := restore.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	if probe.group.selectedOutboundTCP != members[2] || probe.group.selectedOutboundUDP != members[2] {
		t.Fatal("fallback preempted a retained selection")
	}
	if err := restore.Rollback(context.Background()); err != nil {
		t.Fatal(err)
	}
	if probe.group.selectedOutboundTCP != members[2] {
		t.Fatal("rollback lost previous URLTest selection")
	}
	tcpOnly := &membershipTestLeaf{outbound.NewAdapter("direct", "tcp-only", []string{"tcp"}, nil)}
	if _, err := PrepareMembership([]MembershipUpdate{{Group: probe, Members: []adapter.Outbound{tcpOnly, members[1]}, FallbackUDP: "tcp-only"}}); !errors.Is(err, ErrMembershipInvalid) {
		t.Fatal("unsupported UDP fallback was accepted")
	}
}

type membershipReentrantLeaf struct {
	*membershipTestLeaf
	check func()
}

func (leaf *membershipReentrantLeaf) DialContext(context.Context, string, M.Socksaddr) (net.Conn, error) {
	leaf.check()
	return nil, context.Canceled
}

func (leaf *membershipReentrantLeaf) ListenPacket(context.Context, M.Socksaddr) (net.PacketConn, error) {
	leaf.check()
	return nil, context.Canceled
}

func TestMembershipDialDoesNotHoldSelectionLocks(t *testing.T) {
	selector, probe, _ := membershipTestGroups(t)
	leaf := &membershipReentrantLeaf{membershipTestLeaf: newMembershipTestLeaf("a")}
	transaction := prepareMembershipTest(t, selector, probe, []adapter.Outbound{leaf})
	if err := transaction.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	for name, value := range map[string]adapter.Outbound{"selector": selector, "URLTest": probe} {
		t.Run(name, func(t *testing.T) {
			leaf.check = func() {
				if !selector.membershipAccess.TryLock() {
					t.Fatal("selector holds its lock during network I/O")
				}
				selector.membershipAccess.Unlock()
				if !probe.group.updateAccess.TryLock() {
					t.Fatal("URLTest holds its lock during network I/O")
				}
				probe.group.updateAccess.Unlock()
				// DNS detours can inspect/re-enter the same group during a dial.
				_ = selector.Now()
				_ = probe.Now()
			}
			_, _ = value.DialContext(context.Background(), "tcp", M.Socksaddr{})
			_, _ = value.ListenPacket(context.Background(), M.Socksaddr{})
		})
	}
}

func TestMembershipTransactionGuardHonorsCallerDeadline(t *testing.T) {
	selector, probe, members := membershipTestGroups(t)
	transaction := prepareMembershipTest(t, selector, probe, members[:1])
	for name, operation := range map[string]func(context.Context) error{"commit": transaction.Commit, "rollback": transaction.Rollback} {
		t.Run(name, func(t *testing.T) {
			transaction.access.Lock()
			defer transaction.access.Unlock()
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- operation(ctx) }()
			select {
			case err := <-done:
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("unexpected error: %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("transaction guard ignored caller deadline")
			}
		})
	}
}

type membershipProbeLeaseLeaf struct {
	*membershipReentrantLeaf
	acquired atomic.Int32
	released chan struct{}
}

type membershipMultiplexLeaf struct{ *membershipTestLeaf }

func (leaf *membershipMultiplexLeaf) AcquireDialer() (N.Dialer, func(), error) {
	return leaf, func() {}, nil
}

func (*membershipMultiplexLeaf) MultiplexEnabled() bool { return true }

func TestMembershipLeasedProbePreservesMultiplexWarmup(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	leaf := &membershipMultiplexLeaf{newMembershipTestLeaf("a")}
	members, release, err := leaseMembershipOutbounds([]adapter.Outbound{leaf})
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if _, err := urltest.URLTest(context.Background(), server.URL, members[0]); err != nil {
		t.Fatal(err)
	}
	if requests.Load() != 2 {
		t.Fatal("leased probe skipped multiplex warm-up")
	}
}

func (leaf *membershipProbeLeaseLeaf) AcquireDialer() (N.Dialer, func(), error) {
	leaf.acquired.Add(1)
	return leaf, func() { close(leaf.released) }, nil
}

func TestMembershipProbeSnapshotLeaseSurvivesBatchCancellation(t *testing.T) {
	for _, entry := range []string{"native", "selector", "nested-selector", "raw-leaf"} {
		t.Run(entry, func(t *testing.T) { testMembershipProbeLease(t, entry) })
	}
}

func testMembershipProbeLease(t *testing.T, entry string) {
	entered, resume := make(chan struct{}), make(chan struct{})
	leaf := &membershipProbeLeaseLeaf{
		membershipReentrantLeaf: &membershipReentrantLeaf{membershipTestLeaf: newMembershipTestLeaf("a"), check: func() { close(entered); <-resume }},
		released:                make(chan struct{}),
	}
	manager := &membershipTestManager{members: map[string]adapter.Outbound{"a": leaf}}
	logger := log.NewNOPFactory().Logger()
	members := []adapter.Outbound{leaf}
	var release func()
	if entry == "native" {
		var err error
		members, release, err = leaseMembershipOutbounds(members)
		if err != nil {
			t.Fatal(err)
		}
	} else if entry != "raw-leaf" {
		count := 1
		if entry == "nested-selector" {
			count = 2
		}
		for index := range count {
			ctx := service.ContextWith[adapter.OutboundManager](context.Background(), manager)
			tag := fmt.Sprintf("selector-%d", index)
			value, err := NewSelector(ctx, nil, logger, tag, option.SelectorOutboundOptions{Outbounds: []string{members[0].Tag()}})
			if err != nil {
				t.Fatal(err)
			}
			if err := value.(*Selector).Start(); err != nil {
				t.Fatal(err)
			}
			manager.members[tag] = value
			members = []adapter.Outbound{value}
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		if entry == "native" {
			urlTestOutbounds(ctx, manager, urltest.NewHistoryStorage(), logger, members, "http://probe.invalid", time.Minute, true, release)
		} else {
			URLTestOutbounds(ctx, manager, urltest.NewHistoryStorage(), logger, members, "http://probe.invalid", time.Minute, true)
		}
		close(done)
	}()
	<-entered
	if leaf.acquired.Load() != 1 {
		t.Fatal("probe did not acquire exactly one lease")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("batch did not honor cancellation")
	}
	select {
	case <-leaf.released:
		t.Fatal("snapshot released under an unfinished probe")
	default:
	}
	close(resume)
	select {
	case <-leaf.released:
	case <-time.After(time.Second):
		t.Fatal("finished probe leaked its lease")
	}
}

func TestMembershipRetainsEstablishedTCPAndUDP(t *testing.T) {
	selector, probe, members := membershipTestGroups(t)
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		connection, err := listener.Accept()
		if err == nil {
			defer connection.Close()
			_, _ = io.Copy(connection, connection)
		}
	}()
	selector.SelectOutbound("b")
	connection, err := selector.DialContext(context.Background(), "tcp", M.ParseSocksaddr(listener.Addr().String()))
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	_ = connection.SetDeadline(time.Now().Add(5 * time.Second))
	peer, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	_ = peer.SetDeadline(time.Now().Add(5 * time.Second))
	packet, err := selector.ListenPacket(context.Background(), M.ParseSocksaddr(peer.LocalAddr().String()))
	if err != nil {
		t.Fatal(err)
	}
	defer packet.Close()
	_ = packet.SetDeadline(time.Now().Add(5 * time.Second))
	check := func() {
		t.Helper()
		if _, err := connection.Write([]byte("alive")); err != nil {
			t.Fatal(err)
		}
		data := make([]byte, 5)
		if _, err := io.ReadFull(connection, data); err != nil || string(data) != "alive" {
			t.Fatalf("TCP stream interrupted: %v", err)
		}
		if _, err := packet.WriteTo([]byte("alive"), peer.LocalAddr()); err != nil {
			t.Fatal(err)
		}
		if _, _, err := peer.ReadFrom(data); err != nil || string(data) != "alive" {
			t.Fatalf("UDP session interrupted: %v", err)
		}
	}
	check()
	transaction := prepareMembershipTest(t, selector, probe, members[:1])
	if err := transaction.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	check()
	if selector.Now() != "a" || selector.SelectOutbound("b") {
		t.Fatal("removed leaf remains selectable")
	}
	if err := transaction.Rollback(context.Background()); err != nil {
		t.Fatal(err)
	}
	check()
}

func TestMembershipConcurrentObserversAndPolicySelection(t *testing.T) {
	selector, probe, members := membershipTestGroups(t)
	ctx, cancel := context.WithCancel(context.Background())
	var workers sync.WaitGroup
	for range 4 {
		workers.Go(func() {
			for ctx.Err() == nil {
				_ = selector.Now()
				_ = selector.All()
				_ = selector.Network()
				selector.SelectOutbound("a")
				_ = probe.Now()
				_ = probe.All()
				probe.PerformUpdateCheck()
			}
		})
	}
	t.Cleanup(func() { cancel(); workers.Wait() })
	for range 100 {
		transaction := prepareMembershipTest(t, selector, probe, members[:1])
		deadline, done := context.WithTimeout(context.Background(), time.Second)
		if err := transaction.Commit(deadline); err != nil {
			done()
			t.Fatal(err)
		}
		if err := transaction.Rollback(deadline); err != nil {
			done()
			t.Fatal(err)
		}
		done()
	}
}
