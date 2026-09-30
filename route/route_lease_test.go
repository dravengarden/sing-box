package route

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/outbound"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

type routeLeaseLeaf struct{ outbound.Adapter }

func (*routeLeaseLeaf) DialContext(context.Context, string, M.Socksaddr) (net.Conn, error) {
	return nil, errors.New("unused")
}

func (*routeLeaseLeaf) ListenPacket(context.Context, M.Socksaddr) (net.PacketConn, error) {
	return nil, errors.New("unused")
}

type routeLeaseGroup struct {
	routeLeaseLeaf
	member   adapter.Outbound
	err      error
	released int
}

func (group *routeLeaseGroup) All() []string                     { return []string{group.member.Tag()} }
func (group *routeLeaseGroup) Selected(string) adapter.Outbound  { return group.member }
func (group *routeLeaseGroup) AttachConnection(io.Closer) func() { return func() {} }
func (group *routeLeaseGroup) release()                          { group.released++ }
func (group *routeLeaseGroup) SelectedWithDialerLease(string) (adapter.Outbound, func(), error) {
	if group.err != nil {
		return nil, nil, group.err
	}
	return group.member, group.release, nil
}

func newRouteLeaseGroup(tag string, member adapter.Outbound) *routeLeaseGroup {
	return &routeLeaseGroup{routeLeaseLeaf: routeLeaseLeaf{outbound.NewAdapter("selector", tag, []string{N.NetworkTCP, N.NetworkUDP}, nil)}, member: member}
}

func TestResolveRoutedOutboundReleasesEveryLease(t *testing.T) {
	leaf := &routeLeaseLeaf{outbound.NewAdapter("vless", "leaf", []string{N.NetworkTCP}, nil)}
	inner := newRouteLeaseGroup("inner", leaf)
	outer := newRouteLeaseGroup("outer", inner)

	chain, release, err := resolveRoutedOutbound(outer, N.NetworkTCP)
	if err != nil || len(chain) != 3 || chain[2] != leaf {
		t.Fatalf("resolved %v: %v", chain, err)
	}
	if outer.released != 0 || inner.released != 0 {
		t.Fatal("lease released before the connection closed")
	}
	release()
	if outer.released != 1 || inner.released != 1 {
		t.Fatal("connection close did not release every lease")
	}

	// A member that cannot carry the network fails after selection; the
	// leases taken on the way must not leak.
	if _, _, err := resolveRoutedOutbound(outer, N.NetworkUDP); err == nil {
		t.Fatal("UDP resolved to a TCP-only member")
	}
	if outer.released != 2 || inner.released != 2 {
		t.Fatal("failed resolution leaked a lease")
	}

	inner.err = errors.New("retired")
	if _, _, err := resolveRoutedOutbound(outer, N.NetworkTCP); err == nil {
		t.Fatal("lease failure ignored")
	}
	if outer.released != 3 {
		t.Fatal("lease failure leaked the outer lease")
	}
}
