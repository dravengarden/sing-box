package adapter

import (
	"context"
	"net/netip"

	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing-tun"
	N "github.com/sagernet/sing/common/network"
)

// Note: for proxy protocols, outbound creates early connections by default.

type Outbound interface {
	Type() string
	Tag() string
	Network() []string
	Dependencies() []string
	N.Dialer
}

type OutboundWithPreferredRoutes interface {
	Outbound
	PreferredDomain(metadata *InboundContext, domain string) bool
	PreferredAddress(metadata *InboundContext, address netip.Addr) bool
}

type OutboundWithMultiplex interface {
	Outbound
	MultiplexEnabled() bool
}

// OutboundWithDialerLease pins a leaf across group selection and a dial. The
// acquisition must not perform network I/O or call group/manager methods.
// Release is called after DialContext/ListenPacket returns; implementations
// separately retain successful flows until their connections close. A lease
// permits the in-flight dial even if the leaf is concurrently retired.
type OutboundWithDialerLease interface {
	Outbound
	AcquireDialer() (N.Dialer, func(), error)
}

// OutboundGroupWithDialerLease resolves a group's selected member for a
// routed connection. When the member implements OutboundWithDialerLease, the
// lease is acquired while the group's membership lock is held and the
// returned outbound dials through it; release is idempotent and must be called
// when the routed connection closes. Other members are returned unwrapped.
type OutboundGroupWithDialerLease interface {
	OutboundGroup
	SelectedWithDialerLease(network string) (Outbound, func(), error)
}

type FlowOutbound interface {
	Outbound
	tun.Port
	PreMatchFlow(network string, destination netip.Addr) PreMatchAction
}

type OutboundRegistry interface {
	option.OutboundOptionsRegistry
	CreateOutbound(ctx context.Context, router Router, logger log.ContextLogger, tag string, outboundType string, options any) (Outbound, error)
}

type OutboundManager interface {
	Lifecycle
	Outbounds() []Outbound
	Outbound(tag string) (Outbound, bool)
	Default() Outbound
	Create(ctx context.Context, router Router, logger log.ContextLogger, tag string, outboundType string, options any) error
}

type IdleConnectionKeeper interface {
	SetKeepIdleConnections(keep bool)
	CloseIdleConnections()
}

type Referrer interface {
	References() []string
}
