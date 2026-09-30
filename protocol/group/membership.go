package group

import (
	"context"
	"errors"
	"net"
	"slices"
	"sync"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing/common"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

var (
	ErrMembershipInvalid  = errors.New("invalid flat group membership update")
	ErrMembershipConflict = errors.New("group membership changed since preparation")
	ErrMembershipFinished = errors.New("group membership transaction is finished")
)

// MembershipUpdate changes only a started native group's flat member list.
// The caller owns preparation, health checks, manager lookup registration, and
// eventual draining/closure of leaf outbounds. This API never closes leaves or
// interrupts established connections. Existing group objects retain their
// concrete type, so cached DNS detours and Clash API selectors remain valid.
type MembershipUpdate struct {
	Group   adapter.OutboundGroup
	Members []adapter.Outbound
	// Fallback is used only if a selector's current member is removed. Empty
	// selects its configured default (or first member when no default exists).
	Fallback string
	// URLTest fallbacks are independently checked for network support and used
	// only when the previous selection is removed or no longer supports it.
	FallbackTCP string
	FallbackUDP string
}

type membershipChange struct {
	tag          string
	selector     *Selector
	urltest      *URLTestGroup
	access       *sync.RWMutex
	revision     *uint64
	expected     uint64
	members      []adapter.Outbound
	byTag        map[string]adapter.Outbound
	tags         []string
	fallback     string
	fallbackTCP  string
	fallbackUDP  string
	dependencies *common.TypedValue[[]string]

	oldMembers []adapter.Outbound
	oldByTag   map[string]adapter.Outbound
	oldTags    []string
	oldTCP     adapter.Outbound
	oldUDP     adapter.Outbound
}

// MembershipTransaction acquires every affected group before publishing any
// state. Failed preparation/lock acquisition changes nothing. A successful
// commit keeps previous references available for rollback until Release.
// Use a bounded context. Network operations never hold membership locks;
// OutboundWithDialerLease lets the caller pin a selected leaf before unlocking.
type MembershipTransaction struct {
	access     sync.Mutex
	changes    []*membershipChange
	committed  bool
	finished   bool
	rolledBack bool
}

func PrepareMembership(updates []MembershipUpdate) (*MembershipTransaction, error) {
	if len(updates) == 0 {
		return nil, ErrMembershipInvalid
	}
	transaction := &MembershipTransaction{}
	seen := make(map[string]bool)
	for _, update := range updates {
		switch value := update.Group.(type) {
		case *Selector:
			if value == nil {
				return nil, ErrMembershipInvalid
			}
		case *URLTest:
			if value == nil {
				return nil, ErrMembershipInvalid
			}
		default:
			return nil, ErrMembershipInvalid
		}
		if update.Group == nil || update.Group.Tag() == "" || seen[update.Group.Tag()] || len(update.Members) == 0 {
			return nil, ErrMembershipInvalid
		}
		seen[update.Group.Tag()] = true
		change := &membershipChange{tag: update.Group.Tag(), members: slices.Clone(update.Members), byTag: make(map[string]adapter.Outbound), fallback: update.Fallback, fallbackTCP: update.FallbackTCP, fallbackUDP: update.FallbackUDP}
		for _, member := range change.members {
			if member == nil || member.Tag() == "" || change.byTag[member.Tag()] != nil {
				return nil, ErrMembershipInvalid
			}
			if _, nested := member.(adapter.OutboundGroup); nested {
				return nil, ErrMembershipInvalid
			}
			change.byTag[member.Tag()] = member
			change.tags = append(change.tags, member.Tag())
		}
		switch group := update.Group.(type) {
		case *Selector:
			if update.FallbackTCP != "" || update.FallbackUDP != "" {
				return nil, ErrMembershipInvalid
			}
			change.selector, change.access, change.revision = group, &group.membershipAccess, &group.membershipRevision
			change.dependencies = &group.dependencyTags
		case *URLTest:
			if group.group == nil || update.Fallback != "" {
				return nil, ErrMembershipInvalid
			}
			change.urltest, change.access, change.revision = group.group, &group.group.updateAccess, &group.group.membershipRevision
			change.dependencies = &group.dependencyTags
		default:
			return nil, ErrMembershipInvalid
		}
		change.access.RLock()
		valid := change.validateLocked()
		change.expected = *change.revision
		change.access.RUnlock()
		if !valid {
			return nil, ErrMembershipInvalid
		}
		transaction.changes = append(transaction.changes, change)
	}
	slices.SortFunc(transaction.changes, func(a, b *membershipChange) int {
		if a.tag < b.tag {
			return -1
		}
		return 1
	})
	return transaction, nil
}

func (change *membershipChange) validateLocked() bool {
	var previous []adapter.Outbound
	if selector := change.selector; selector != nil {
		if selector.selected.Load() == nil || len(selector.outbounds) == 0 {
			return false
		}
		for _, member := range selector.outbounds {
			previous = append(previous, member)
		}
		if selector.defaultTag != "" && change.byTag[selector.defaultTag] == nil {
			return false
		}
		if change.fallback == "" {
			change.fallback = selector.defaultTag
			if change.fallback == "" {
				change.fallback = change.tags[0]
			}
		}
		if change.byTag[change.fallback] == nil {
			return false
		}
	} else {
		previous = change.urltest.outbounds
		for network, tag := range map[string]string{N.NetworkTCP: change.fallbackTCP, N.NetworkUDP: change.fallbackUDP} {
			if tag != "" && (change.byTag[tag] == nil || !slices.Contains(change.byTag[tag].Network(), network)) {
				return false
			}
		}
	}
	for _, member := range previous {
		if _, nested := member.(adapter.OutboundGroup); nested {
			return false
		}
	}
	return true
}

func (transaction *MembershipTransaction) lockGroups(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		locked := 0
		for _, change := range transaction.changes {
			if !change.access.TryLock() {
				break
			}
			locked++
		}
		if locked == len(transaction.changes) {
			if err := ctx.Err(); err != nil {
				transaction.unlockGroups()
				return err
			}
			return nil
		}
		for i := locked - 1; i >= 0; i-- {
			transaction.changes[i].access.Unlock()
		}
		timer := time.NewTimer(time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (transaction *MembershipTransaction) unlockGroups() {
	for _, change := range slices.Backward(transaction.changes) {
		change.access.Unlock()
	}
}

func (transaction *MembershipTransaction) Commit(ctx context.Context) error {
	if err := transaction.lock(ctx); err != nil {
		return err
	}
	defer transaction.access.Unlock()
	if transaction.finished {
		return ErrMembershipFinished
	}
	if transaction.committed {
		return nil
	}
	if err := transaction.lockGroups(ctx); err != nil {
		return err
	}
	defer transaction.unlockGroups()
	for _, change := range transaction.changes {
		if *change.revision != change.expected {
			return ErrMembershipConflict
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	for _, change := range transaction.changes {
		change.oldTags = change.dependencies.Load()
		if selector := change.selector; selector != nil {
			change.oldTags, change.oldByTag, change.oldTCP = selector.tags, selector.outbounds, selector.selected.Load()
			selector.tags, selector.outbounds = change.tags, change.byTag
			selected := change.byTag[change.oldTCP.Tag()]
			if selected == nil {
				selected = change.byTag[change.fallback]
			}
			selector.selected.Store(selected)
		} else {
			group := change.urltest
			change.oldMembers, change.oldTCP, change.oldUDP = group.outbounds, group.selectedOutboundTCP, group.selectedOutboundUDP
			group.outbounds = change.members
			group.selectedOutboundTCP = retainedMember(change.byTag, change.oldTCP, N.NetworkTCP)
			group.selectedOutboundUDP = retainedMember(change.byTag, change.oldUDP, N.NetworkUDP)
			if group.selectedOutboundTCP == nil {
				group.selectedOutboundTCP = change.byTag[change.fallbackTCP]
				if group.selectedOutboundTCP == nil {
					group.selectedOutboundTCP, _ = group.selectLocked(N.NetworkTCP)
				}
			}
			if group.selectedOutboundUDP == nil {
				group.selectedOutboundUDP = change.byTag[change.fallbackUDP]
				if group.selectedOutboundUDP == nil {
					group.selectedOutboundUDP, _ = group.selectLocked(N.NetworkUDP)
				}
			}
		}
		change.dependencies.Store(change.tags)
		*change.revision++
	}
	transaction.committed = true
	return nil
}

func retainedMember(members map[string]adapter.Outbound, previous adapter.Outbound, network string) adapter.Outbound {
	if previous == nil {
		return nil
	}
	member := members[previous.Tag()]
	if member == nil || !slices.Contains(member.Network(), network) {
		return nil
	}
	return member
}

func (transaction *MembershipTransaction) Rollback(ctx context.Context) error {
	if err := transaction.lock(ctx); err != nil {
		return err
	}
	defer transaction.access.Unlock()
	if transaction.rolledBack {
		return nil
	}
	if transaction.finished {
		return ErrMembershipFinished
	}
	if !transaction.committed {
		transaction.finished = true
		transaction.rolledBack = true
		return nil
	}
	if err := transaction.lockGroups(ctx); err != nil {
		return err
	}
	defer transaction.unlockGroups()
	for _, change := range transaction.changes {
		if *change.revision != change.expected+1 {
			return ErrMembershipConflict
		}
	}
	for _, change := range transaction.changes {
		if selector := change.selector; selector != nil {
			selector.tags, selector.outbounds = change.oldTags, change.oldByTag
			selector.selected.Store(change.oldTCP)
		} else {
			change.urltest.outbounds, change.urltest.selectedOutboundTCP, change.urltest.selectedOutboundUDP = change.oldMembers, change.oldTCP, change.oldUDP
		}
		change.dependencies.Store(change.oldTags)
		*change.revision++
	}
	transaction.finished = true
	transaction.rolledBack = true
	return nil
}

func (transaction *MembershipTransaction) lock(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if transaction.access.TryLock() {
			return nil
		}
		timer := time.NewTimer(time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func acquireMembershipDialer(outbound adapter.Outbound) (N.Dialer, func(), error) {
	if leased, ok := outbound.(adapter.OutboundWithDialerLease); ok {
		return leased.AcquireDialer()
	}
	return outbound, func() {}, nil
}

type membershipLeasedOutbound struct {
	adapter.Outbound
	dialer N.Dialer
}

func (outbound *membershipLeasedOutbound) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	return outbound.dialer.DialContext(ctx, network, destination)
}

func (outbound *membershipLeasedOutbound) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	return outbound.dialer.ListenPacket(ctx, destination)
}

func (outbound *membershipLeasedOutbound) MultiplexEnabled() bool {
	multiplex, ok := outbound.Outbound.(adapter.OutboundWithMultiplex)
	return ok && multiplex.MultiplexEnabled()
}

// Called while the membership lock is held. Nested groups remain native and
// acquire their own snapshot when tested; only lease-aware leaves are wrapped.
func leaseMembershipOutbounds(members []adapter.Outbound) ([]adapter.Outbound, func(), error) {
	var releases []func()
	release := func() {
		for _, done := range releases {
			done()
		}
	}
	leased := slices.Clone(members)
	for index, member := range leased {
		if _, group := member.(adapter.OutboundGroup); group {
			continue
		}
		if _, ok := member.(adapter.OutboundWithDialerLease); !ok {
			continue
		}
		dialer, done, err := acquireMembershipDialer(member)
		if err != nil {
			release()
			return nil, nil, err
		}
		releases = append(releases, done)
		leased[index] = &membershipLeasedOutbound{Outbound: member, dialer: dialer}
	}
	return leased, release, nil
}

func (transaction *MembershipTransaction) Release() {
	transaction.access.Lock()
	defer transaction.access.Unlock()
	if transaction.committed {
		transaction.finished = true
		transaction.changes = nil
	}
}
