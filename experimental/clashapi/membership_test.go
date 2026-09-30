package clashapi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing-box/protocol/group"
	"github.com/sagernet/sing/service"
)

type membershipAPILeaf struct {
	adapter.Outbound
	tag string
}

func (leaf membershipAPILeaf) Tag() string { return leaf.tag }

type membershipAPIManager struct {
	adapter.OutboundManager
	members map[string]adapter.Outbound
}

func (manager membershipAPIManager) Outbound(tag string) (adapter.Outbound, bool) {
	leaf, found := manager.members[tag]
	return leaf, found
}

func TestClashSelectorWritesSurviveMembershipUpdate(t *testing.T) {
	a, b := membershipAPILeaf{tag: "a"}, membershipAPILeaf{tag: "b"}
	ctx := service.ContextWith[adapter.OutboundManager](context.Background(), membershipAPIManager{members: map[string]adapter.Outbound{"a": a}})
	value, err := group.NewSelector(ctx, nil, log.NewNOPFactory().Logger(), "proxy", option.SelectorOutboundOptions{Outbounds: []string{"a"}})
	if err != nil {
		t.Fatal(err)
	}
	selector := value.(*group.Selector)
	if err := selector.Start(); err != nil {
		t.Fatal(err)
	}
	transaction, err := group.PrepareMembership([]group.MembershipUpdate{{Group: selector, Members: []adapter.Outbound{a, b}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := transaction.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	put := func() int {
		request := httptest.NewRequest(http.MethodPut, "/proxies/proxy", strings.NewReader(`{"name":"b"}`))
		request.Header.Set("Content-Type", "application/json")
		request = request.WithContext(context.WithValue(request.Context(), CtxKeyProxy, selector))
		recorder := httptest.NewRecorder()
		updateProxy(recorder, request)
		return recorder.Code
	}
	if status := put(); status != http.StatusNoContent || selector.Now() != "b" {
		t.Fatalf("native selector write failed after membership update: %d", status)
	}
	if err := transaction.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if status := put(); status != http.StatusBadRequest || selector.Now() != "a" {
		t.Fatalf("removed member remained selectable after rollback: %d", status)
	}
}
