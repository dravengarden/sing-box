//go:build with_gvisor

package tailscale

import (
	"context"
	"net/netip"
	"testing"

	"github.com/sagernet/sing-box/adapter"

	mDNS "github.com/miekg/dns"
)

func TestNegativeHostBootstrapExpiresBeforeLiveRecord(t *testing.T) {
	transport := &DNSTransport{hosts: make(map[string][]netip.Addr)}
	message := new(mDNS.Msg)
	message.SetQuestion("peer.example.", mDNS.TypeA)
	response, err := transport.Exchange(context.Background(), message)
	if err != nil || response.Rcode != mDNS.RcodeNameError {
		t.Fatalf("bootstrap lookup: %v, %v", response, err)
	}
	assertNegativeTTL(t, response, "peer.example.")
	transport.hosts["peer.example."] = []netip.Addr{netip.MustParseAddr("100.64.0.1")}
	response, err = transport.Exchange(context.Background(), message)
	if err != nil || response.Rcode != mDNS.RcodeSuccess || len(response.Answer) != 1 || len(response.Ns) != 0 {
		t.Fatalf("live record lookup: %v, %v", response, err)
	}
}

func TestNegativeHostFamilyAndAuthorityRoutes(t *testing.T) {
	for _, routes := range []map[string][]adapter.DNSTransport{nil, {"example.": nil}} {
		transport := &DNSTransport{routes: routes, hosts: map[string][]netip.Addr{
			"v4.example.": {netip.MustParseAddr("100.64.0.1")},
		}}
		for _, name := range []string{"missing.example.", "v4.example."} {
			message := new(mDNS.Msg)
			message.SetQuestion(name, mDNS.TypeAAAA)
			response, err := transport.Exchange(context.Background(), message)
			if err != nil {
				t.Fatal(err)
			}
			assertNegativeTTL(t, response, name)
		}
	}
}

func TestSearchNegativeAuthorityMatchesOriginalQuestion(t *testing.T) {
	transport := &DNSTransport{acceptSearchDomain: true, searchDomains: []string{"example."}}
	message := new(mDNS.Msg)
	message.SetQuestion("missing.", mDNS.TypeA)
	response, err := transport.Exchange(context.Background(), message)
	if err != nil {
		t.Fatal(err)
	}
	if response.Question[0] != message.Question[0] {
		t.Fatalf("question mismatch: %v", response.Question)
	}
	assertNegativeTTL(t, response, "missing.")
}

func assertNegativeTTL(t *testing.T, response *mDNS.Msg, name string) {
	t.Helper()
	if len(response.Answer) != 0 || len(response.Ns) != 1 {
		t.Fatalf("missing negative authority: %v", response)
	}
	soa, ok := response.Ns[0].(*mDNS.SOA)
	if !ok || soa.Hdr.Name != name || soa.Hdr.Ttl != 10 || soa.Minttl != 10 {
		t.Fatalf("unbounded negative response: %v", response)
	}
	wire, err := response.Pack()
	if err != nil {
		t.Fatal(err)
	}
	decoded := new(mDNS.Msg)
	if err := decoded.Unpack(wire); err != nil {
		t.Fatal(err)
	}
}
