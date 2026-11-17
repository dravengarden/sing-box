//go:build with_gvisor && !windows

package tailscale

import (
	"context"
	"testing"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/log"
	singTun "github.com/sagernet/sing-tun"
)

type singleCloseTun struct {
	singTun.Tun
	closes int
}

func (t *singleCloseTun) Close() error {
	t.closes++
	if t.closes > 1 {
		panic("native TUN closed twice")
	}
	return nil
}

func TestScopedTunDeviceClosesNativeTunOnce(t *testing.T) {
	for _, serverClosed := range []bool{false, true} {
		name := "before-server-start"
		if serverClosed {
			name = "after-server-stop"
		}
		t.Run(name, func(t *testing.T) {
			native := &singleCloseTun{}
			scope := adapter.NewScope(context.Background(), log.NewNOPFactory().Logger())
			device, err := newScopedTunDevice(scope, native, 1500, nil)
			if err != nil {
				t.Fatal(err)
			}
			if serverClosed {
				// tsnet closes its Tun when the server stops.
				if err := device.Close(); err != nil {
					t.Fatal(err)
				}
			}
			if err := scope.Close(); err != nil {
				t.Fatal(err)
			}
			if native.closes != 1 {
				t.Fatalf("native close count = %d, want 1", native.closes)
			}
		})
	}
}
