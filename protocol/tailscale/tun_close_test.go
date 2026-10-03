//go:build with_gvisor && !windows

package tailscale

import (
	"testing"

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

func TestEndpointSharesServerTunCloser(t *testing.T) {
	for _, serverClosed := range []bool{false, true} {
		name := "before-server-start"
		if serverClosed {
			name = "after-server-stop"
		}
		t.Run(name, func(t *testing.T) {
			native := &singleCloseTun{}
			device, err := newTunDeviceAdapter(native, 1500, nil)
			if err != nil {
				t.Fatal(err)
			}
			endpoint := &Endpoint{systemTun: device, taildrop: &taildropManager{}}
			if serverClosed {
				if err := device.Close(); err != nil {
					t.Fatal(err)
				}
			}
			if err := endpoint.Close(); err != nil {
				t.Fatal(err)
			}
			if err := endpoint.Close(); err != nil {
				t.Fatal(err)
			}
			if native.closes != 1 {
				t.Fatalf("native close count = %d, want 1", native.closes)
			}
		})
	}
}
