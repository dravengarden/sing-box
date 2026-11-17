//go:build with_gvisor

package tailscale

import (
	"github.com/sagernet/sing-box/adapter"
	singTun "github.com/sagernet/sing-tun"
	"github.com/sagernet/sing/common/logger"
	wgTun "github.com/sagernet/wireguard-go/tun"
)

// newScopedTunDevice wraps the system TUN and registers the wrapper, not the
// native device, for scope cleanup. tsnet closes the wrapper when its server
// stops; the wrapper closes the native TUN exactly once, because closing a
// Darwin TUN twice panics in its stop pipe.
func newScopedTunDevice(scope *adapter.Scope, tun singTun.Tun, mtu int, logger logger.ContextLogger) (wgTun.Device, error) {
	device, err := newTunDeviceAdapter(tun, mtu, logger)
	if err != nil {
		_ = tun.Close()
		return nil, err
	}
	scope.Add(device.Close)
	return device, nil
}
