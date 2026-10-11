//go:build linux && with_gvisor

package tailscale

import (
	"errors"
	"fmt"
	"io"
	"testing"

	singTun "github.com/sagernet/sing-tun"
	wgTun "github.com/sagernet/wireguard-go/tun"
)

type batchReadErrorTun struct {
	singTun.LinuxTUN
	count int
	err   error
}

func (t batchReadErrorTun) BatchRead(_ [][]byte, _ int, _ []int) (int, error) {
	return t.count, t.err
}

func TestLinuxTunReadTranslatesRecoverableSegmentOverflow(t *testing.T) {
	for _, err := range []error{singTun.ErrTooManySegments, fmt.Errorf("batch read: %w", singTun.ErrTooManySegments)} {
		adapter := &tunDeviceAdapter{linuxTUN: batchReadErrorTun{count: 2, err: err}}
		count, readErr := adapter.Read(nil, nil, singTun.PacketOffset)
		if count != 2 || !errors.Is(readErr, wgTun.ErrTooManySegments) {
			t.Fatalf("Read() = (%d, %v), want (2, WireGuard segment overflow)", count, readErr)
		}
	}
}

func TestLinuxTunReadPreservesFatalError(t *testing.T) {
	adapter := &tunDeviceAdapter{linuxTUN: batchReadErrorTun{err: io.ErrUnexpectedEOF}}
	count, err := adapter.Read(nil, nil, singTun.PacketOffset)
	if count != 0 || !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("Read() = (%d, %v), want (0, unexpected EOF)", count, err)
	}
}
