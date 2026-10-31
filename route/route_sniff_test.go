package route

import (
	"testing"

	"github.com/sagernet/sing/common/buf"
	N "github.com/sagernet/sing/common/network"

	"github.com/stretchr/testify/require"
)

type mtuPacketConn struct {
	N.PacketConn
	mtu int
}

func (c *mtuPacketConn) WriterMTU() int {
	return c.mtu
}

func TestNewSniffPacketBufferHonorsDeclaredSize(t *testing.T) {
	for _, testCase := range []struct {
		mtu      int
		expected int
	}{
		{mtu: 0, expected: buf.UDPBufferSize},
		{mtu: 1500, expected: buf.UDPBufferSize},
		{mtu: 65535, expected: 65535},
	} {
		buffer := newSniffPacketBuffer(&mtuPacketConn{mtu: testCase.mtu})
		require.Equal(t, testCase.expected, buffer.FreeLen(), testCase.mtu)
		buffer.Release()
	}
}
