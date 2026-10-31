package route

import (
	"bytes"
	"crypto/rand"
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

func TestCompactSniffPacketBufferKeepsPayload(t *testing.T) {
	for _, size := range []int{0, 1200, 60000} {
		buffer := newSniffPacketBuffer(&mtuPacketConn{mtu: 65535})
		rand.Read(buffer.Extend(size))
		payload := bytes.Clone(buffer.Bytes())
		compact := compactSniffPacketBuffer(buffer)
		require.True(t, bytes.Equal(payload, compact.Bytes()), size)
		require.Less(t, compact.Cap(), 65535, size)
		compact.Release()
	}
}

func TestCompactSniffPacketBufferKeepsDefaultBuffer(t *testing.T) {
	buffer := buf.NewPacket()
	defer buffer.Release()
	require.Same(t, buffer, compactSniffPacketBuffer(buffer))
}
