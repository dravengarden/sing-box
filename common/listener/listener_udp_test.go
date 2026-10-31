package listener

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/sagernet/sing/common/buf"
	"github.com/sagernet/sing/common/logger"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"

	"github.com/stretchr/testify/require"
)

type sizeRecorder chan int

func (r sizeRecorder) NewPacket(buffer *buf.Buffer, oob []byte, source M.Socksaddr) {
	r <- buffer.Len()
}

func receivedDatagramSize(t *testing.T, readBufferSize int, datagramSize int) int {
	t.Helper()
	recorder := make(sizeRecorder, 1)
	listener := New(Options{
		Context:           context.Background(),
		Logger:            logger.NOP(),
		Network:           []string{N.NetworkUDP},
		OOBPacketHandler:  recorder,
		UDPReadBufferSize: readBufferSize,
	})
	require.NoError(t, listener.Start())
	defer listener.Close()

	client, err := net.Dial("udp", listener.udpConn.LocalAddr().String())
	require.NoError(t, err)
	defer client.Close()
	_, err = client.Write(make([]byte, datagramSize))
	require.NoError(t, err)

	select {
	case size := <-recorder:
		return size
	case <-time.After(5 * time.Second):
		t.Fatal("datagram was not delivered")
		return 0
	}
}

func TestListenerDefaultBufferTruncatesLargeDatagram(t *testing.T) {
	require.Equal(t, buf.UDPBufferSize, receivedDatagramSize(t, 0, 60000))
}

func TestListenerReadBufferSizeCarriesLargeDatagram(t *testing.T) {
	require.Equal(t, 60000, receivedDatagramSize(t, 65535, 60000))
}
