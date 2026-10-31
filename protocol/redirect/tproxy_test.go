package redirect

import (
	"testing"

	N "github.com/sagernet/sing/common/network"

	"github.com/stretchr/testify/require"
)

func TestTProxyPacketWriterSizesBothCopyDirections(t *testing.T) {
	writer := &tproxyPacketWriter{}
	for name, options := range map[string]N.ReadWaitOptions{
		"reply":   N.NewReadWaitOptions(nil, writer),
		"forward": N.NewReadWaitOptions(writer, nil),
	} {
		buffer := options.NewPacketBuffer()
		require.GreaterOrEqual(t, buffer.FreeLen(), 65527, name)
		buffer.Release()
	}
}
