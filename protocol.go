package unghost

import (
	"encoding/binary"
)

const (
	FlagPing     byte = 0x01
	FlagPong     byte = 0x02
	FlagUserData byte = 0x03
)

const (
	HEADERLENGTH int = 9
)

type tcpReadData struct {
	msg []byte
	len int
	err error
}

// parse header
// header contains flag|credits|length|data
func parseHeader(buf []byte) (byte, uint32, uint32) {
	// will put flag check later with error
	crd := binary.BigEndian.Uint32(buf[1:5])
	len := binary.BigEndian.Uint32(buf[5:9])
	return buf[0], crd, len
}

func putHeader(flag byte, crd uint32, len uint32, buf []byte) {
	// will put flag check later with error
	buf[0] = flag
	binary.BigEndian.PutUint32(buf[1:5], crd)
	binary.BigEndian.PutUint32(buf[5:9], len)
}
