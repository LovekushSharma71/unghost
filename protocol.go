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
	// HEADERLENGTH defines the strict 9-byte layout: [1B Flag | 4B Credits | 4B DataLength].
	HEADERLENGTH int = 9
)

type tcpReadData struct {
	msg []byte
	len int
	err error
}

// parseHeader extracts the flag, credits, and length from a 9-byte header.
//
// [SPECIFICATION]
// - INTENT: Decode a standardized protocol header according to BigEndian format.
// - PRECONDITION: buf MUST be exactly HEADERLENGTH (9) bytes long to prevent panics.
// - POSTCONDITION: Returns (Flag, Credits, Length) accurately mapped from indices.
func parseHeader(buf []byte) (byte, uint32, uint32) {
	// will put flag check later with error
	crd := binary.BigEndian.Uint32(buf[1:5])
	len := binary.BigEndian.Uint32(buf[5:9])
	return buf[0], crd, len
}

// putHeader encodes the flag, credits, and length into a 9-byte header.
//
// [SPECIFICATION]
// - INTENT: Encode a standardized protocol header according to BigEndian format.
// - PRECONDITION: buf MUST be exactly HEADERLENGTH (9) bytes long to prevent panics.
// - POSTCONDITION: Modifies the target buf array in place.
func putHeader(flag byte, crd uint32, len uint32, buf []byte) {
	// will put flag check later with error
	buf[0] = flag
	binary.BigEndian.PutUint32(buf[1:5], crd)
	binary.BigEndian.PutUint32(buf[5:9], len)
}
