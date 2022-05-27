package format

import (
	"encoding/binary"
)

func DecodeHeader(buf []byte) (int64, int32, int32) {
	timestamp := binary.LittleEndian.Uint64(buf[:8])
	key_size := binary.LittleEndian.Uint32(buf[8:12])
	value_size := binary.LittleEndian.Uint32(buf[12:16])
	return int64(timestamp), int32(key_size), int32(value_size)
}

func DecodeKeyValue(buf []byte) (int, string, string) {
	timestamp, key_size, value_size := DecodeHeader(buf[:HEADER_SIZE])
	key := string(buf[HEADER_SIZE : HEADER_SIZE+key_size])
	value := string(buf[HEADER_SIZE+key_size : HEADER_SIZE+key_size+value_size])
	return int(timestamp), key, value
}
