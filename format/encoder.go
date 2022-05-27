package format

import (
	"bytes"
	"encoding/binary"
)

func encodeHeader(timestamp int64, key_size int32, value_size int32) bytes.Buffer {
	var buf bytes.Buffer
	binary.Write(&buf, binary.LittleEndian, timestamp)
	binary.Write(&buf, binary.LittleEndian, key_size)
	binary.Write(&buf, binary.LittleEndian, value_size)
	return buf
}

func EncodeKeyValue(timestamp int64, key string, value string) (int32, []byte) {
	headerBuffer := encodeHeader(timestamp, int32(len(key)), int32(len(value)))

	var dataBuffer bytes.Buffer
	dataBuffer.WriteString(key)
	dataBuffer.WriteString(value)

	var byteArray []byte
	byteArray = append(byteArray, headerBuffer.Bytes()...)
	byteArray = append(byteArray, dataBuffer.Bytes()...)
	return HEADER_SIZE + int32(dataBuffer.Len()), byteArray
}
