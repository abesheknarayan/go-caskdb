package format

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

func generateRandomHeader() (int64, int32, int32) {
	return rand.Int63(), rand.Int31(), rand.Int31()
}

func TestEncodeAndDecodeHeader(t *testing.T) {
	timestamp, key_size, value_size := generateRandomHeader()
	encodedHeader := encodeHeader(timestamp, key_size, value_size)
	d_timestamp, d_key_size, d_value_size := DecodeHeader(encodedHeader.Bytes())
	assert.Equal(t, timestamp, d_timestamp, "Timestamps are not equal!")
	assert.Equal(t, key_size, d_key_size, "Key sizes are not equal!")
	assert.Equal(t, value_size, d_value_size, "Value sizes are not equal!")
}

func TestEncodeAndDecodeKeyValue(t *testing.T) {
	timestamp := int64(rand.Int63())
	key := "name"
	value := "abeshek"
	_, buf := EncodeKeyValue(timestamp, key, value)
	d_timestamp, d_key, d_value := DecodeKeyValue(buf)
	assert.Equal(t, timestamp, d_timestamp, "Timestamps are not equal!")
	assert.Equal(t, key, d_key, "Keys are not equal!")
	assert.Equal(t, value, d_value, "Values are not equal!")
}
