package error

import "errors"

var (
	KeyDoesNotExistError = errors.New("Key does not exist")
	MaxSizeExceedError   = errors.New("Maximum memtable size reached")
)
