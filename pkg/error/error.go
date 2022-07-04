package error

import "errors"

var (
	ErrKeyDoesNotExist    = errors.New("key does not exist")
	ErrMaxSizeExceeded    = errors.New("maximum memtable size reached")
	ErrOpeningSegmentFile = errors.New("error while opening segment file")
	ErrSegmentLevelEmpty  = errors.New("requested segment level is empty")
)
