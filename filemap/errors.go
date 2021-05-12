package filemap

import "errors"


var (
	// ErrDecoding decoding KV failed
	ErrDecoding = errors.New("Error: Decoding error")
	// ErrNotFound value not found
	ErrNotFound = errors.New("Error: Not found")
	// ErrDeleted value was deleted
	ErrDeleted = errors.New("Error: Deleted")
)