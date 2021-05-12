package filemap


// KeyValue interface provides FileMap methods for storing data
type KeyValue interface {
	Encode() []byte
	EncodeWithEmptyValue() []byte
	Decode(encodedKV []byte) error
	EmptyValue(encodedKV []byte) bool
	InitPos(slotsCount int, capacity uint64) int64
	InitPosFromEncodedKV(encodedKV []byte, slotsCount int, capacity uint64) int64
	EqualKeys(encodedKV []byte) bool
}


