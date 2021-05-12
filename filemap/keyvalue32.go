package filemap

import "encoding/binary"


// KeyValue12b created for 32b GromDB and implements KeyValue interface
type KeyValue12b struct {
	Key uint32
	DataIndex uint32
	DataSize uint32
}

// Encode KeyValue12b
func (kv *KeyValue12b) Encode() []byte {
	var encodedKV = make([]byte, 12)
	
	binary.BigEndian.PutUint32(encodedKV[0:4], kv.Key)
	binary.BigEndian.PutUint32(encodedKV[4:8], kv.DataIndex)
	binary.BigEndian.PutUint32(encodedKV[8:12], kv.DataSize)

	return encodedKV
}

// EncodeWithEmptyValue encodes only key but returns all 12b 
func (kv *KeyValue12b) EncodeWithEmptyValue() []byte {
	var encodedKV = make([]byte, 12)
	
	binary.BigEndian.PutUint32(encodedKV[0:4], kv.Key)

	return encodedKV
}

// Decode puts decoded key and values to their fields
func (kv *KeyValue12b) Decode(encodedKV []byte) error {
	if len(encodedKV) != 12 { return ErrDecoding }
	
	kv.Key = binary.BigEndian.Uint32(encodedKV[0:4])
	kv.DataIndex = binary.BigEndian.Uint32(encodedKV[4:8])
	kv.DataSize = binary.BigEndian.Uint32(encodedKV[8:12])

	return nil
}

// EmptyValue checks if encodedValue encoded with EncodeWithEmptyValue()
func (kv *KeyValue12b) EmptyValue(encodedKV []byte) bool {
	return isFree(encodedKV[4:12])
}

// InitPos returns init pos in hashmap withs slotsCount
func (kv *KeyValue12b) InitPos(slotsCount int, capacity uint64) int64 {
	return int64(kv.Key % uint32(slotsCount)) * int64(capacity) / int64(slotsCount)
}

// InitPosFromEncodedKV decodes encodedKey and returns it init pos in hashmap withs slotsCount
func (kv *KeyValue12b) InitPosFromEncodedKV(encodedKV []byte, slotsCount int, capacity uint64) int64 {
	if len(encodedKV) != 12 { return 0 }

	var key = binary.BigEndian.Uint32(encodedKV[0:4])

	return int64(key % uint32(slotsCount)) * int64(capacity) / int64(slotsCount)
}

// EqualKeys returns true if kv.Key and key from encodedKV are equal, false otherwise
func (kv *KeyValue12b) EqualKeys(encodedKV []byte) bool {
	var key = binary.BigEndian.Uint32(encodedKV[0:4])

	if key == kv.Key { return true }
	return false
}