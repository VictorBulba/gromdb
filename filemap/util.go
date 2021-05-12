package filemap


func isFree(byteSlice []byte) bool {
	for _, v := range byteSlice {
		if v != 0 {
			return false
		}
	}
	return true
}

func isFreeOrDeleted(encodedKV []byte, kv KeyValue) bool {
	if kv.EmptyValue(encodedKV) {
		return true
	}
	return false
}

func (m *FileMap) isRichDIB(encodedKV []byte, kv KeyValue, actualIndex int64) bool {
	var actualPos = actualIndex / int64(m.keySize)

	var kvInitPos = kv.InitPos(m.slotsCount, m.capacity)
	var encodedKVInitPos = kv.InitPosFromEncodedKV(encodedKV, m.slotsCount, m.capacity)

	var kvDIB = actualPos - kvInitPos
	var encodedKVDIB = actualPos - encodedKVInitPos

	if encodedKVDIB < kvDIB { return true }
	return false
} 

func (m *FileMap) write(v []byte, atIndex int64) error {
	var _, err = m.file.WriteAt(v, atIndex)
	return err
}