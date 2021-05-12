package filemap

import (
	"../filebuffer"
	"sync"
)


func newFB() interface{} {
	return filebuffer.NewFileBuffer(12 * 32)
}

var bufPool = sync.Pool{New: newFB}

func getFB() *filebuffer.FileBuffer {
	return bufPool.Get().(*filebuffer.FileBuffer)
}

func putFB(fb *filebuffer.FileBuffer) {
	bufPool.Put(fb)
}

func (m *FileMap) insert(kv KeyValue, startSearchIndex int64) error {
	var buf = getFB()
	defer putFB(buf)
	var err = buf.Load(m.file, startSearchIndex)
	if err != nil { return err }

	for buf.Scan(m.keySize) {
		var actualIndex = startSearchIndex + int64(buf.Offset())

		if isFree(buf.Bytes()) || kv.EqualKeys(buf.Bytes()) {
			return m.write(kv.Encode(), actualIndex)
		}
	
		if m.isRichDIB(buf.Bytes(), kv, actualIndex) {
			var writeBuf = kv.Encode()
			return m.moveAndWrite(kv, actualIndex, writeBuf, actualIndex)
		}
	}

	var endSearchIndex = startSearchIndex + int64(buf.EndOffset())
	return m.insert(kv, endSearchIndex)
}

func (m *FileMap) find(kv KeyValue, startSearchIndex int64) error {
	var buf = getFB()
	defer putFB(buf)
	var err = buf.Load(m.file, startSearchIndex)
	if err != nil { return err }

	for buf.Scan(m.keySize) {
		var actualIndex = startSearchIndex + int64(buf.Offset())

		if isFree(buf.Bytes()) || m.isRichDIB(buf.Bytes(), kv, actualIndex) {
			return ErrNotFound
		}

		if kv.EqualKeys(buf.Bytes()) {
			if kv.EmptyValue(buf.Bytes()) { return ErrDeleted }
			return kv.Decode(buf.Bytes())
		}
	}

	var endSearchIndex = startSearchIndex + int64(buf.EndOffset())
	return m.find(kv, endSearchIndex)
}

func (m *FileMap) delete(kv KeyValue, startSearchIndex int64) error {
	var buf = getFB()
	defer putFB(buf)
	var err = buf.Load(m.file, startSearchIndex)
	if err != nil { return err }

	for buf.Scan(m.keySize) {
		var actualIndex = startSearchIndex + int64(buf.Offset())

		if isFree(buf.Bytes()) || m.isRichDIB(buf.Bytes(), kv, actualIndex) {
			return ErrNotFound
		}

		if kv.EqualKeys(buf.Bytes()) {
			return m.write(kv.EncodeWithEmptyValue(), actualIndex)
		}
	}

	var endSearchIndex = startSearchIndex + int64(buf.EndOffset())
	return m.delete(kv, endSearchIndex)
}

func (m *FileMap) moveAndWrite(kv KeyValue, startSearchIndex int64, writeBuf []byte, writeAt int64) error {
	var buf = getFB()
	defer putFB(buf)
	var err = buf.Load(m.file, startSearchIndex)
	if err != nil { return err }
	
	for buf.Scan(m.keySize) {
		if isFreeOrDeleted(buf.Bytes(), kv) {
			return m.write(writeBuf, writeAt)
		}

		writeBuf = append(writeBuf, buf.Bytes()...)
	}

	var endSearchIndex = startSearchIndex + int64(buf.EndOffset())
	return m.moveAndWrite(kv, endSearchIndex, writeBuf, writeAt)
}