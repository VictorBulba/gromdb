package filebuffer

import "os"
import "io"


// FileBuffer helps to do sequential read operations
type FileBuffer struct {
	offset int
	endOffset int
	data []byte
}

// NewFileBuffer returns inited FileBuffer
func NewFileBuffer(size int) *FileBuffer {
	var buf = &FileBuffer{}
	buf.data = make([]byte, size) 
	return buf
}

// Load reads and caches data at offset from f
func (buf *FileBuffer) Load(f *os.File, offset int64) error {
	buf.clear()
	var _, err = f.ReadAt(buf.data, offset)

	if err == io.EOF { return nil }
	return err
}

func (buf *FileBuffer) clear() {
	for i := range buf.data {
		buf.data[i] = 0
	}
	buf.offset = 0
	buf.endOffset = 0
}

// Offset returns current offset
func (buf *FileBuffer) Offset() int {
	return buf.offset
}

// EndOffset returns current endOffset
func (buf *FileBuffer) EndOffset() int {
	return buf.endOffset
}

// Scan data move offset on n and returns true if it possible 
func (buf *FileBuffer) Scan(n int) bool {
	if (buf.endOffset + n) <= len(buf.data) {
		buf.offset = buf.endOffset
		buf.endOffset += n
		return true
	}
	return false
}

// Bytes returns slice from data witn n scan elements
func (buf *FileBuffer) Bytes() []byte {
	return buf.data[ buf.offset : buf.endOffset ]
}
