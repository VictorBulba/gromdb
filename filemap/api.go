package filemap

import "os"
import "math"
import "sync"


// FileMap is a hashmap with Insert, Find and Delete methods but in file, not in memory
type FileMap struct {
	sync.RWMutex
	file *os.File
	slotsCount int
	capacity uint64
	elemCount uint64
	keySize int
}

// OpenFileMap returns inited instance of FileMap
func OpenFileMap(file *os.File, slotsCount int, keySize int, elemCount uint64) *FileMap {
	var m = FileMap{}
	m.keySize = keySize
	m.slotsCount = slotsCount
	m.elemCount = elemCount
	m.capacity = uint64(slotsCount * int(math.Ceil(math.Log2(float64(slotsCount)))))
	m.file = file

	return &m
}

// FillStage range: 0.0 - 1.0. If FileMap keeps 40 keys and if t capacy is 50 the FillStage is 0.8 
func (m *FileMap) FillStage() float32 {
	return float32(m.elemCount) / float32(m.capacity)
}

// ElemCount returns elem count
func (m *FileMap) ElemCount() uint64 {
	return m.elemCount
}

// SlotsCount returns slots count
func (m *FileMap) SlotsCount() int {
	return m.slotsCount
}

// Insert value in FileMap
func (m *FileMap) Insert(kv KeyValue) error {
	var initPos = kv.InitPos(m.slotsCount, m.capacity)
	var startSearchIndex = initPos * int64(m.keySize)
	
	m.Lock()
	var err = m.insert(kv, startSearchIndex)
	if err == nil { m.elemCount++ }
	m.Unlock()
	return err
}

// Find value in file map. Value will be decoded to KeyValue struct
func (m *FileMap) Find(kv KeyValue) error {
	var initPos = kv.InitPos(m.slotsCount, m.capacity)
	var startSearchIndex = initPos * int64(m.keySize)
	
	m.RLock()
	var err = m.find(kv, startSearchIndex)
	m.RUnlock()
	return err
}

// Delete value in file map
func (m *FileMap) Delete(kv KeyValue) error {
	var initPos = kv.InitPos(m.slotsCount, m.capacity)
	var startSearchIndex = initPos * int64(m.keySize)
	
	m.Lock()
	var err = m.delete(kv, startSearchIndex)
	m.Unlock()
	return err
}

// Close file
func (m *FileMap) Close() {
	m.Lock()
	m.file.Close()
	m.Unlock()
}