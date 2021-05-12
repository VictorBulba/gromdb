package gromdb

import (
	"hash/fnv"
	"math/big"
	"strconv"
	"encoding/json"
	"./filemap"
	"os"
	"path/filepath"
	"math"
)


// GromDB is a 32b database
type GromDB struct {
	m []*filemap.FileMap
	data *os.File
	cfg *dbConfig
	path string
}

type fileMapConfig struct {
	Path string
	SlotsCount int
	ElemCount uint64
}

type dbConfig struct {
	FmCfg []fileMapConfig
	DataLen uint64
	MapNum int
}

// Open a GromDB
func Open(path string) (*GromDB, error) {
	return openWithInitSlotsCount(path, 1009)
}

func openWithInitSlotsCount(path string, slotsCount int) (*GromDB, error) {
	var db = GromDB{}

	db.path = path
	
	var dataPath = path + "/data"

	var parseCfgErr = db.parseCfg()
	if parseCfgErr != nil { return nil, parseCfgErr }

	
	var dataErr error
	db.data, dataErr = createFileWithDirs(dataPath)
	if dataErr != nil { return nil, dataErr }

	if len(db.cfg.FmCfg) == 0 {
		var err = db.createFileMap(slotsCount)
		if err != nil { return nil, err }
	} else {
		for _, cfg := range db.cfg.FmCfg {
			db.openFileMap(cfg)
		}
	}

	return &db, nil
}

func (db *GromDB) createFileMap(slotsCount int) error {
	var name = strconv.Itoa(db.cfg.MapNum) + ".idx"
	var path = db.path + "/" + name
	db.cfg.MapNum++

	var f, err = createFileWithDirs(path)
	if err != nil { return err }

	var m = filemap.OpenFileMap(f, slotsCount, 12, 0)
	db.m = append(db.m, m)

	var cfg = fileMapConfig{Path: name, SlotsCount: slotsCount, ElemCount: 0}
	db.cfg.FmCfg = append(db.cfg.FmCfg, cfg)

	return nil
}

func (db *GromDB) openFileMap(cfg fileMapConfig) error {
	var f, err = createFileWithDirs(db.path + "/" + cfg.Path)
	if err != nil { return err }

	var m = filemap.OpenFileMap(f, cfg.SlotsCount, 12, cfg.ElemCount)
	db.m = append(db.m, m)

	return nil
}

func createFileWithDirs(path string) (*os.File, error) {
	var dirPath = filepath.Dir(path)

	var err1 = os.MkdirAll(dirPath, 0777)
	if err1 != nil { return nil, err1 }

	var file, err2 = os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)

	return file, err2
}

// Put a value to GromDB
func (db *GromDB) Put(key uint32, value []byte) error {
	var lastMapIndex = len(db.m) - 1
	
	if db.m[lastMapIndex].FillStage() >= 0.75 {
		var lastDbSlotsCount = db.m[lastMapIndex].SlotsCount()
		var err = db.createFileMap(db.getNextSlotsCount(lastDbSlotsCount))
		if err != nil { lastDbSlotsCount++ }
	}

	var kv = filemap.KeyValue12b{Key: key, DataSize: uint32(len(value)), DataIndex: uint32(db.cfg.DataLen)}

	var err1 = db.m[lastMapIndex].Insert(&kv)
	if err1 != nil { return err1 }

	var _, err2 = db.data.WriteAt(value, int64(db.cfg.DataLen))
	if err2 != nil { return err2 }

	db.cfg.DataLen += uint64(len(value))

	return nil
}

// Get a value from GromDb
func (db *GromDB) Get(key uint32) ([]byte, error) {
	var lastMapIndex = len(db.m) - 1

	var kv = filemap.KeyValue12b{Key: key}
	var err1 error
	for i:=lastMapIndex; i >= 0; i-- {
		err1 = db.m[i].Find(&kv)
		if err1 == filemap.ErrNotFound { continue }
		if err1 == filemap.ErrDeleted { return nil, ErrNotFound }

		break
	}

	if err1 != nil { return nil, ErrNotFound }

	var value = make([]byte, int(kv.DataSize))
	var _, err2 = db.data.ReadAt(value, int64(kv.DataIndex))
	if err2 != nil { return nil, err2 }

	return value, nil
}

// Close a GromDB
func (db *GromDB) Close() {
	db.updateCfg()
	for i:=0; i<len(db.m); i++ {
		db.m[i].Close()
	}
	db.data.Close()
}

func (db *GromDB) parseCfg() error {
	var path = db.path + "/cfg"

	var cfgFile, err1 = createFileWithDirs(path)
	if err1 != nil { return err1 }
	
	var stat, err2 = cfgFile.Stat()
	if err2 != nil { return err2 }

	if stat.Size() == 0 { db.cfg = &dbConfig{}; return nil }

	var data = make([]byte, stat.Size())
	cfgFile.Read(data)

	var err3 = json.Unmarshal(data, &db.cfg)

	return err3
}

func (db *GromDB) updateCfg() error {
	for i:=0; i < len(db.cfg.FmCfg); i++ {
		db.cfg.FmCfg[i].ElemCount = db.m[i].ElemCount()
	}

	var path = db.path + "/cfg"

	var cfgFile, err1 = createFileWithDirs(path)
	if err1 != nil { return err1 }
	defer cfgFile.Close()
	
	cfgFile.Write([]byte{})

	var jsonData, err2 = json.Marshal(db.cfg)
	if err2 != nil { return err2 }

	cfgFile.Write(jsonData)

	return nil
}

func (db *GromDB) getNextSlotsCount(prevSlotsCount int) int {
	var mult = math.Pow( 4, 1 / (1.0 + 0.1 * float64(db.cfg.MapNum)) )
	var nextSlotsCount = int(math.Ceil(float64(prevSlotsCount) * mult))
	var nextPrimeSlotsCount = getPrimeNumber(nextSlotsCount)
	//fmt.Println(nextPrimeSlotsCount)
	return nextPrimeSlotsCount
}

func getPrimeNumber(at int) int {
	for {
		if big.NewInt(int64(at)).ProbablyPrime(0) {
			return at
		}
		at++
	}
}

// Hash string into uint32 to use as key
func Hash(str string) uint32 {
	var hasher = fnv.New32()
	hasher.Write([]byte(str))
	return hasher.Sum32()
}

// PutObj puts any object
func (db *GromDB) PutObj(key uint32, value interface{}) error {
	var encodedValue, jsonErr = json.Marshal(value)
	if jsonErr != nil { return jsonErr }

	db.Put(key, encodedValue)

	return nil
}

// GetObj get any object , put with PutObj
func (db *GromDB) GetObj(key uint32, value interface{}) error {
	var encodedValue, getErr = db.Get(key)
	if getErr != nil { return getErr }

	var decodingErr = json.Unmarshal(encodedValue, value)
	
	return decodingErr
}