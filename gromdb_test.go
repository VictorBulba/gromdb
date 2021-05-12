package gromdb

import (
	"bytes"
	"testing"
	"math/rand"
	"os"
)


func TestGromDB(t *testing.T) {
	var db, err1 = Open("testData")
	if err1 != nil { t.Error(err1); return }
	defer func () {
		db.Close()
		os.RemoveAll("testData")
	}()
	
	db.PutObj(12345, "One two three four five")
	db.PutObj(54321, "Five four three two one")
	db.PutObj(11111, "One one one one one")

	var secondValue string
	db.GetObj((54321), &secondValue)
	var thirdValue string
	db.GetObj((11111), &thirdValue)
	var firstValue string
	db.GetObj((12345), &firstValue)

	if firstValue != "One two three four five" { t.Error("First value"); return }
	if secondValue != "Five four three two one" { t.Error("Second value"); return }
	if thirdValue != "One one one one one" { t.Error("Third value"); return }
}


func TestGromDB2(t *testing.T) {
	var n = 1024

	var keys = genUINT32Slice(n)
	var values = make([][]byte, n)
	for i := range values {
		values[i] = genByteSlice(64)
	}

	var db, err1 = openWithInitSlotsCount("testData", 3)
	if err1 != nil { t.Error(err1); return }
	defer func () {
		db.Close()
		os.RemoveAll("testData")
	}()


	for i, k := range keys {
		var putErr = db.Put(k, values[i])
		if putErr != nil { t.Error(putErr); return }
	}

	for i, k := range keys {
		var value, gerErr = db.Get(k)
		if gerErr != nil { t.Error(gerErr); return }
		if !bytes.Equal(value, values[i]) { t.Error("Values are not equal\n", value, "\n", values[i]); return }
	}
}

func TestGromDBCloseOpen(t *testing.T) {
	var n = 1024

	var keys = genUINT32Slice(n)
	var values = make([][]byte, n)
	for i := range values {
		values[i] = genByteSlice(64)
	}

	var db1, err1 = openWithInitSlotsCount("testData", 3)
	if err1 != nil { t.Error(err1); return }


	for i, k := range keys {
		var putErr = db1.Put(k, values[i])
		if putErr != nil { t.Error(putErr); return }
	}

	db1.Close()

	var db2, err2 = openWithInitSlotsCount("testData", 3)
	if err2 != nil { t.Error(err2); return }
	defer func () {
		db2.Close()
		os.RemoveAll("testData")
	}()

	for i, k := range keys {
		var value, gerErr = db2.Get(k)
		if gerErr != nil { t.Error(gerErr); return }
		if !bytes.Equal(value, values[i]) { t.Error("Values are not equal\n", value, "\n", values[i]); return }
	}
}

func genByteSlice(n int) []byte {
	var byteSlice = make([]byte, n)
	for i := range byteSlice {
		var b = byte(rand.Intn(256))
		byteSlice[i] = b
	}
	return byteSlice
}

func genUINT32Slice(n int) []uint32 {
	var uint32SLice = make([]uint32, n)
	for i := range uint32SLice {
		var num = uint32(rand.Intn(2000000000))
		uint32SLice[i] = num
	}
	return uint32SLice
}


func BenchmarkPut(b *testing.B) {
	var keys = genUINT32Slice(b.N)
	var values = make([][]byte, b.N)
	for i := range values {
		values[i] = genByteSlice(64)
	}

	var db, err1 = Open("testData")
	if err1 != nil { b.Error(err1); return }
	defer func () {
		db.Close()
		os.RemoveAll("testData")
	}()

	b.ResetTimer()
	for i:=0; i<b.N; i++ {
		db.Put(keys[i], values[i])
	}
}


func BenchmarkGet(b *testing.B) {
	var keys = genUINT32Slice(b.N)
	var values = make([][]byte, b.N)
	for i := range values {
		values[i] = genByteSlice(64)
	}

	var db, err1 = Open("testData")
	if err1 != nil { b.Error(err1); return }
	defer func () {
		db.Close()
		os.RemoveAll("testData")
	}()

	for i:=0; i<b.N; i++ {
		db.Put(keys[i], values[i])
	}

	b.ResetTimer()
	for i:=0; i<b.N; i++ {
		db.Get(keys[i])
	}
}