package filemap

import (
	"fmt"
	"math/rand"
	"os"
	"testing"
)


func generateByteSlice(n int) []byte {
	var data []byte

	for i := 0; i < n; i++ {
		var b = byte(rand.Intn(256))
		data = append(data, b)
	}

	return data
}

func genUINT32Slice(n uint32) []uint32 {
	var data []uint32 

	for i:= uint32(0); i<n; i++ {
		var r = uint32(rand.Intn(2000000000))
		data = append(data, r)
	}
	
	return data
}

func genTestData(n uint32) (keys []uint32, values1 []uint32, values2 []uint32) {
	keys = genUINT32Slice(n)
	values1 = genUINT32Slice(n)
	values2 = genUINT32Slice(n)
	return
}

func BenchmarkInsert(b *testing.B) {
	//fmt.Println("b.N", b.N)
	b.N = 500000
	var file, _ = os.OpenFile("testData", os.O_RDWR|os.O_CREATE, 0666)


	var m = OpenFileMap(file, 65003, 12, 0)

	var keys, values1, values2 = genTestData(uint32(b.N))
	//fmt.Println("Test data created!")
	b.ResetTimer()
	for i:=0; i<b.N; i++ {
		var kv = KeyValue12b{Key: keys[i], DataIndex: values1[i], DataSize: values2[i]}
		m.Insert(&kv)
	}
	b.StopTimer()
	var stat, _ = os.Stat("testData")
	fmt.Println("FileMap size:", stat.Size() / 1024 / 1024, "MB")
	os.Remove("testData")
}

func BenchmarkFind(b *testing.B) {
	//fmt.Println("b.N", b.N)
	b.N = 500000
	var file, _ = os.OpenFile("testData", os.O_RDWR|os.O_CREATE, 0666)

	var m = OpenFileMap(file, 65003, 12, 0)

	var keys, values1, values2 = genTestData(uint32(b.N))
	//fmt.Println("Test data created!")
	for i:=0; i<b.N; i++ {
		var kv = KeyValue12b{Key: keys[i], DataIndex: values1[i], DataSize: values2[i]}
		m.Insert(&kv)
	}
	b.ResetTimer()
	for i:=0; i<b.N; i++ {
		var kv = KeyValue12b{Key: keys[i]}
		m.Find(&kv)
	}
	b.StopTimer()
	os.Remove("testData")
}