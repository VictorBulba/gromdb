package filemap

import (
	//"fmt"
	"os"
	"testing"
)

func TestFileMap(t *testing.T) {
	var f, _ = os.OpenFile("testData", os.O_RDWR|os.O_CREATE, 0666)
	var m = OpenFileMap(f, 7, 12, 0)
	defer m.Close()
	defer os.Remove("testData")

	var wantKV1 = KeyValue12b{Key: 130, DataIndex: 12, DataSize: 55}
	var wantKV2 = KeyValue12b{Key: 23423, DataIndex: 31, DataSize: 12}
	var wantKV3 = KeyValue12b{Key: 121221, DataIndex: 31, DataSize: 12}
	var wantKV4 = KeyValue12b{Key: 3, DataIndex: 3, DataSize: 12}
	var wantKV5 = KeyValue12b{Key: 564666455, DataIndex: 4343, DataSize: 12}

	var haveKV1 = KeyValue12b{Key: 130}
	var haveKV2 = KeyValue12b{Key: 23423}
	var haveKV3 = KeyValue12b{Key: 121221}
	var haveKV4 = KeyValue12b{Key: 3}
	var haveKV5 = KeyValue12b{Key: 564666455}

	m.Insert(&wantKV1)
	m.Insert(&wantKV2)
	m.Insert(&wantKV3)
	m.Insert(&wantKV4)
	m.Insert(&wantKV5)

	m.Find(&haveKV1)
	m.Find(&haveKV2)
	m.Find(&haveKV3)
	m.Find(&haveKV4)
	m.Find(&haveKV5)

	//fmt.Println(haveKV1.DataIndex)

	if haveKV1.DataIndex != wantKV1.DataIndex {
		t.Error("Insert and Find are not equal")
	}
	if haveKV2.DataIndex != wantKV2.DataIndex {
		t.Error("Insert and Find are not equal")
	}
	if haveKV3.DataIndex != wantKV3.DataIndex {
		t.Error("Insert and Find are not equal")
	}
	if haveKV4.DataIndex != wantKV4.DataIndex {
		t.Error("Insert and Find are not equal")
	}
	if haveKV5.DataIndex != wantKV5.DataIndex {
		t.Error("Insert and Find are not equal")
	}
}

func genKVTestData(n int) (kvSlice, emptyKVSlice []KeyValue12b) {
	var keys, values1, values2 = genTestData(uint32(n))

	for i := 0; i < n; i++ {
		var kv = KeyValue12b{Key: keys[i], DataIndex: values1[i], DataSize: values2[i]}
		kvSlice = append(kvSlice, kv)
		var emptyKV = KeyValue12b{Key: keys[i]}
		emptyKVSlice = append(kvSlice, emptyKV)
	}

	return
}
