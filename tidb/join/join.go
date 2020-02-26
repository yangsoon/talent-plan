package main

import (
	"encoding/csv"
	"github.com/pingcap/tidb/util/mvmap"
	"io"
	"os"
	"strconv"
	"unsafe"
)

const blockLine  = 1000

// Join accepts a join query of two relations, and returns the sum of
// relation0.col0 in the final result.
// Input arguments:
//   f0: file name of the given relation0
//   f1: file name of the given relation1
//   offset0: offsets of which columns the given relation0 should be joined
//   offset1: offsets of which columns the given relation1 should be joined
// Output arguments:
//   sum: sum of relation0.col0 in the final result
//func Join(f0, f1 string, offset0, offset1 []int) (sum uint64) {
//
//}
func Join(f0, f1 string, offset0, offset1 []int) (sum uint64) {

	blockResourceInnerCh := make(chan [][]string, 1)
	blockResourceOuterCh := make(chan [][]string, 1)

	go fecthCSVBlock(f0, blockResourceInnerCh)
	hashtable := hashWorker(blockResourceInnerCh, offset0)

	go fecthCSVBlock(f1, blockResourceOuterCh)
	joinWorker(hashtable, blockResourceOuterCh, offset1, &sum)

	return
}

func fecthCSVBlock(f string, blockResourceCh chan [][]string){

	defer close(blockResourceCh)

	csvFile, err := os.Open(f)
	if err != nil {
		panic("ReadFile " + f + "fail\n" + err.Error())
	}
	defer csvFile.Close()

	csvReader := csv.NewReader(csvFile)
	block := make([][]string, 0, blockLine)
	c := 0
	for {
		row, err := csvReader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			panic("ReadCSVFile " + f + "fail\n" + err.Error())
		}
		c++
		block = append(block, row)
		if c == blockLine {
			blockResourceCh <- block
			c = 0
			block = make([][]string, 0, blockLine)
		}
	}

	blockResourceCh <- block
}

func hashWorker(blockResourceCh chan [][]string, offset []int) (hashtable *mvmap.MVMap){

	var keyBuffer []byte
	valBuffer := make([]byte, 8)
	hashtable = mvmap.NewMVMap()
	for block := range blockResourceCh {
		for _, row := range block {
			for j, off := range offset {
				if j > 0 {
					keyBuffer = append(keyBuffer, '_')
				}
				keyBuffer = append(keyBuffer, []byte(row[off])...)
			}
			v, err := strconv.ParseUint(row[0], 10, 64)
			if err != nil {
				panic("hashWorker Convert\n" + err.Error())
			}
			*(*int64)(unsafe.Pointer(&valBuffer[0])) = int64(v)
			hashtable.Put(keyBuffer, valBuffer)
			keyBuffer = keyBuffer[:0]
		}
	}
	return
}

func joinWorker(hashtable *mvmap.MVMap, blockResourceCh chan [][]string, offset []int, sum *uint64) {

	var keyHash []byte
	var vals [][]byte
	for block := range blockResourceCh {
		for _, row := range block {
			for i, off := range offset {
				if i > 0 {
					keyHash = append(keyHash, '_')
				}
				keyHash = append(keyHash, []byte(row[off])...)
			}
			vals = hashtable.Get(keyHash, vals)
			keyHash = keyHash[:0]
			for _, val := range vals {
				v := *(*int64)(unsafe.Pointer(&val[0]))
				*sum += uint64(v)
			}
			vals = vals[:0]
		}
	}

}
//
//func main() {
//	Join("./t/r3.tbl","./t/r3.tbl",[]int{0}, []int{1})
//}