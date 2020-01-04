package main

import (
	"fmt"
	"runtime"
	"strconv"

	"github.com/pingcap/tidb/util/mvmap"
)

// Join accepts a join query of two relations, and returns the sum of
// relation0.col0 in the final result.
// Input arguments:
//   f0: file name of the given relation0
//   f1: file name of the given relation1
//   offset0: offsets of which columns the given relation0 should be joined
//   offset1: offsets of which columns the given relation1 should be joined
// Output arguments:
//   sum: sum of relation0.col0 in the final result
func Join(f0, f1 string, offset0, offset1 []int) (sum uint64) {
	tbl0, tbl1 := readCSVFileIntoTbl(f0), readCSVFileIntoTbl(f1)
	if len(tbl0) > len(tbl1) {
		tbl0, tbl1 = tbl1, tbl0
		offset0, offset1 = offset1, offset0
	}
	fmt.Printf("tbl0:%d tbl1:%d\n", len(tbl0), len(tbl1))
	hashtable := buildHashTable(tbl0, offset0)
	numCPU := runtime.NumCPU()
	resultCh := make(chan uint64, numCPU)
	batch := len(tbl1) / numCPU
	for i := 0; i < numCPU; i++ {
		start := i * batch
		end := start + batch
		if i == numCPU-1 {
			end = len(tbl1)
		}
		go func() {
			s, e := start, end
			joinWorker(hashtable, tbl1[s:e], tbl0, offset1, resultCh)
		}()
	}
	for i := 0; i < numCPU; i++ {
		sum += <-resultCh
	}
	return
}

func joinWorker(hashtable *mvmap.MVMap, outerSlice [][]string, innertbl [][]string, offset []int, resultCh chan uint64) {
	var sum uint64
	for _, row := range outerSlice {
		rowIDs := probe(hashtable, row, offset)
		for _, id := range rowIDs {
			v, err := strconv.ParseUint(innertbl[id][0], 10, 64)
			if err != nil {
				panic("JoinExample panic\n" + err.Error())
			}
			sum += v
		}
	}
	resultCh <- sum
}
