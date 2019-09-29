package main

import (
	"runtime"
	"sort"
	"sync"
)

type srcSlice struct {
	data []int64
	beginIdx int
	endIdx int
}

func splitArr(src []int64) [][]int64{
	cpuNum := runtime.NumCPU()
	srcLen := len(src)
	if srcLen < cpuNum {
		cpuNum = srcLen
	}

	arrs := make(chan []int64, cpuNum)
	var wg sync.WaitGroup
	wg.Add(cpuNum)

	batch := srcLen / cpuNum

	slices := make([][]int64, cpuNum)
	for i:=0; i < cpuNum; i++ {
		beginIdx := i * batch
		endIdx := beginIdx + batch
		if i == cpuNum - 1 {
			endIdx = srcLen
		}
		slices[i] = make([]int64, endIdx-beginIdx)
		copy(slices[i], src[beginIdx: endIdx])
		slices[i] = src[beginIdx: endIdx]
	}

	for i:= 0; i < cpuNum; i++ {
		go func(cpy []int64) {
			defer wg.Done()
			sort.Slice(cpy, func(i, j int) bool {
				return cpy[i] < cpy[j]
			})
			arrs <- cpy
		}(slices[i])
	}

	wg.Wait()
	close(arrs)

	out := make([][]int64, cpuNum)
	for i:=0; i < cpuNum; i++{
		out[i] = <-arrs
	}
	return out
}

func subMerge(in1, in2 []int64) []int64{
	leftIdx, rightIdx := 0,0
	leftLen, rightLen := len(in1), len(in2)
	res := make([]int64, leftLen+rightLen)

	i:=0
	for leftIdx < leftLen && rightIdx < rightLen {
		if in1[leftIdx] < in2[rightIdx] {
			res[i] = in1[leftIdx]
			leftIdx += 1
		} else {
			res[i] = in2[rightIdx]
			rightIdx += 1
		}
		i += 1
	}

	for leftIdx < leftLen {
		res[i] = in1[leftIdx]
		i++; leftIdx++
	}
	for rightIdx < rightLen {
		res[i] = in2[rightIdx]
		i++; rightIdx++
	}
	return res
}

func merge(in... []int64) []int64{
	if len(in) == 1 {
		return in[0]
	}
	mid := len(in)/2
	return subMerge(merge(in[:mid]...), merge(in[mid:]...))
}

func MergeSort(src []int64) {
	out := splitArr(src)
	copy(src, merge(out...))
}

//func prepare(src []int64) {
//	rand.Seed(time.Now().Unix())
//	for i := range src {
//		src[i] = rand.Int63()
//	}
//}

//func main()  {
//	numElements := 16 << 20
//	src := make([]int64, numElements)
//	prepare(src)
//	MergeSort(src)
//	//fmt.Println(src)
//}