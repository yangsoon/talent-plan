package main

import (
	"runtime"
	"sort"
	"sync"
)

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

	for i:= 0; i < cpuNum; i++ {
		go func(idx int) {
			defer wg.Done()
			beginIdx := idx * batch
			endIdx := beginIdx + batch
			if idx == cpuNum - 1 {
				endIdx = srcLen
			}
			cpy := make([]int64, endIdx-beginIdx)
			copy(cpy, src[beginIdx: endIdx])
			sort.Slice(cpy, func(i, j int) bool {
				return cpy[i] < cpy[j]
			})
			arrs <- cpy
		}(i)
	}

	wg.Wait()
	close(arrs)

	out := make([][]int64, cpuNum)

	for arr := range arrs {
		out = append(out, arr)
	}
	return out
}

func subMerge(in1, in2 []int64) []int64{
	var res []int64
	leftIdx, rightIdx := 0,0
	leftLen, rightLen := len(in1), len(in2)

	for leftIdx < leftLen && rightIdx < rightLen {
		if in1[leftIdx] < in2[rightIdx] {
			res = append(res, in1[leftIdx])
			leftIdx += 1
		} else {
			res = append(res, in2[rightIdx])
			rightIdx += 1
		}
	}
	res = append(res, in1[leftIdx:]...)
	res = append(res, in2[rightIdx:]...)
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

	//cpuf, err := os.Create("cpu_pro")
	//
	//if err != nil {
	//	log.Fatal(err)
	//}
	//
	//pprof.StartCPUProfile(cpuf)
    //defer pprof.StopCPUProfile()

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