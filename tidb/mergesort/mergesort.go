package main

import (
	"runtime"
	"sort"
	"sync"
)

var interSrc []int64

type partSrc struct {
	start int
	end int
}

func MergeSort(src []int64)  {
	length := len(src)
	numCPU := runtime.NumCPU()
	if length < numCPU {
		sort.Slice(src, func(i, j int) bool {
			return src[i] < src[j]
		})
		return
	}

	interSrc = make([]int64, length)
	batch := length/numCPU
	var wg sync.WaitGroup
	wg.Add(numCPU)

	parts := make([]partSrc, numCPU)

	for i := 0; i < numCPU; i++ {
		start := i*batch
		end := start + batch
		if i == numCPU - 1 {
			end = length
		}
		parts[i] = partSrc{start, end}
		go func() {
			defer wg.Done()
			s,e := start, end
			coreSort(src, s, e)
		}()
	}
	wg.Wait()
	subMerge(src, parts)
}

func coreSort(src []int64, start, end int) {
	if end-start <= 1 {
		return
	}
	mid := (start + end) >> 1
	coreSort(src, start, mid)
	coreSort(src, mid, end)
	merge(src, start, mid, end)
}

func subMerge(src []int64, parts []partSrc) {
	n := len(parts)
	for size:=1; size < n; size *= 2 {
		var wg sync.WaitGroup
		for low := 0; low < n - size; low += size * 2 	{
			start := parts[low].start
			mid := parts[low + size - 1].end
			endIdx := low + size*2 -1
			if endIdx > n - 1 {
				endIdx = n - 1
			}
			end := parts[endIdx].end
			wg.Add(1)
			go func() {
				defer wg.Done()
				merge(src, start, mid, end)
			}()
		}
		wg.Wait()
	}
}

func merge(src []int64, start, mid, end int) {
	left := start
	right := mid
	temp := start
	for left < mid && right < end {
		if src[left] > src[right] {
			interSrc[temp] = src[right]
			temp++; right++
		} else{
			interSrc[temp] = src[left]
			temp++; left++
		}
	}

	for left < mid {
		interSrc[temp] = src[left]
		temp++; left++
	}

	for right < end {
		interSrc[temp] = src[right]
		temp++; right++
	}

	for i:=start; i < end; i++ {
		src[i] = interSrc[i]
	}
}