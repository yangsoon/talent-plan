package main

import (
	"runtime"
	"sort"
	"sync"
)

// MergeSort performs the merge sort algorithm.
// Please supplement this function to accomplish the home work.

func merge(input ...<-chan int64) <-chan int64{
	if len(input) == 1 {
		return input[0]
	}
	mid := len(input)/2
	return subMerge(merge(input[:mid]...), merge(input[mid:]...))
}

func subMerge(input1, input2 <-chan int64) <-chan int64{
	out := make(chan int64)

	go func() {
		item1, ok1 := <- input1
		item2, ok2 := <- input2

		for ok1 || ok2 {
			if !ok1 || (ok2 && item2 <= item1) {
				out <- item2
				item2, ok2 = <-input2
			} else {
				out <- item1
				item1, ok1 = <-input1
			}
		}
		close(out)
	}()

	return out
}

func arr2chan(arr []int64) <- chan int64{
	out := make(chan int64, 2048)
	go func() {
		for _, n := range arr {
			out <- n
		}
		close(out)
	}()
	return out
}

func MergeSort(src []int64) {
	number := runtime.NumCPU()

	if len(src) < number {
		number = len(src)
	}

	arrs := make(chan []int64, number)
	batch := len(src)/number

	var wg sync.WaitGroup
	wg.Add(number)

	for i:=0; i < number; i++ {
		go func(index int) {
			defer wg.Done()

			size := batch
			offset := index * batch

			if index == number - 1{
				size = len(src) - offset
			}

			cpy := make([]int64, size)
			copy(cpy, src[offset:offset+size])

			sort.Slice(cpy, func(i, j int) bool {
				return cpy[i] < cpy[j]
			})

			arrs <- cpy
		}(i)
	}

	wg.Wait()
	close(arrs)

	var inputs [] <- chan int64

	for arr := range arrs{
		inputs = append(inputs, arr2chan(arr))
	}

	output := merge(inputs...)
	for i:=0; i < len(src); i++{
		src[i] = <-output
	}
}
//
//func main(){
//	in := []int64{12,3,4,1,23,12}
//	MergeSort(in)
//	fmt.Println(in)
//}
