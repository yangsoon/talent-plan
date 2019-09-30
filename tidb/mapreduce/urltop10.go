package main

import (
	"bytes"
	"container/heap"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// URLTop10 .
func URLTop10(nWorkers int) RoundsArgs {
	var args RoundsArgs
	// round 1: do url count
	args = append(args, RoundArgs{
		MapFunc:    URLCountMap,
		ReduceFunc: URLCountReduce,
		NReduce:    nWorkers,
	})

	args = append(args, RoundArgs{
		MapFunc: GetTopKMap,
		ReduceFunc: GetTopKReduce,
		NReduce: 1,
	})

	return args
}

// ExampleURLCountMap is the map function in the first round
func URLCountMap(filename string, contents string) []KeyValue {
	lines := strings.Split(string(contents), "\n")
	kvs := make([]KeyValue, len(lines))
	c := 0
	for _, l := range lines {
		l = strings.TrimSpace(l)
		if len(l) == 0 {
			continue
		}
		kvs[c] = KeyValue{Key: l}
		c++
	}
	return kvs[:c]
}

// ExampleURLCountReduce is the reduce function in the first round
func URLCountReduce(key string, values []string) string {
	return fmt.Sprintf("%s %s\n", key, strconv.Itoa(len(values)))
}


func GetTopKMap (filename string, contents string) []KeyValue {
	lines := strings.Split(contents, "\n")
	kvs := make([]KeyValue, 0, 10)
	us, cs := TopK(lines, 10)
	for i:=0; i < len(us); i++ {
		kvs = append(kvs, KeyValue{"", fmt.Sprintf("%s %d", us[i], cs[i])})
	}
	return kvs
}

func GetTopKReduce (key string, values []string) string {
	us, cs := TopK(values, 10)
	buf := new(bytes.Buffer)
	for i:=len(us)-1; i >= 0; i-- {
		fmt.Fprintf(buf, "%s: %d\n", us[i], cs[i])
	}
	return buf.String()
}


func TopK(urls []string, n int) ([]string, []int) {

	us := make([]string, 0, n)
	cs := make([]int, 0, n)

	if len(urls) <= n {
		ucs := make([] *UrlItem, 0, len(urls))
		for i:=0; i < len(urls); i++{
			if len(urls[i]) == 0 {
				continue
			}
			tmp := strings.Split(urls[i], " ")
			n, err := strconv.Atoi(tmp[1])
			if err != nil {
				panic(err)
			}
			ucs = append(ucs, &UrlItem{tmp[0], n})
		}
		sort.Slice(ucs, func(i, j int) bool {
			if ucs[i].cnt == ucs[j].cnt {
				return ucs[i].url > ucs[j].url
			}
			return ucs[i].cnt < ucs[j].cnt
		})

		for _, u := range ucs {
			us = append(us, u.url)
			cs = append(cs, u.cnt)
		}
		return us, cs
	}

	topk := make(UrlTopK, n)
	for i:=0; i < n; i++ {
		if len(urls[i]) == 0 {
			continue
		}
		tmp := strings.Split(urls[i], " ")
		n, err := strconv.Atoi(tmp[1])
		if err != nil {
			panic(err)
		}
		topk[i] = UrlItem{tmp[0], n}
	}
	heap.Init(&topk)

	minItem := heap.Pop(&topk)
	minVal := minItem.(UrlItem).cnt
	heap.Push(&topk, minItem)

	for i:=n; i < len(urls); i++ {
		if len(urls[i]) == 0 {
			continue
		}
		tmp := strings.Split(urls[i], " ")
		n, err := strconv.Atoi(tmp[1])
		if err != nil {
			panic(err)
		}
		if n < minVal {
			continue
		} else {
			heap.Push(&topk, UrlItem{tmp[0], n})
			heap.Pop(&topk)
			minItem = heap.Pop(&topk)
			minVal = minItem.(UrlItem).cnt
			heap.Push(&topk, minItem)
		}

	}
	for topk.Len() > 0 {
		item := heap.Pop(&topk).(UrlItem)
		us = append(us, item.url)
		cs = append(cs, item.cnt)
	}
	return us, cs
}

type UrlItem struct {
	url string
	cnt int
}

type UrlTopK [] UrlItem

func (u UrlTopK) Len() int {
	return len(u)
}

func (u UrlTopK) Less(i, j int) bool {
	if u[i].cnt == u[j].cnt {
		return u[i].url > u[j].url
	}
	return u[i].cnt < u[j].cnt
}

func (u UrlTopK) Swap(i, j int){
	u[i], u[j] = u[j], u[i]
}

func (u *UrlTopK) Push(a interface{}){
	item := a.(UrlItem)
	*u = append(*u, item)
}

func (u *UrlTopK) Pop() interface{} {
	n := len(*u)
	item := (*u)[n-1]
	*u = (*u)[:n-1]
	return item
}