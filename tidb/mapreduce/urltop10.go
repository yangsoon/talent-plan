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

	args = append(args, RoundArgs{
		MapFunc:    URLCountMap,
		ReduceFunc: URLCountReduce,
		NReduce:    nWorkers,
	})

	args = append(args, RoundArgs{
		MapFunc:    TopKMergeMap,
		ReduceFunc: GetTopKReduce,
		NReduce:    1,
	})

	return args
}

func URLCountMap(filename string, contents string) []KeyValue {
	lines := strings.Split(contents, "\n")
	kv := make(map[string]int, len(lines))
	for _, l := range lines {
		//l = strings.TrimSpace(l)
		if len(l) == 0 {
			continue
		}
		kv[l] += 1
	}
	kvs := make([]KeyValue, 0, len(lines))
	var buffer bytes.Buffer
	for k, v := range kv {

		buffer.WriteString(k)
		buffer.WriteString(" ")
		buffer.WriteString(strconv.Itoa(v))

		kvs = append(kvs, KeyValue{
			Key:   strconv.Itoa(ihash(k) % GetMRCluster().NWorkers()),
			Value: buffer.String(),
		})

		buffer.Reset()
	}
	//fmt.Println(kvs)
	return kvs
}

func URLCountReduce(key string, values []string) string {

	kv := make(map[string]int, len(values))

	for _, value := range values {
		if len(value) == 0 {
			continue
		}
		tmp := strings.Split(value, " ")
		n, err := strconv.Atoi(tmp[1])
		if err != nil {
			panic(err)
		}
		kv[tmp[0]] += n
	}

	topk := Top10(kv)

	buf := new(bytes.Buffer)
	for i := 0; i < len(topk); i++ {
		fmt.Fprintf(buf, "%s %d\n", topk[i].url, topk[i].cnt)
	}
	return buf.String()
}

func TopKMergeMap(filename string, contents string) []KeyValue {
	lines := strings.Split(contents, "\n")
	kvs := make([]KeyValue, 0, len(lines))

	var buffer bytes.Buffer
	for _, l := range lines {
		if len(l) == 0 {
			continue
		}
		tmp := strings.Split(l, " ")

		buffer.WriteString(tmp[0])
		buffer.WriteString(" ")
		buffer.WriteString(tmp[1])

		kvs = append(kvs, KeyValue{
			Key: "",
			Value: buffer.String(),
		})
		buffer.Reset()
	}
	return kvs
}

func GetTopKReduce(key string, values []string) string {
	ucs := make([]*UrlItem, 0, len(values))

	for _, v := range values {
		//v := strings.TrimSpace(v)
		if len(v) == 0 {
			continue
		}
		tmp := strings.Split(v, " ")
		n, err := strconv.Atoi(tmp[1])
		if err != nil {
			panic(err)
		}
		ucs = append(ucs, &UrlItem{tmp[0], n})
	}

	sort.Slice(ucs, func(i, j int) bool {
		if ucs[i].cnt == ucs[j].cnt {
			return ucs[i].url < ucs[j].url
		}
		return ucs[i].cnt > ucs[j].cnt
	})

	buf := new(bytes.Buffer)
	for i := 0; i < len(ucs); i++ {
		if i == 10 {
			break
		}
		fmt.Fprintf(buf, "%s: %d\n", ucs[i].url, ucs[i].cnt)
	}
	return buf.String()
}

func Top10(urlkv map[string]int) UrlTopK {

	c := 0
	topk := make(UrlTopK, 0, 10)

	var minItem interface{}
	var minVal int

	for url, num := range urlkv {
		c ++
		switch {
		case c > 10:
			if num < minVal {
				continue
			}
			heap.Push(&topk, UrlItem{url, num})
			heap.Pop(&topk)
			minItem = heap.Pop(&topk)
			minVal = minItem.(UrlItem).cnt
			heap.Push(&topk, minItem)
		case c < 10:
			topk = append(topk, UrlItem{url, num})
		case c == 10:
			topk = append(topk, UrlItem{url, num})
			heap.Init(&topk)
			minItem = heap.Pop(&topk)
			minVal = minItem.(UrlItem).cnt
			heap.Push(&topk, minItem)
		}
	}
	return topk
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

func (u UrlTopK) Swap(i, j int) {
	u[i], u[j] = u[j], u[i]
}

func (u *UrlTopK) Push(a interface{}) {
	item := a.(UrlItem)
	*u = append(*u, item)
}

func (u *UrlTopK) Pop() interface{} {
	n := len(*u)
	item := (*u)[n-1]
	*u = (*u)[:n-1]
	return item
}
