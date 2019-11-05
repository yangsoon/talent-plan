## Introduction

This is the Map-Reduce homework for PingCAP Talent Plan Online of week 2.

There is a uncompleted Map-Reduce framework, you should complete it and use it to extract the 10 most frequent URLs from data files.

## Getting familiar with the source

The simple Map-Reduce framework is defined in `mapreduce.go`.

It is uncompleted and you should fill your code below comments `YOUR CODE HERE`.

The map and reduce function are defined as same as MIT 6.824 lab 1.
```
type ReduceF func(key string, values []string) string
type MapF func(filename string, contents string) []KeyValue
```

There is an example in `urltop10_example.go` which is used to extract the 10 most frequent URLs.

After completing the framework, you can run this example by `make test_example`.

And then please implement your own `MapF` and `ReduceF` in `urltop10.go` to accomplish this task.

After filling your code, please use `make test_homework` to test.

All data files will be generated at runtime, and you can use `make cleanup` to clean all test data.

Please output URLs by lexicographical order and ensure that your result has the same format as test data so that you can pass all tests.

Each test cases has **different data distribution** and you should take it into account.

## Requirements and rating principles

* (40%) Performs better than `urltop10_example`.
* (20%) Pass all test cases.
* (30%) Have a document to describe your idea and record the process of performance optimization (both the framework and your own code) with `pprof`.
* (10%) Have a good code style.

NOTE: **go 1.12 is required**

## How to use

Fill your code below comments `YOUR CODE HERE` in `mapreduce.go` to complete this framework.

Implement your own `MapF` and `ReduceF` in `urltop10.go` and use `make test_homework` to test it.

There is a builtin unit test defined in `urltop10_test.go`, however, you still can write your own unit tests.

How to run example:
```
make test_example
```

How to test your implementation:
```
make test_homework
```

How to clean up all test data:
```
make cleanup
```

How to generate test data again:
```
make gendata
```

## 实验报告

### 实验结果

测试之前，已经事先生成好数据。

1. 测试example，但是由于电脑性能问题，无法在10分钟内完成，最终耗时684.54s。

```shell
go test -v -run=TestExampleURLTop -timeout=20m                                            === RUN   TestExampleURLTop
Case0 PASS, dataSize=1MB, nMapFiles=5, cost=119.523695ms
Case1 PASS, dataSize=1MB, nMapFiles=5, cost=115.721811ms
Case2 PASS, dataSize=1MB, nMapFiles=5, cost=124.180261ms
Case3 PASS, dataSize=1MB, nMapFiles=5, cost=127.17985ms
Case4 PASS, dataSize=1MB, nMapFiles=5, cost=130.713453ms
Case5 PASS, dataSize=1MB, nMapFiles=5, cost=52.638151ms
Case6 PASS, dataSize=1MB, nMapFiles=5, cost=54.842114ms
Case7 PASS, dataSize=1MB, nMapFiles=5, cost=52.565348ms
Case8 PASS, dataSize=1MB, nMapFiles=5, cost=59.055714ms
Case9 PASS, dataSize=1MB, nMapFiles=5, cost=59.378349ms
Case10 PASS, dataSize=1MB, nMapFiles=5, cost=52.794742ms
Case0 PASS, dataSize=10MB, nMapFiles=10, cost=1.115459725s
Case1 PASS, dataSize=10MB, nMapFiles=10, cost=1.255453303s
Case2 PASS, dataSize=10MB, nMapFiles=10, cost=1.156200224s
Case3 PASS, dataSize=10MB, nMapFiles=10, cost=1.127780733s
Case4 PASS, dataSize=10MB, nMapFiles=10, cost=1.095433279s
Case5 PASS, dataSize=10MB, nMapFiles=10, cost=326.149776ms
Case6 PASS, dataSize=10MB, nMapFiles=10, cost=321.470048ms
Case7 PASS, dataSize=10MB, nMapFiles=10, cost=332.02869ms
Case8 PASS, dataSize=10MB, nMapFiles=10, cost=338.62935ms
Case9 PASS, dataSize=10MB, nMapFiles=10, cost=324.002196ms
Case10 PASS, dataSize=10MB, nMapFiles=10, cost=335.976145ms
Case0 PASS, dataSize=100MB, nMapFiles=20, cost=7.469667305s
Case1 PASS, dataSize=100MB, nMapFiles=20, cost=6.545724881s
Case2 PASS, dataSize=100MB, nMapFiles=20, cost=6.952095177s
Case3 PASS, dataSize=100MB, nMapFiles=20, cost=6.66964212s
Case4 PASS, dataSize=100MB, nMapFiles=20, cost=7.130592017s
Case5 PASS, dataSize=100MB, nMapFiles=20, cost=3.097909471s
Case6 PASS, dataSize=100MB, nMapFiles=20, cost=4.529885731s
Case7 PASS, dataSize=100MB, nMapFiles=20, cost=3.668212233s
Case8 PASS, dataSize=100MB, nMapFiles=20, cost=3.211908399s
Case9 PASS, dataSize=100MB, nMapFiles=20, cost=3.558424724s
Case10 PASS, dataSize=100MB, nMapFiles=20, cost=4.053394304s
Case0 PASS, dataSize=500MB, nMapFiles=40, cost=22.772489568s
Case1 PASS, dataSize=500MB, nMapFiles=40, cost=21.082449099s
Case2 PASS, dataSize=500MB, nMapFiles=40, cost=20.13371313s
Case3 PASS, dataSize=500MB, nMapFiles=40, cost=24.846036444s
Case4 PASS, dataSize=500MB, nMapFiles=40, cost=20.128082566s
Case5 PASS, dataSize=500MB, nMapFiles=40, cost=17.532140057s
Case6 PASS, dataSize=500MB, nMapFiles=40, cost=15.599581275s
Case7 PASS, dataSize=500MB, nMapFiles=40, cost=17.79744616s
Case8 PASS, dataSize=500MB, nMapFiles=40, cost=18.660878745s
Case9 PASS, dataSize=500MB, nMapFiles=40, cost=17.923747023s
Case10 PASS, dataSize=500MB, nMapFiles=40, cost=15.723015869s
Case0 PASS, dataSize=1GB, nMapFiles=60, cost=38.860258931s
Case1 PASS, dataSize=1GB, nMapFiles=60, cost=40.10961604s
Case2 PASS, dataSize=1GB, nMapFiles=60, cost=39.532105622s
Case3 PASS, dataSize=1GB, nMapFiles=60, cost=41.06001186s
Case4 PASS, dataSize=1GB, nMapFiles=60, cost=38.938958061s
Case5 PASS, dataSize=1GB, nMapFiles=60, cost=36.904743343s
Case6 PASS, dataSize=1GB, nMapFiles=60, cost=38.0003422s
Case7 PASS, dataSize=1GB, nMapFiles=60, cost=31.111604142s
Case8 PASS, dataSize=1GB, nMapFiles=60, cost=36.862151186s
Case9 PASS, dataSize=1GB, nMapFiles=60, cost=32.246308677s
Case10 PASS, dataSize=1GB, nMapFiles=60, cost=32.189717158s
--- PASS: TestExampleURLTop (684.54s)
PASS
ok  	talent	684.721s
```

2. 测试自己实现的函数，每次执行时间都在82s左右。

```shell
make test_homework
go test -v -run=TestURLTop -timeout=20m
=== RUN   TestURLTop
Case0 PASS, dataSize=1MB, nMapFiles=5, cost=15.252723ms
Case1 PASS, dataSize=1MB, nMapFiles=5, cost=13.985207ms
Case2 PASS, dataSize=1MB, nMapFiles=5, cost=10.857452ms
Case3 PASS, dataSize=1MB, nMapFiles=5, cost=36.515872ms
Case4 PASS, dataSize=1MB, nMapFiles=5, cost=45.720862ms
Case5 PASS, dataSize=1MB, nMapFiles=5, cost=8.11681ms
Case6 PASS, dataSize=1MB, nMapFiles=5, cost=9.484432ms
Case7 PASS, dataSize=1MB, nMapFiles=5, cost=8.609927ms
Case8 PASS, dataSize=1MB, nMapFiles=5, cost=13.522972ms
Case9 PASS, dataSize=1MB, nMapFiles=5, cost=13.754726ms
Case10 PASS, dataSize=1MB, nMapFiles=5, cost=8.85537ms
Case0 PASS, dataSize=10MB, nMapFiles=10, cost=29.353604ms
Case1 PASS, dataSize=10MB, nMapFiles=10, cost=25.887663ms
Case2 PASS, dataSize=10MB, nMapFiles=10, cost=28.145605ms
Case3 PASS, dataSize=10MB, nMapFiles=10, cost=124.192422ms
Case4 PASS, dataSize=10MB, nMapFiles=10, cost=351.065391ms
Case5 PASS, dataSize=10MB, nMapFiles=10, cost=25.943889ms
Case6 PASS, dataSize=10MB, nMapFiles=10, cost=26.579916ms
Case7 PASS, dataSize=10MB, nMapFiles=10, cost=26.825025ms
Case8 PASS, dataSize=10MB, nMapFiles=10, cost=54.322984ms
Case9 PASS, dataSize=10MB, nMapFiles=10, cost=57.368284ms
Case10 PASS, dataSize=10MB, nMapFiles=10, cost=25.156917ms
Case0 PASS, dataSize=100MB, nMapFiles=20, cost=169.519274ms
Case1 PASS, dataSize=100MB, nMapFiles=20, cost=158.753297ms
Case2 PASS, dataSize=100MB, nMapFiles=20, cost=192.766038ms
Case3 PASS, dataSize=100MB, nMapFiles=20, cost=412.692097ms
Case4 PASS, dataSize=100MB, nMapFiles=20, cost=3.525838885s
Case5 PASS, dataSize=100MB, nMapFiles=20, cost=155.941815ms
Case6 PASS, dataSize=100MB, nMapFiles=20, cost=150.832533ms
Case7 PASS, dataSize=100MB, nMapFiles=20, cost=143.09943ms
Case8 PASS, dataSize=100MB, nMapFiles=20, cost=326.409926ms
Case9 PASS, dataSize=100MB, nMapFiles=20, cost=360.033779ms
Case10 PASS, dataSize=100MB, nMapFiles=20, cost=170.866633ms
Case0 PASS, dataSize=500MB, nMapFiles=40, cost=1.023166465s
Case1 PASS, dataSize=500MB, nMapFiles=40, cost=875.27209ms
Case2 PASS, dataSize=500MB, nMapFiles=40, cost=883.850602ms
Case3 PASS, dataSize=500MB, nMapFiles=40, cost=1.441801894s
Case4 PASS, dataSize=500MB, nMapFiles=40, cost=15.604020984s
Case5 PASS, dataSize=500MB, nMapFiles=40, cost=843.779825ms
Case6 PASS, dataSize=500MB, nMapFiles=40, cost=668.798865ms
Case7 PASS, dataSize=500MB, nMapFiles=40, cost=680.748046ms
Case8 PASS, dataSize=500MB, nMapFiles=40, cost=1.19898088s
Case9 PASS, dataSize=500MB, nMapFiles=40, cost=1.184320055s
Case10 PASS, dataSize=500MB, nMapFiles=40, cost=878.084649ms
Case0 PASS, dataSize=1GB, nMapFiles=60, cost=1.585902212s
Case1 PASS, dataSize=1GB, nMapFiles=60, cost=1.645816069s
Case2 PASS, dataSize=1GB, nMapFiles=60, cost=1.726014583s
Case3 PASS, dataSize=1GB, nMapFiles=60, cost=2.986537283s
Case4 PASS, dataSize=1GB, nMapFiles=60, cost=30.434269406s
Case5 PASS, dataSize=1GB, nMapFiles=60, cost=1.770623857s
Case6 PASS, dataSize=1GB, nMapFiles=60, cost=1.803911532s
Case7 PASS, dataSize=1GB, nMapFiles=60, cost=1.564552633s
Case8 PASS, dataSize=1GB, nMapFiles=60, cost=2.224590624s
Case9 PASS, dataSize=1GB, nMapFiles=60, cost=2.301024641s
Case10 PASS, dataSize=1GB, nMapFiles=60, cost=1.662136729s
--- PASS: TestURLTop (82.38s)
PASS
ok  	talent	82.628s
```



### 实现和优化

##### 任务一: 补全 `mapreduce.go`文件中的相应的代码

补全 `mapreduce.go`文件中的相应的代码之后，`make test_example` 可以正常执行。

`MRCluster.worker`函数针对不同类型的任务做不同的处理，这个函数需要我们实现当任务为reduce类型的任务时需要进行的操作。

reduce任务可以分成2步：首先，reduce任务收集属于同一个关键字的所有值，这里用`[]string`存储一个键对应的所有值，用`map[string] []string`来存储所有的键的信息。然后迭代map中的键值对，交由`reduceF`来对每个键的 值集合 进行处理并得出结果，并输出到相应的文件中。

这部分的代码如下所示：

```go
fw, bw := CreateFileAndBuf(mergeName(t.dataDir, t.jobName, t.taskNumber))
kvMap := make(map[string] []string)

for i:=0; i < t.nMap; i++ {
	rpath := reduceName(t.dataDir, t.jobName, i, t.taskNumber)

	file, err := os.Open(rpath)
	if err != nil {
		panic(err)
	}
	decoder := json.NewDecoder(file)
	for {
		var kv KeyValue
		err = decoder.Decode(&kv)
		if err != nil {
			break
		}
		if v, ok := kvMap[kv.Key]; ok{
			kvMap[kv.Key] = append(v, kv.Value)
		} else{
			kvMap[kv.Key] = []string{kv.Value}
		}
	}
	file.Close()

}

for key, value := range kvMap {
	res := t.reduceF(key, value)
	_, err := bw.Write([]byte(res))
	if err != nil {
		log.Fatal(err)
	}
}

SafeClose(fw, bw)
}
t.wg.Done()
```

`MRCluster.run`函数提取来自用户提交程序的配置信息来生成相应的map和reduce任务。

我们要实现根据配置信息生成reduce任务，这部分很简单，关键在于每次任务可能分成了多个MapReduce任务，每次MapReduce任务执行之后需要将reduce生成的结果文件名传递给`notify`通道，告知下一次map任务需要读取的输入文件。

```go
rtasks := make([]*task, 0, nReduce)
for i:=0; i < nReduce; i++ {
	t := &task{
		dataDir:    dataDir,
		jobName:    jobName,
		phase:      reducePhase,
		taskNumber: i,
		nReduce:    nReduce,
		nMap:       nMap,
		reduceF:    reduceF,
	}
	t.wg.Add(1)
	rtasks = append(rtasks, t)
	go func() { c.taskCh <- t }()
}

notifyFiles := make([]string, 0)
for _,t := range rtasks {
	t.wg.Wait()
	fileName := mergeName(t.dataDir, t.jobName, t.taskNumber)
	notifyFiles = append(notifyFiles, fileName)
}
notify <- notifyFiles
```

##### 任务二: 实现你自己`MapF`和`ReduceF`来计算top10。

**example代码分析**

首先我们看一下example的实现,语言描述比较难以理解，可以直接看下面的图片(为了简化流程，图中展示的是计算top1 url的执行过程): 如图所示，example中的topK的计算分成了2次MapReduce来实现。

1. 第一轮，map任务为每个url生成一个键值对，每个map任务将对url做hash处理，将url映射到不同的的reduce任务中，第一轮中的reduce任务统计每种url的出现次数，并输出结果。具体流程如下图的round1所示，第一轮MapReduce任务执行之后，会将产生的结果文件名发送到`notify`管道中，交由下一轮map任务读取。

![](./img/mr-example-1.png)

2. 第二轮，map任务从`notify`管道中读取需要处理的文件，这部分map任务就是简单的将每条数据做一个键值化，将每条url计数后的结果都分配给唯一的reduce任务，reduce任务只处理一个键值对，其中value中存储了所有种类url的计算结果，接下来reduce任务计算出topK，输出到唯一的一个结果文件中。具体的执行过程参考下面的图片。

![](./img/mr-example-2.png)

**第一次优化**	

> 当大概阅读完example的代码之后，我的第一感觉就是example代码有2处可以优化的部分：首先，在第2轮的map任务就可以提前计算每个map中的top10的url，然后将结果发送给reduce任务处理，这样reduce任务的计算压力就会比较小了。第二：因为只需要计算出top10即可，不需要对所有的url进行排序，计算top10的算法可以使用堆进行处理，维护一个有10个元素的小顶堆，这样也会减少对内存的申请，减少gc时间。

根据这样最初的想法，我实现了第一版的urltop10，和example的代码一样，也是分成两轮，其中第一轮的代码直接复用example的代码，第二轮的map任务使用小顶堆来计算top10，然后将结果发送给reduce任务，reduce任务再对收集来的结果也使用小顶堆来计算top10。

**但是**，最终的结果不是很理想，多次执行后，时间和example相差无几，时间在630s左右。

```shell
go tool pprof cpu.prof                                                                                                      Time: Oct 4, 2019 at 5:01pm (CST)
Duration: 10.55mins, Total samples = 24.77mins (234.68%)
Entering interactive mode (type "help" for commands, "o" for options)
(pprof) top
Type: cpu
Showing nodes accounting for 588.25s, 39.58% of 1486.06s total
Dropped 377 nodes (cum <= 7.43s)
Showing top 10 nodes out of 146
      flat  flat%   sum%        cum   cum%
   101.25s  6.81%  6.81%    114.77s  7.72%  encoding/json.stateInString
    76.69s  5.16% 11.97%    164.62s 11.08%  encoding/json.(*decodeState).scanWhile
    66.87s  4.50% 16.47%    189.21s 12.73%  encoding/json.checkValid
    63.91s  4.30% 20.77%     63.91s  4.30%  runtime.pthread_cond_signal
    62.79s  4.23% 25.00%    133.73s  9.00%  runtime.scanobject
    58.89s  3.96% 28.96%     79.60s  5.36%  runtime.findObject
    46.98s  3.16% 32.12%    183.71s 12.36%  runtime.mallocgc
    44.32s  2.98% 35.11%     44.33s  2.98%  encoding/json.unquoteBytes
    33.31s  2.24% 37.35%     94.69s  6.37%  runtime.gcWriteBarrier
    33.24s  2.24% 39.58%     69.77s  4.69%  runtime.mapassign_faststr
```

可以看到大部分时间都消耗在了json的解析和gc上，突然我意识到，性能瓶颈不在于排序计算，而是在于要降低json解析的压力，那么就需要尽量减少中间文件的大小。

**第二次优化**

**继续看example代码，还能发现几个很明显的问题，首先每一轮的map任务都在做一些很简单的事情，只是对输入结果进行了一下简单的格式化。其次，第一轮的reduce任务每次只能在一组键值对上进行reduce操作，但是这时候reduce任务上已经有足够的信息来计算局部的topk来对中间结果进行压缩。**

根据上面的分析，我进行了第二次的优化，抛弃之前的思路，具体的执行流程如下图所示, 问题的解决还是通过2轮MapReduce操作进行解决。

1. 第一轮，map任务对输入文件的url进行个数统计，计算每种url的出现次数，得到一个url和出现次数的结果 **然后在对结果格式化的时候进行一个特别的处理，和之前的处理不同，格式化的key不再是url，而是ihash(url)，就是经过hash之后的url值， value存储着 url: count形式的字符串**，这样处理的原因是这样的话，map任务中将要发送到同一个reduce work的url都会有相同的key，这样就能保证每个reduce worker经过shuffle&merge之后 获得的输入信息是只有一个键值对的map对象，其中包括了所有发送到该work的url统计信息；reduce任务获取到只有一个键值对的map对象后，首先merge相同url的执行次数，然后计算局部topk，这样输出结果就只包含 K * nReduce 个信息。

   > 可以对比下图和上图，在标有🚀部分的文件，可以看到第一次中间文件的压缩结果，在标有🛩部分的中间文件，有更加明显的压缩。

![](./img/mr-1.png)

2. 第二轮的操作和example的处理就一样了，这里就不做赘述。

![](./img/mr-2.png)

> 之前也考虑过只用一轮MapReduce解决问题，但是思考了一下，发现并不好，首先map worker的处理逻辑就会比较复杂，而且只能开一个reduce worker 会比较浪费cpu资源。实际上经过一轮的压缩，第二轮能够较快的执行完。实验显示第二轮的执行时间都在1ms左右。

**其他优化**

urlcountmap urlcountreduce 申请空间过大