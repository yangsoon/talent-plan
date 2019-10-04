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

1. 测试example，但是由于电脑性能问题，无法在10分钟内完成，最终耗时658.60s。

```shell
go test -v -run=TestExampleURLTop -timeout=20m                                                                                                                                                                                        === RUN   TestExampleURLTop
Case0 PASS, dataSize=1MB, nMapFiles=5, cost=71.227628ms
Case1 PASS, dataSize=1MB, nMapFiles=5, cost=38.368677ms
Case2 PASS, dataSize=1MB, nMapFiles=5, cost=39.188912ms
Case3 PASS, dataSize=1MB, nMapFiles=5, cost=60.133963ms
Case4 PASS, dataSize=1MB, nMapFiles=5, cost=106.654834ms
Case5 PASS, dataSize=1MB, nMapFiles=5, cost=51.533737ms
Case6 PASS, dataSize=1MB, nMapFiles=5, cost=54.91819ms
Case7 PASS, dataSize=1MB, nMapFiles=5, cost=50.392565ms
Case8 PASS, dataSize=1MB, nMapFiles=5, cost=59.541801ms
Case9 PASS, dataSize=1MB, nMapFiles=5, cost=41.949663ms
Case10 PASS, dataSize=1MB, nMapFiles=5, cost=40.056578ms
Case0 PASS, dataSize=10MB, nMapFiles=10, cost=468.779079ms
Case1 PASS, dataSize=10MB, nMapFiles=10, cost=381.587179ms
Case2 PASS, dataSize=10MB, nMapFiles=10, cost=329.319128ms
Case3 PASS, dataSize=10MB, nMapFiles=10, cost=322.881101ms
Case4 PASS, dataSize=10MB, nMapFiles=10, cost=1.072345961s
Case5 PASS, dataSize=10MB, nMapFiles=10, cost=524.189537ms
Case6 PASS, dataSize=10MB, nMapFiles=10, cost=408.346889ms
Case7 PASS, dataSize=10MB, nMapFiles=10, cost=446.816374ms
Case8 PASS, dataSize=10MB, nMapFiles=10, cost=487.561721ms
Case9 PASS, dataSize=10MB, nMapFiles=10, cost=306.624337ms
Case10 PASS, dataSize=10MB, nMapFiles=10, cost=376.177796ms
Case0 PASS, dataSize=100MB, nMapFiles=20, cost=4.750714284s
Case1 PASS, dataSize=100MB, nMapFiles=20, cost=2.983633463s
Case2 PASS, dataSize=100MB, nMapFiles=20, cost=2.948469839s
Case3 PASS, dataSize=100MB, nMapFiles=20, cost=3.043936117s
Case4 PASS, dataSize=100MB, nMapFiles=20, cost=6.422089994s
Case5 PASS, dataSize=100MB, nMapFiles=20, cost=4.868142606s
Case6 PASS, dataSize=100MB, nMapFiles=20, cost=4.652548463s
Case7 PASS, dataSize=100MB, nMapFiles=20, cost=4.082551044s
Case8 PASS, dataSize=100MB, nMapFiles=20, cost=3.113776453s
Case9 PASS, dataSize=100MB, nMapFiles=20, cost=2.713142089s
Case10 PASS, dataSize=100MB, nMapFiles=20, cost=3.204178575s
Case0 PASS, dataSize=500MB, nMapFiles=40, cost=27.915669149s
Case1 PASS, dataSize=500MB, nMapFiles=40, cost=17.785030865s
Case2 PASS, dataSize=500MB, nMapFiles=40, cost=14.928478109s
Case3 PASS, dataSize=500MB, nMapFiles=40, cost=15.174448278s
Case4 PASS, dataSize=500MB, nMapFiles=40, cost=19.036761745s
Case5 PASS, dataSize=500MB, nMapFiles=40, cost=22.623413456s
Case6 PASS, dataSize=500MB, nMapFiles=40, cost=20.502704714s
Case7 PASS, dataSize=500MB, nMapFiles=40, cost=21.296458964s
Case8 PASS, dataSize=500MB, nMapFiles=40, cost=15.326500158s
Case9 PASS, dataSize=500MB, nMapFiles=40, cost=13.868498836s
Case10 PASS, dataSize=500MB, nMapFiles=40, cost=16.3501742s
Case0 PASS, dataSize=1GB, nMapFiles=60, cost=47.301266006s
Case1 PASS, dataSize=1GB, nMapFiles=60, cost=33.105371391s
Case2 PASS, dataSize=1GB, nMapFiles=60, cost=31.467110542s
Case3 PASS, dataSize=1GB, nMapFiles=60, cost=30.571562211s
Case4 PASS, dataSize=1GB, nMapFiles=60, cost=35.320646583s
Case5 PASS, dataSize=1GB, nMapFiles=60, cost=46.651973173s
Case6 PASS, dataSize=1GB, nMapFiles=60, cost=42.557989971s
Case7 PASS, dataSize=1GB, nMapFiles=60, cost=43.077041606s
Case8 PASS, dataSize=1GB, nMapFiles=60, cost=30.530047577s
Case9 PASS, dataSize=1GB, nMapFiles=60, cost=31.555702192s
Case10 PASS, dataSize=1GB, nMapFiles=60, cost=32.766306771s
--- PASS: TestExampleURLTop (658.60s)
PASS
ok  	talent	658.799s
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

1. 任务一，补全 `mapreduce.go`文件中的相应的代码之后，`make test_example` 可以正常执行。

   `mapreduce.go - MRCluster.worker`

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

   > 优化 使用decode 代替 json.unmarshal

   `mapreduce.go - MRCluster.run`

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

2. 任务二：实现你自己`MapF`和`ReduceF`来计算top10。

   首先我们看一下example的实现,语言描述比较难以理解，可以直接看下面的图片: 如图所示，example中的topK的计算分成了2次MapReduce来实现。
   
   <img src="./img/mr-example-1.png"/>

![](./img/mr-example-2.png)

![](./img/mr-1.png)

![](./img/mr-2.png)