package main

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// сюда писать код

var inputData = []int{0, 1, 1, 2, 3, 5, 8}

func main() {
	testPipeline()
}

func testPipeline() {
	var result string

	hashSignJobs := []job{
		job(func(in, out chan interface{}) {
			for i, fibNum := range inputData {
				fmt.Println("send to chanel, i: ", i)
				out <- fibNum
			}
		}),
		job(SingleHash),
		job(MultiHash),
		job(CombineResults),
		job(func(in, out chan interface{}) {
			fmt.Println(0, "JOB func")
			dataRaw := <-in
			data, ok := dataRaw.(string)
			if !ok {
				panic("cant convert result data to string")
			}
			result = data
		}),
	}

	start := time.Now()

	ExecutePipeline(hashSignJobs...)

	end := time.Since(start)

	fmt.Println("result: ", result, end)
}

func ExecutePipeline(jobs ...job) {
	wg := &sync.WaitGroup{}

	firstIn := make(chan interface{}, 10)
	prevOut := make(chan interface{}, 10)

	for i, jobItem := range jobs {
		in := prevOut
		out := make(chan interface{}, 10)
		prevOut = out

		if i == 0 {
			in = firstIn
		}

		wg.Add(1)
		go func(jb job, i int) {
			fmt.Println(i, "JOB started")
			jb(in, out)
			fmt.Println(i, "JOB finish")
			close(out)
			wg.Done()
		}(jobItem, i)
	}

	wg.Wait()
}

func SingleHash(in, out chan interface{}) {
	wg := &sync.WaitGroup{}

	for dataRaw := range in {

		dataNum, ok := dataRaw.(int)
		if !ok {
			panic("cant convert result data to string")
		}

		data := strconv.Itoa(dataNum)

		part1 := make(chan string)
		part2 := make(chan string)

		md5Res := DataSignerMd5(data)

		go crc32Func(part1, data)
		go crc32Func(part2, md5Res)

		wg.Add(1)
		go func() {
			result := <-part1 + "~" + <-part2
			fmt.Println("[SingleHash] result: ", result)
			out <- result
			wg.Done()
		}()
	}
	wg.Wait()
}

func crc32Func(out chan<- string, data string) {
	out <- DataSignerCrc32(data)
}

const thCount = 6

type multihashItem struct {
	th     int
	result string
}

func createMultihash(data string, out chan interface{}) {
	wg := &sync.WaitGroup{}
	mu := &sync.Mutex{}
	results := []multihashItem{}

	for th := 0; th < thCount; th++ {
		wg.Add(1)

		go func(t int, d string) {
			result := DataSignerCrc32(strconv.Itoa(t) + d)
			mu.Lock()
			results = append(results, multihashItem{t, result})
			mu.Unlock()
			wg.Done()
		}(th, data)
	}

	wg.Wait()

	sort.SliceStable(results, func(i, j int) bool {
		return results[i].th < results[j].th
	})

	fmt.Println("[createMultihash] results slice: ", results)

	hashStr := ""

	for _, item := range results {
		hashStr += item.result
	}

	out <- hashStr
}

func MultiHash(in, out chan interface{}) {
	wg := &sync.WaitGroup{}

	for dataRaw := range in {

		data, ok := dataRaw.(string)
		if !ok {
			panic("cant convert result data to string")
		}

		wg.Add(1)
		go func() {
			createMultihash(data, out)
			wg.Done()
		}()
	}

	wg.Wait()
}

func CombineResults(in, out chan interface{}) {
	resultArr := []string{}

	for dataRaw := range in {
		data, ok := dataRaw.(string)
		if !ok {
			panic("cant convert result data to string")
		}

		resultArr = append(resultArr, data)
	}
	out <- strings.Join(resultArr, "_")
}
