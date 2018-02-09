package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce  // #.output
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)  // #.input
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	var wg sync.WaitGroup
	// stopCh := make(chan struct{})

	TaskNumber := 0  // [0, ntasks - 1]
	Loop:
	for {
		select{
		case worker := <- registerChan:
			if TaskNumber == ntasks {
				// close(stopCh)
				break Loop  // need add label because break also work in select
			}
			go func(){
				wg.Add(1)
				defer wg.Done()
				args := DoTaskArgs{
					JobName: jobName,
					File: mapFiles[TaskNumber],
					Phase: phase,
					TaskNumber: TaskNumber,
					NumOtherPhase: n_other,
				}
				TaskNumber++
				call(worker, "Worker.DoTask", args, new(struct{}))

				// use an additional stopCh to avoid sending to the closed RegisterCh
				// I feel like this is a more elegant way of closing the channel,
				// otherwise the goroutine won't exit until the end of the program
				// select {
				// case _, ok := <-stopCh:
				// 	if !ok{
				// 		return
				// 	}
				// case registerChan <- worker:
				// }

				// A bad(maybe?) way of queuing the working
				go func() { registerChan <- worker }()
			}()
		}
	}
	wg.Wait()

	fmt.Printf("Schedule: %v done\n", phase)

	// registerChan should but never closed
}
