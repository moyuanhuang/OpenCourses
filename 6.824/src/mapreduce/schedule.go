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
	taskCh := make(chan int)
	nCompleted := 0

	go func(){
		for i := 0; i < ntasks; i++ {
			taskCh <- i
		}
	}()

	Loop:
	for {
		select{
		case taskNumber := <- taskCh:
			worker := <- registerChan  // fetch a worker
			debug("prepare to assign task %d\n", taskNumber)
			go func(){
				wg.Add(1)
				defer wg.Done()
				args := DoTaskArgs{
					JobName: jobName,
					File: mapFiles[taskNumber],
					Phase: phase,
					TaskNumber: taskNumber,
					NumOtherPhase: n_other,
				}
				ok := call(worker, "Worker.DoTask", args, new(struct{}))
				if ok {
					nCompleted++
				} else {
					go func() { taskCh <- taskNumber }()
				}

				go func() { registerChan <- worker }()
			}()
		default:
			if nCompleted == ntasks {
				break Loop  // need add label because break also work in select
			}
		}
	}
	wg.Wait()

	fmt.Printf("Schedule: %v done\n", phase)

	// registerChan should but never closed
}
