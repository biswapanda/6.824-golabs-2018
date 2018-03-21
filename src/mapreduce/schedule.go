package mapreduce

import (
	"fmt"
	"sync"
	"sync/atomic"
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
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//
	fmt.Printf("Schedule: %v done\n", phase)

	var wg sync.WaitGroup
	defer wg.Wait()

	args := make(chan DoTaskArgs)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < ntasks; i++ {
			arg := DoTaskArgs{
				JobName:       jobName,
				Phase:         phase,
				TaskNumber:    i,
				NumOtherPhase: n_other,
			}
			if phase == mapPhase {
				arg.File = mapFiles[i]
			}
			args <- arg
		}
	}()

	var finished uint32
	done := make(chan struct{})

	for arg := range args {
		worker := <-registerChan

		wg.Add(1)
		go func(worker string, arg DoTaskArgs) {
			defer wg.Done()

			ok := call(worker, "Worker.DoTask", arg, nil)

			wg.Add(1)
			go func() {
				defer wg.Done()

				select {
				case registerChan <- worker:
				case <-done:
				}
			}()

			if ok {
				if atomic.AddUint32(&finished, 1) == uint32(ntasks) {
					close(args)
					close(done)
					return
				}
			} else {
				args <- arg
			}

		}(worker, arg)
	}
}
