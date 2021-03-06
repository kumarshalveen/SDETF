package mapreduce

import "container/list"
import "fmt"


type WorkerInfo struct {
	address string
	// You can add definitions here.
}


// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

// Schedule jobs to workers
// Lab1_Part2
// Lab1_Part3
func (mr *MapReduce) JobScheduler(id int, operation JobType) {
	for {
		//set variables
		var worker string
		var args DoJobArgs
		var reply DoJobReply
		args.File = mr.file
		args.Operation = operation
		args.JobNumber = id
		switch operation {
		case Map :
			args.NumOtherPhase = mr.nReduce
		case Reduce:
			args.NumOtherPhase = mr.nMap
		}

		//send a job
		ok := false
		select {
			case worker = <- mr.idle_worker :
				ok = call(worker, "Worker.DoJob", args, &reply)
			case worker = <- mr.registerChannel :
				ok = call(worker, "Worker.DoJob", args, &reply)
		}

		//handle a successful one
		if (ok) {
			switch operation {
			case Map :
				mr.map_worker <- id
			case Reduce :
				mr.reduce_worker <- id
			}
			mr.idle_worker <- worker
			return
		} 
	}
}

func (mr *MapReduce) RunMaster() *list.List {
	// Your code here
	// Lab1_Part2
	// Lab1_Part3
	//fmt.Println("nmap", mr.nMap,"    nReduce", mr.nReduce)
	for i := 0; i < mr.nMap; i++ {
		go mr.JobScheduler(i, Map)
	}
	for i := 0; i < mr.nMap; i++ {
		<- mr.map_worker
	}
	//fmt.Println("Map done!")
	for i := 0; i < mr.nReduce; i++ {
		go mr.JobScheduler(i, Reduce)
	}
	for i := 0; i < mr.nReduce; i++ {
		<- mr.reduce_worker
	}

	//fmt.Println("Reduce done")
	return mr.KillWorkers()
}
