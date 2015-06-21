package shardmaster

import "net"
import "fmt"
import "net/rpc"
import "log"

import "paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"

//Lab4_PartA
import "time"

type ShardMaster struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	configs []Config // indexed by config num

	//Lab4_PartA
	seq        int   // max seq number
	index      int   // max index of the configs
	max_ts     int64 // max timestamp
	id         map[int64]bool //map
}


type Op struct {
	// Your data here.
	//Lab4_PartA
	Op         string   // Operation type
	Shard      int      // Shard
	GID        int64    // group id
	Servers    []string // servers
	Ts         int64    // timestamp
	Num        int      // the config num
}

//Lab4_PartA
func (sm *ShardMaster) BalanceShardsAfterJoin (config *Config, gid int64, op Op) {
	divide_min := NShards / len(config.Groups)
    count := map[int64]int{}
    
    //count each gid
    for _, g := range config.Shards {
        count[g]++
    }

    move_cnt := 0
    for i, g := range config.Shards {
        if (move_cnt < divide_min && count[g] > divide_min && g != gid) {
            move_cnt++
            config.Shards[i] = gid
            count[g]--
        }
    }
}

//Lab4_PartA
func (sm *ShardMaster) BalanceShardsAfterLeave (config *Config, gid int64, op Op) {
	//fmt.Println("Leaving:", config)
	//fmt.Println("Op:", op)
	divide_min := NShards / len(config.Groups)
    count := map[int64]int{}

    //count each gid
    for _, g := range config.Shards {
    	count[g]++
    }
    //fmt.Println(count)

    for gi, _ := range config.Groups {
    	for (count[gi] < divide_min && count[gid] > 0) {
    		for i, g := range config.Shards {
    			if (g == gid) {
    				config.Shards[i] = gi
    				count[gi]++
    				count[gid]--
    				break
    			}
    		}
    	}
    }
    for gi, _ := range config.Groups {
    	for (count[gi] == divide_min && count[gid] > 0) {
    		for i, g := range config.Shards {
    			if (g == gid) {
    				config.Shards[i] = gi
    				count[gi]++
    				count[gid]--
    				break
    			}
    		}
    	}
    }
}

//Lab4_PartA
func (sm *ShardMaster) ProcOperation (op Op) {
	//generate a new config
	config := sm.configs[sm.index]
	config.Num = sm.index + 1
	config.Shards = [NShards]int64{}
	for i, g := range sm.configs[sm.index].Shards {
		config.Shards[i] = g
	}
	config.Groups = map[int64][]string{}
	for g, s := range sm.configs[sm.index].Groups {
		config.Groups[g] = s
	}
	sm.configs = append(sm.configs, config)
	sm.index++

	//process a operation
	switch op.Op {
	case "Join":
		_, exist := sm.configs[sm.index].Groups[op.GID]
		if (exist == true) {
			return
		}
		sm.configs[sm.index].Groups[op.GID] = op.Servers
		//load balance
		if (len(sm.configs[sm.index].Groups) > 1) {
			sm.BalanceShardsAfterJoin(&sm.configs[sm.index], op.GID, op)
		} else if (len(sm.configs[sm.index].Groups) == 1) {
			for i:= 0; i < NShards; i++ {
				sm.configs[sm.index].Shards[i] = op.GID
			}
		}
		break
	case "Leave":
		_, exist := sm.configs[sm.index].Groups[op.GID]
		if (exist == false) {
			return
		}
		delete(sm.configs[sm.index].Groups, op.GID)
		if (len(sm.configs[sm.index].Groups) > 0) {
			sm.BalanceShardsAfterLeave(&sm.configs[sm.index], op.GID, op)
		} else if (len(sm.configs[sm.index].Groups) == 0) {
			for i:= 0; i < NShards; i++ {
				sm.configs[sm.index].Shards[i] = 0
			}
		}
		break
	case "Move":
		if (sm.configs[sm.index].Shards[op.Shard] == op.GID) {
			return
		} else {
			sm.configs[sm.index].Shards[op.Shard] = op.GID
		}
		break
	}
	
}

//Lab4_PartA
func (sm *ShardMaster) UpdateDB(proposal Op) {
	for {
		sm.seq++
		sm.px.Start(sm.seq, proposal)
		to := 10 * time.Millisecond
		Act := Op{}
		//get a act log
		for {
			stat, act := sm.px.Status(sm.seq)
			if (stat == paxos.Decided) {
				Act = act.(Op)
				break
			}
			time.Sleep(to)
			if (to < 10*time.Second) {
				to *= 2
			} 
		}
		if (Act.Op != "Query") {
			_, exist := sm.id[Act.Ts]
			if (exist == false) {
				sm.ProcOperation(Act)
			}
			
		}
		sm.id[Act.Ts] = true
		sm.px.Done(sm.seq)
		if (Act.Ts == proposal.Ts) {
			return
		}
	}
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
	// Your code here.
	//Lab4_PartA
	sm.mu.Lock()
	defer sm.mu.Unlock()
	//ts := time.Now().UnixNano()
	ts := rand.Int63()
	proposal := Op{Op:"Join", GID:args.GID, Servers:args.Servers, Ts:ts}
	//update db
	sm.UpdateDB(proposal)
	//time.Sleep(200*time.Millisecond)

	return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	// Your code here.
	//Lab4_PartA
	sm.mu.Lock()
	defer sm.mu.Unlock()
	//ts := time.Now().UnixNano()
	ts := rand.Int63()
	proposal := Op{Op:"Leave", GID:args.GID, Ts:ts}
	//update db
	sm.UpdateDB(proposal)
	//time.Sleep(200*time.Millisecond)

	return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	// Your code here.
	//Lab4_PartA
	sm.mu.Lock()
	defer sm.mu.Unlock()
	//ts := time.Now().UnixNano()
	ts := rand.Int63()
	proposal := Op{Op:"Move", Shard:args.Shard, GID:args.GID, Ts:ts}
	//update db
	sm.UpdateDB(proposal)
	//time.Sleep(200*time.Millisecond)

	return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	// Your code here.
	//Lab4_PartA
	sm.mu.Lock()
	defer sm.mu.Unlock()
	//ts := time.Now().UnixNano()
	ts := rand.Int63()
	proposal := Op{Op:"Query", Num:args.Num, Ts:ts}
	//update db
	sm.UpdateDB(proposal)
	//time.Sleep(200*time.Millisecond)

	//get the config
	if (args.Num == -1 || args.Num > sm.index) {
		reply.Config = sm.configs[sm.index]
	} else {
		reply.Config = sm.configs[args.Num]
	}

	return nil
}


// please don't change these two functions.
func (sm *ShardMaster) Kill() {
	atomic.StoreInt32(&sm.dead, 1)
	sm.l.Close()
	sm.px.Kill()
}

func (sm *ShardMaster) isdead() bool {
	return atomic.LoadInt32(&sm.dead) != 0
}

// please do not change these two functions.
func (sm *ShardMaster) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&sm.unreliable, 1)
	} else {
		atomic.StoreInt32(&sm.unreliable, 0)
	}
}

func (sm *ShardMaster) isunreliable() bool {
	return atomic.LoadInt32(&sm.unreliable) != 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int64][]string{}
	//Lab4_PartA
	sm.index = 0
	sm.seq = 0
	sm.max_ts = 0
	sm.id = make(map[int64]bool)

	rpcs := rpc.NewServer()

	gob.Register(Op{})
	rpcs.Register(sm)
	sm.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	sm.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for sm.isdead() == false {
			conn, err := sm.l.Accept()
			if err == nil && sm.isdead() == false {
				if sm.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if sm.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && sm.isdead() == false {
				fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
				sm.Kill()
			}
		}
	}()

	return sm
}
