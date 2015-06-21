package kvpaxos

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

//Lab3_PartB
//import "errors"
import "time"
//import "reflect"
//import "sort"

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	//Lan3_PartB
	Key        string
	Value      string
	Op         string
	Me         string
	Id         string
	Ts         int64

}
//Lab3_PartB bugs to be fixed
//Test: Concurrent clients
//
type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	// Your definitions here.
	servers    []string//servers of paxos
	seq        int     //the instance number
	step       int     //the interval of seq increment
	database   map[string]string   //database
	logs       map[string]string   //logs
	logs_time  map[string]int64    //the time logs
	seqmap     map[int]bool        //seq map
	seqmax     int
	cnt        int     //if timeout cnt++;if ok cnt = 1
}


func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	//Lab3_PartB
	kv.mu.Lock()
	defer kv.mu.Unlock()
	
	proposal := Op{args.Key, "Get", args.Op, args.Me, args.Id, args.Ts}
	//proposal := Op{Key:args.Key, Op:"Get", Me:args.Me, Id:args.Id}//, time.Now().UnixNano()}
	//cnt := 1
	
	kv.UpdateDB(proposal)
	//fmt.Println(kv.px.GetDB())
	value, ok := kv.database[args.Key]
	if (ok == false) {
		reply.Err = ErrNoKey
	} else {
		reply.Err = OK
		reply.Value = value
	}
	return nil
	
}

func (kv *KVPaxos) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
	//Lab3_PartB
	kv.mu.Lock()
	defer kv.mu.Unlock()
	//kv.UpdateDB("PutAppend")
	// if (kv.logs[args.Me+args.Op] == args.Id) {
	// 	reply.Err = OK
	// 	kv.mu.Unlock()
	// 	return nil
	// }
	proposal := Op{args.Key, args.Value, args.Op, args.Me, args.Id, args.Ts}
	//proposal := Op{Key:args.Key, Value:args.Value, Op:args.Op, Me:args.Me, Id:args.Id}//, time.Now().UnixNano()}
	//kv.px.Start(kv.seq, proposal)//request
	kv.UpdateDB(proposal)
	to := 100*time.Millisecond
	time.Sleep(to)
	reply.Err = OK
	return nil
	
}

//Lab3_PartB
func (kv *KVPaxos) UpdateDB(now Op) {
	ts, in := kv.logs_time[now.Me + now.Op]
	if (in == true && ts >= now.Ts) {
		return
	}
	flag := false
	for {
		i := kv.seq+1
		kv.px.Start(i, now)
		to := 10*time.Millisecond
		for {
			stat, proposal := kv.px.Status(i)
			//flag := false
			if (stat == paxos.Decided) {
				if (proposal == nil) {
					continue
				} else {
					act := proposal.(Op)
					//fmt.Println("SSS",i, kv.px.Max(), act, kv.servers[kv.me])//Test: Concurrent clients 
					//fmt.Println("SSS",kv.px.GetDB())//Test: Concurrent clients 
					if (kv.seqmap[i] == true) {
						break
					}
					if (kv.logs[act.Key] == act.Id) {
						break
					}
					// if (kv.logs2[act.Me + act.Id] == true) {
					// 	break
					// }
					if (act.Op == "Put") {
						tmp2, in2 := kv.logs_time[act.Me + act.Op]
						if (in2 == false) {
							kv.database[act.Key] = act.Value
						} else {
							if (tmp2 < act.Ts) {
								kv.database[act.Key] = act.Value
							}
						}
					} else if (act.Op == "Append") {
						if (kv.logs[act.Me] != act.Id) {
							tmp2, in2 := kv.logs_time[act.Me + act.Op]
							if (in2 == false) {
								kv.database[act.Key] += act.Value
							} else {
								if (tmp2 < act.Ts) {
									kv.database[act.Key] += act.Value
								}
							}
						}
					} else {
						//break
					}
					//kv.px.AddDone(i, kv.me)
					kv.px.Done(i)
					kv.logs[act.Me] = act.Id
					kv.logs_time[act.Me + act.Op] = act.Ts
					kv.seqmap[i] = true
					if (act.Id == now.Id) {
						//kv.seq = i
						//kv.mu.Unlock()
						flag = true 
					}
				}
				break
			}
			time.Sleep(to)
			if (to < 10*time.Second) {
				to *= 2
			} else {
				break
			}
		}
		kv.seq = i
		if (flag == true) {
			return
		}
	}	
	//kv.mu.Unlock()
	return
}


// tell the server to shut itself down.
// please do not change these two functions.
func (kv *KVPaxos) kill() {
	DPrintf("Kill(%d): die\n", kv.me)
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

func (kv *KVPaxos) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *KVPaxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *KVPaxos) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *KVPaxos {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(KVPaxos)
	kv.me = me

	// Your initialization code here.
	//Lab3_PartB
	kv.servers = servers
	kv.seq = 0//kv.me
	kv.step = len(servers)
	kv.database = make(map[string]string)
	kv.logs = make(map[string]string)
	kv.logs_time = make(map[string]int64)
	kv.seqmap = make(map[int]bool)
	kv.seqmax = -1
	kv.cnt = 1


	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l


	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for kv.isdead() == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.isdead() == false {
				if kv.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.isunreliable() && (rand.Int63()%1000) < 200 {
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
			if err != nil && kv.isdead() == false {
				fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	return kv
}
