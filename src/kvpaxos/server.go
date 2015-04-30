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
import "errors"
import "time"

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

}

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
	database   map[string]string //database
	instance   map[int]interface{} //instance
}


func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	//Lab3_PartB
	kv.mu.Lock()
	proposal := &Proposal{args.Me, args.Id}
	for {
		kv.px.Start(kv.seq, proposal)//request
		to := 10*time.Millisecond
		for {
			status, _ := kv.px.Status(kv.seq)
			if status == paxos.Decided {//have decided
				val, ok := kv.database[args.Key]
				if (ok == false) {//key doesnt exist
					reply.Err = ErrNoKey
				} else {
					reply.Value = val
					reply.Err = OK
				}
				kv.seq += kv.step
				kv.mu.Unlock()
				return nil
			}
			time.Sleep(to)
			if (to < 10*time.Second) {
				to *= 2
			}
		}
	}
	kv.mu.Unlock()
	return errors.New("Get error")
}

func (kv *KVPaxos) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
	//Lab3_PartB
	kv.mu.Lock()
	proposal := &Proposal{args.Me, args.Id}
	for {
		kv.px.Start(kv.seq, proposal)
		to := 10*time.Millisecond
		for {
			status, _ := kv.px.Status(kv.seq)
			if status == paxos.Decided {
				if (kv.database[args.Me] == args.Id) {
					reply.Err = OK
					kv.mu.Unlock()
					return nil
				}
				kv.database[args.Me] = args.Id
				if (args.Op == "Put") {
					kv.database[args.Key] = args.Value
				} else {
					kv.database[args.Key] += args.Value
				}
 				reply.Err = OK
				kv.seq += kv.step
				kv.mu.Unlock()
				return nil
			}
			time.Sleep(to)
			if (to < 10 * time.Second) {
				to *= 2
			}
		}
	}
	kv.mu.Unlock()
	return errors.New("PutAppend error")
}

//Lab3_PartB
func (kv *KVPaxos) GetServers(args *GetServersArgs, reply *GetServersReply) error {
	if (args.Me == "") {
		return errors.New("Empty source address")
	} else {
		reply.Servers = kv.servers
		reply.Err = OK
		return nil
	}
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
	gob.Register(Proposal{})
	gob.Register(paxos.Paxos{})
	kv.servers = servers
	kv.seq = kv.me
	kv.step = len(servers)
	kv.database = make(map[string]string)
	kv.instance = make(map[int]interface{})


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
