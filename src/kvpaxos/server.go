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
import "reflect"
import "sort"

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
	database   map[string]string   //database
	instance   map[int]interface{} //instance
	seqs       map[int]bool        //the instances done
}


func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	//Lab3_PartB
	kv.mu.Lock()
	kv.UpdateDB("Get")
	proposal := args
	cnt := 1
	step := len(kv.servers)
	for {
		kv.seq = (kv.px.Max() / step +cnt )* step + kv.me
		kv.px.Start(kv.seq, proposal)//request
		to := 10*time.Millisecond
		for {
			status, _ := kv.px.Status(kv.seq)
			if status == paxos.Decided {//have decided
				kv.UpdateDB("Get")
				val, ok := kv.database[args.Key]
				if (ok == false) {//key doesnt exist
					reply.Err = ErrNoKey
				} else {
					reply.Value = val
					reply.Err = OK
				}
				kv.seq += kv.step
				//fmt.Println("get OK")
				kv.mu.Unlock()
				return nil
			}
			time.Sleep(to)
			//if (to < 100*time.Millisecond) {
			if (to < 4*time.Second) {
				to *= 2
			} else {
				//fmt.Println(kv.database)
				//kv.UpdateDB("Get")
				//kv.UpdateDB("Get")
				//val, ok := kv.database[args.Key]
				//if (ok == false) {//key doesnt exist
				//	reply.Err = ErrNoKey
				//} else {
				//	reply.Value = val
				//	reply.Err = OK
				//}
				//kv.seq += kv.step
				//fmt.Println("get OK")
				//kv.mu.Unlock()
				//return nil

				kv.mu.Unlock()
				return errors.New("Get timeout")
			}
		}
		//cnt++
	}
	kv.mu.Unlock()
	return errors.New("Get error")
}

func (kv *KVPaxos) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
	//Lab3_PartB
	kv.mu.Lock()
	if (kv.database[args.Me] == args.Id) {
		reply.Err = OK
		kv.mu.Unlock()
		return nil
	}
	proposal := args
	cnt := 1
	step := len(kv.servers)
	for {
		//fmt.Println(kv.px.Max())
		kv.seq = (kv.px.Max() / step +cnt )* step + kv.me
		kv.px.Start(kv.seq, proposal)
		to := 10*time.Millisecond
		for {
			status, _ := kv.px.Status(kv.seq)
			if status == paxos.Forgotten {
				fmt.Println("Forgotten")
				break
			}
			if status == paxos.Decided {
				//fmt.Println("PutAppend done")
				reply.Err = OK
				kv.seq += kv.step
				kv.UpdateDB("PutAppend")
				//fmt.Println("pa OK")
				kv.mu.Unlock()
				return nil
			}
			time.Sleep(to)
			//break
			//if (to < 200*time.Millisecond) {
			if (to < 4*time.Second) {
				to *= 2
			} else {
				//kv.UpdateDB("PutAppend")
				//reply.Err = OK
				//kv.seq += kv.step
				//kv.UpdateDB("PutAppend")
				//fmt.Println("pa OK")
				//kv.mu.Unlock()
				//return nil
				//fmt.Println(args, kv.database[args.Key], kv.seq, kv.px.Max())
				kv.mu.Unlock()
				return errors.New("PutAppend timeout")
			}
		}
		//cnt++
		//break
	}
	kv.mu.Unlock()
	return errors.New("PutAppend error")
}

//Lab3_PartB
func (kv *KVPaxos) UpdateDB(op string) {
	//time.Sleep(2*time.Second)
	//kv.mu.Lock()
	args := &paxos.UpdateDBArgs{kv.seq, kv.servers[kv.me]}
	var reply paxos.UpdateDBReply
	var tmp PutAppendArgs
	srv := kv.servers[kv.me]
	//fmt.Println(kv.servers)
	//for _, srv := range kv.servers {
	for {
		ok := call(srv, "Paxos.UpdateDB", args, &reply)
		if (ok == true) {
			db := reply.Database
			//fmt.Println(db)
			//in order to get a ordered map
			keys := make([]int, len(db))
			i := 0
			for k, _ := range db {
				keys[i] = k
				i++
			}
			
			sort.Ints(keys)
			//fmt.Println(keys)
			for _, seq := range keys {
				v := db[seq]
				if (kv.seqs[seq] == true) {
					continue
				}
				//fmt.Println("type:",reflect.TypeOf(v))
				if (reflect.TypeOf(v) == reflect.TypeOf(tmp)) {
					tmp = v.(PutAppendArgs)
					//if (kv.database[tmp.Me] == tmp.Id) {
					//	continue
					//}
					if (tmp.Op == "Put") {
						kv.database[tmp.Key] = tmp.Value
					} else {
						if (kv.database[tmp.Me] != tmp.Id) {
							kv.database[tmp.Key] += tmp.Value
						}
					}
					kv.database[tmp.Me] = tmp.Id
				} else {
					//var t2 GetArgs
					//t2 = v.(GetArgs)
					//kv.database[t2.Me] = t2.Id
				}
				kv.seqs[seq] = true
			}
			break
		}
	}
	//fmt.Println("TTT-",op)
	return
}


//Lab3_PartB
//func (kv *KVPaxos) tick() {
//
//}

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
	gob.Register(PutAppendArgs{})
	gob.Register(GetArgs{})
	gob.Register(paxos.UpdateDBArgs{})
	gob.Register(paxos.UpdateDBReply{})
	gob.Register(paxos.UpdateDBReply{})
	gob.Register(paxos.State{})
	kv.servers = servers
	kv.seq = kv.me
	kv.step = len(servers)
	kv.database = make(map[string]string)
	kv.instance = make(map[int]interface{})
	kv.seqs = make(map[int]bool)

	/*kick
	go func(){
		for {
			time.Sleep(200*time.Millisecond)
			kv.tick()
		}
	}()
	*/


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
