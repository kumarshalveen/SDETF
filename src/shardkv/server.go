package shardkv

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "shardmaster"


const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	//Lab4_PartB
	Key       string   // key
	Value     string   // value
	Op        string   // "Put", "Append" or "Get"
	Me        string   // the id of the client
	Ts        int64    // the timestamp of a operation
	Index     int      // the index of the config
}


type ShardKV struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	sm         *shardmaster.Clerk
	px         *paxos.Paxos

	gid int64 // my replica group ID

	// Your definitions here.
	//Lab4_PartB
	database   map[string]string   //database
	config     shardmaster.Config  //config
	index      int                 //index of the config
	seq        int                 //max seq numvber
}


func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	//Lab4_PartB
	kv.index = kv.Config.Num
	if (args.Num > kv.index) {
		reply.Err = ErrIndex
		return nil
	}
	shard := key2shard(args.Key)
	if (shard != kv.gid) {
		reply.Err = ErrWrongGroup
		return nil
	}
	proposal := Op{args.Key, "", args.Op, args.Me, args.Ts, args.Index}
	kv.UpdateDB(proposal)
	return nil
}

// RPC handler for client Put and Append requests
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
	//Lab4_PartB
	kv.index = kv.Config.Num
	if (args.Num > kv.index) {
		reply.Err = ErrIndex
		return nil
	}
	shard := key2shard(args.Key)
	if (shard != kv.gid) {
		reply.Err = ErrWrongGroup
		return nil
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	proposal := Op{args.Key, args.Value, args.Op, args.Me, args.Ts, args.Index}
	kv.UpdateDB(proposal)
	return nil
}

//Lab4_PartB
func (kv *shardkv) UpdateDB(op Op) {
	for {
		kv.seq++
		kv.px.Start(kv.seq, op)
		Act := Op{}
		to := 10 * time.Millisecond
		for {
			stat, act := kv.px.GetSeq(kv.seq)
			if (stat == paxos.Decided) {
				Act = act.(Op)
				break
			}
			time.Sleep(to)
			if (to < 10*time.Second) {
				to *= 2
			}
		}
		kv.ProcOperation(Act)
	}
}

//Lab4_PartB
func (kv *ShardKV) ProcOperation(op Op) {
	
}

//Lab4_PartB
func (kv *ShardKV) GetShardDatabase(args *GetShardDatabaseArgs, reply *GetReply) err {
	shard := args.Shard
	val, ok := kv.config.Groups[shard]
	if (ok == false) {
		reply.Err = ErrWrongGroup
	}
	reply.Err = OK
	reply.Database = kv.database
	return nil
}

//Lab4_PartB
func exist(arr []int64, e int64) bool {
	for _, v := range arr {
		if (v == e) {
			return true
		}
	}
	return false
}

//Lab4_PartB
func equal(a []int, b []int) bool {
	la := len(a)
	lb := len(b)
	if (la != lb) {
		return false 
	}
	for i := 0; i < la; i++ {
		if (a[i] != b[i]) {
			return false
		}
	}
	return true
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {
	//Lab4_PartB
	kv.mu.Lock()
	defer kv.mu.Unlock()
	new_config := kv.sm.Query(-1)
	new_index := new_config.Num
	if (kv.index >= new_index) {
		return
	}

	// update config and get the new shards' databases 
	kv.index++
	for kv.index < new_index {
		config := kv.sm.Query(kv.index)
		if (equal(kv.config.Groups[kv.gid], config.Groups[kv.gid]) ) {
			return
		}
		for _, srv := range config.Groups[kv.gid] {
			args := &GetShardDatabaseArgs(kv.gid)
			reply := GetShardDatabaseReply{}
			for _, srv := range config.Groups[new_shard] {
				ok := call(srv, "ShardKV.GetShardDatabase", args, &reply)
				if (ok && reply.Error == OK) {
					kv.config = config
					kv.database = reply.database
					return
				}
			}

		}
	}
}

// tell the server to shut itself down.
// please don't change these two functions.
func (kv *ShardKV) kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

func (kv *ShardKV) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *ShardKV) Setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *ShardKV) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//   servers that implement the shardmaster.
// servers[] contains the ports of the servers
//   in this replica group.
// Me is the index of this server in servers[].
//
func StartServer(gid int64, shardmasters []string,
	servers []string, me int) *ShardKV {
	gob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.gid = gid
	kv.sm = shardmaster.MakeClerk(shardmasters)

	// Your initialization code here.
	// Don't call Join().
	//Lab4_PartB
	kv.database = make(map[string]string{})
	kv.config = shardmaster.Config{}
	kv.index = 0
	kv.seq = 0

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
				fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	go func() {
		for kv.isdead() == false {
			kv.tick()
			time.Sleep(250 * time.Millisecond)
		}
	}()

	return kv
}
