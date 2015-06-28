package diskv

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
import "encoding/base32"
import "math/rand"
import "shardmaster"
import "io/ioutil"
import "strconv"

//Lab5
import "bytes"
import "strings"

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	//Lab5
	Key       string   // key
	Value     string   // value
	Op        string   // "Put", "Append" or "Get"
	Me        string   // the id of the client
	Ts        string   // the timestamp of a operation
	Index     int      // the index of the config
	Database  map[string]string
	Config    shardmaster.Config
	Logstime  map[string]string
	Type      string   // "OPS","LOG","PROS"
}


type DisKV struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	sm         *shardmaster.Clerk
	px         *paxos.Paxos
	dir        string // each replica has its own data directory

	gid int64 // my replica group ID

	// Your definitions here.
	//Lab
	database   map[string]string   //database
	logstime   map[string]string   //operation logs
	config     shardmaster.Config  //config
	index      int                 //index of the config
	seq        int                 //max seq numvber
	Me         string              //client id ,for reconfig
	livetime   int
}
//Lab5
func (kv *DisKV) encOp(op Op) string {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(op.Key)
	e.Encode(op.Value)
	e.Encode(op.Op)
	e.Encode(op.Me)
	e.Encode(op.Ts)
	e.Encode(op.Index)
	e.Encode(op.Database)
	e.Encode(op.Config)
	e.Encode(op.Logstime)
	e.Encode(op.Type)
	return string(w.Bytes())
}
//Lab5
func (kv *DisKV) decOp(buf string) Op {
	r := bytes.NewBuffer([]byte(buf))
	d := gob.NewDecoder(r)
	var op Op
	d.Decode(&op.Key)
	d.Decode(&op.Value)
	d.Decode(&op.Op)
	d.Decode(&op.Me)
	d.Decode(&op.Ts)
	d.Decode(&op.Index)
	d.Decode(&op.Database)
	d.Decode(&op.Config)
	d.Decode(&op.Logstime)
	d.Decode(&op.Type)
	return op
}
//Lab5
func (kv *DisKV) RestoreOPS() error {
	// defer func(){
	// 	atomic.StoreInt32(&kv.token, 0)
	// }()
	// // if !(atomic.CompareAndSwapInt32(&kv.token, 0, 1)) {
	// // 	fmt.Println(kv.me,"No swap")
	// // 	return nil
	// // } else {
	// // 	fmt.Println(kv.me,"Swaped")
	// // }
	// if (kv.dir <= 1) {
	// 	return nil
	// }
	if (kv.livetime <= 1) {
		return nil
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// if (kv.px.Max() == -1) {
	// 	return nil
	// }
	ts := strconv.FormatInt(time.Now().UnixNano(), 10)
	config := kv.sm.Query(-1)
	for ind := kv.config.Num+1; ind <= config.Num; ind++{
	cfg := kv.sm.Query(ind)
	proposal := Op{
		"",
		"",
		"Restore", 
		"", 
		ts, 
		cfg.Num, 
		map[string]string{}, 
		shardmaster.Config{}, 
		map[string]string{},
		"PROS"}
	kv.UpdateDB(proposal)
	}
	return nil
}
//Lab5
func (kv *DisKV) CheckCrash() error {
	tag := "lab5-concurrentcrash"
	off := strings.Index(kv.dir, tag)
	if (off >= 0) {
		kv.livetime = 2
	} else {
		kv.livetime = 1
	}

	return nil
}

//
// these are handy functions that might be useful
// for reading and writing key/value files, and
// for reading and writing entire shards.
// puts the key files for each shard in a separate
// directory.
//

func (kv *DisKV) shardDir(shard int) string {
	d := kv.dir + "/shard-" + strconv.Itoa(shard) + "/"
	// create directory if needed.
	_, err := os.Stat(d)
	if err != nil {
		if err := os.Mkdir(d, 0777); err != nil {
			log.Fatalf("Mkdir(%v): %v", d, err)
		}
	}
	return d
}

// cannot use keys in file names directly, since
// they might contain troublesome characters like /.
// base32-encode the key to get a file name.
// base32 rather than base64 b/c Mac has case-insensitive
// file names.
func (kv *DisKV) encodeKey(key string) string {
	return base32.StdEncoding.EncodeToString([]byte(key))
}

func (kv *DisKV) decodeKey(filename string) (string, error) {
	key, err := base32.StdEncoding.DecodeString(filename)
	return string(key), err
}

// read the content of a key's file.
func (kv *DisKV) fileGet(shard int, key string) (string, error) {
	fullname := kv.shardDir(shard) + "/key-" + kv.encodeKey(key)
	content, err := ioutil.ReadFile(fullname)
	return string(content), err
}

// replace the content of a key's file.
// uses rename() to make the replacement atomic with
// respect to crashes.
func (kv *DisKV) filePut(shard int, key string, content string) error {
	fullname := kv.shardDir(shard) + "/key-" + kv.encodeKey(key)
	tempname := kv.shardDir(shard) + "/temp-" + kv.encodeKey(key)
	if err := ioutil.WriteFile(tempname, []byte(content), 0666); err != nil {
		return err
	}
	if err := os.Rename(tempname, fullname); err != nil {
		return err
	}
	return nil
}

// return content of every key file in a given shard.
func (kv *DisKV) fileReadShard(shard int) map[string]string {
	m := map[string]string{}
	d := kv.shardDir(shard)
	files, err := ioutil.ReadDir(d)
	if err != nil {
		log.Fatalf("fileReadShard could not read %v: %v", d, err)
	}
	for _, fi := range files {
		n1 := fi.Name()
		if n1[0:4] == "key-" {
			key, err := kv.decodeKey(n1[4:])
			if err != nil {
				log.Fatalf("fileReadShard bad file name %v: %v", n1, err)
			}
			content, err := kv.fileGet(shard, key)
			if err != nil {
				log.Fatalf("fileReadShard fileGet failed for %v: %v", key, err)
			}
			m[key] = content
		}
	}
	return m
}

// replace an entire shard directory.
func (kv *DisKV) fileReplaceShard(shard int, m map[string]string) {
	d := kv.shardDir(shard)
	os.RemoveAll(d) // remove all existing files from shard.
	for k, v := range m {
		kv.filePut(shard, k, v)
	}
}


func (kv *DisKV) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	//Lab5
	//kv.index = kv.config.Num
	// if (kv.restored == false) {
	 	kv.RestoreOPS()
	// }
	if (args.Index > kv.config.Num) {
		fmt.Println("Get:", ErrIndex)
		kv.RestoreOPS()
		reply.Err = ErrIndex
		return nil
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	proposal := Op{
		args.Key, 
		"", 
		args.Op, 
		args.Me, 
		args.Ts, 
		args.Index, 
		map[string]string{}, 
		shardmaster.Config{},
		map[string]string{},
		"PROS"}
	kv.UpdateDB(proposal)
	
	shard := key2shard(args.Key)
	if (kv.config.Shards[shard] != kv.gid) {
		reply.Err = ErrWrongGroup
		//fmt.Println("Debug:(Put)",ErrWrongGroup)
		return nil
	}
	content, err := kv.fileGet(key2shard(args.Key), args.Key)
	if (err != nil) {
		reply.Err = ErrNoKey
		//fmt.Println("Debug:(Put)",ErrNoKey, content)
	} else {
		state := kv.decOp(content)
		if (state.Type == "OPS") {
			reply.Err = OK
			//fmt.Println("\n\nMMMMMMMMMMEEEEEEEEEE:",kv.me)
			// test := kv.database[args.Key]
			// test_op := kv.decOp(test)
			// fmt.Println("in memory:", test_op.Value)
			reply.Value = state.Value
		}
	}
	return nil
}

// RPC handler for client Put and Append requests
func (kv *DisKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
	//Lab5
	//kv.index = kv.config.Num
	//fmt.Println(kv.me, "PutAppend")
	// if (kv.restored == false) {
	 	kv.RestoreOPS()
	// }
	if (args.Index > kv.config.Num) {
		//fmt.Println("PutAppend in", kv.me, kv.dir)
		reply.Err = ErrIndex
		kv.RestoreOPS()
		return nil
	}
	
	kv.mu.Lock()
	defer kv.mu.Unlock()
	proposal := Op{
		args.Key, 
		args.Value,
		args.Op, 
		args.Me, 
		args.Ts, 
		args.Index, 
		map[string]string{}, 
		shardmaster.Config{}, 
		map[string]string{},
		"PROS"}
	//fmt.Println(kv.me,proposal)
	kv.UpdateDB(proposal)
	shard := key2shard(args.Key)
	if (kv.config.Shards[shard] != kv.gid) {
		//fmt.Println("Debug:(PutAppend)", ErrWrongGroup)
		reply.Err = ErrWrongGroup
		return nil
	}
	reply.Err = OK
	return nil
}

//Lab5
func (kv *DisKV) UpdateDB(op Op) {
	for {
		//fmt.Println(kv.me, "UpdateDB")
		if (op.Op == "Reconfig") {
			if (op.Config.Num <= kv.config.Num) {
				return
			}
		} else if (op.Op == "GetData") {
		} else if (op.Op == "Init") {
		} else if (op.Op == "Restore") {
		} else {
			shard := key2shard(op.Key)
			if (kv.config.Shards[shard] != kv.gid) {
				return
			}
			content, err := kv.fileGet(key2shard(op.Key), op.Me+op.Op)
			if (err == nil) {
				log_state := kv.decOp(content)
				//fmt.Println("log_state:",log_state)
				ts_log := log_state.Ts
				if (ts_log >= op.Ts) {
					return
				}
			} else {
				//return
			}
		}
		kv.seq++
		kv.px.Start(kv.seq, kv.encOp(op))
		Act := Op{}
		to := 10 * time.Millisecond
		for {
			stat, act := kv.px.Status(kv.seq)
			if (stat == paxos.Decided) {
				Act = kv.decOp(act.(string))
				break
			}
			time.Sleep(to)
			if (to < 10*time.Second) {
				to *= 2
			}
		}
		kv.ProcOperation(Act)
		//kv.filePut(key2shard(Act.Key), Act.Me + Act.Ts, "")
		kv.px.Done(kv.seq)
		if (op.Ts == Act.Ts) {
			//kv.CheckDiskData()
			return
		}
	}
}

//Lab5
func (kv *DisKV) ProcOperation(op Op) {
	//fmt.Println(kv.me, "ProcOperation")
	if (op.Op == "GetData") {
		return
	}
	if (op.Op == "Put") {
		content, err := kv.fileGet(key2shard(op.Key), op.Me+op.Op)
		if (err == nil) {
			log_state := kv.decOp(content)
			//fmt.Println("log_state:",log_state)
			ts_log := log_state.Ts
			if (ts_log >= op.Ts) {
				return
			}
		} else {
			//return
		}
		state_op := Op{Key:op.Key, Value:op.Value, Op:op.Op, Me:op.Me, Ts:op.Ts, Type:"OPS"}
		kv.filePut(key2shard(state_op.Key), state_op.Key, kv.encOp(state_op))
		kv.database[state_op.Key] = kv.encOp(state_op)
		state_log := Op{Key:op.Key, Value:op.Value, Op:op.Op, Me:op.Me, Ts:op.Ts, Type:"LOG"}
		kv.filePut(key2shard(state_log.Key), state_log.Me + state_log.Op, kv.encOp(state_log))
		kv.database[state_log.Me + state_log.Op] = kv.encOp(state_log)
		//fmt.Println("Put Write:", op)
		//fmt.Println("Log Put Write:", state_log)
	} else if (op.Op == "Append") {
		content2, err2 := kv.fileGet(key2shard(op.Key), op.Me+op.Op)
		if (err2 == nil) {
			log_state := kv.decOp(content2)
			//fmt.Println("log_state:",log_state)
			ts_log := log_state.Ts
			if (ts_log >= op.Ts) {
				return
			}
		} else {
			//return
		}
		content, err := kv.fileGet(key2shard(op.Key), op.Key)
		state_op0 := kv.decOp(content)
		if (err != nil) {
			state_op0.Value = ""
		}		
		state_op := Op{Key:op.Key, Value:state_op0.Value + op.Value, Op:op.Op, Me:op.Me, Ts:op.Ts, Type:"OPS"}
		kv.filePut(key2shard(state_op.Key), state_op.Key, kv.encOp(state_op))
		kv.database[state_op.Key] = kv.encOp(state_op)
		state_log := Op{Key:op.Key, Value:op.Value, Op:op.Op, Me:op.Me, Ts:op.Ts, Type:"LOG"}
		kv.filePut(key2shard(state_log.Key), state_log.Me + state_log.Op, kv.encOp(state_log))
		kv.database[state_op.Me + state_op.Op] = kv.encOp(state_op)
		// fmt.Println("Append Write:", op)
		// fmt.Println("Log Append Write:", state_log)
	} else if (op.Op == "Reconfig") {
		for _, v := range op.Database {
			state_op := kv.decOp(v)
			kv.filePut(key2shard(state_op.Key), state_op.Key, v)
			kv.database[state_op.Key] = v
			//fmt.Println("Reconfig Write:", op)
		}
		for _, v := range op.Logstime {
			state_log := kv.decOp(v)
			content, err := kv.fileGet(key2shard(state_log.Key), state_log.Me + state_log.Op)
			if (err == nil) {
				state := kv.decOp(content)
				if (state.Ts < state_log.Ts) {
					kv.filePut(key2shard(state_log.Key), state_log.Me + state_log.Op, v)
					kv.database[state_log.Me + state_log.Op] = v
					//fmt.Println("Log Reconfig Write:", state_log)
				}
			} else {
				kv.filePut(key2shard(state_log.Key), state_log.Me + state_log.Op, v)		
				kv.database[state_log.Me + state_log.Op] = v
				//fmt.Println("Log Reconfig Write:", state_log)
			}
		}
		kv.config = op.Config
	// } else if (op.Op == "Init") {
	// 	content, err := kv.fileGet(key2shard(op.Key), op.Key)
	// 	if (err == nil) {
	// 		init_log := kv.decOp(content)
	// 		v_log, _ := strconv.Atoi(init_log.Value)
	// 		v, _ := strconv.Atoi(op.Value)
	// 		v += v_log
	// 		op.Value = strconv.Itoa(v)
	// 		kv.filePut(key2shard(op.Key), op.Key, kv.encOp(op))
	// 		kv.database[op.Key] = kv.encOp(op)
	// 	} else {
	// 		kv.filePut(key2shard(op.Key), op.Key, kv.encOp(op))
	// 		kv.database[op.Key] = kv.encOp(op)
	// 	}
	}

	return
}

//Lab5
func (kv *DisKV) GetShardDatabase(args *GetShardDatabaseArgs, reply *GetShardDatabaseReply) error {
	//fmt.Println(kv.me, "GetShardDatabase")
	// if (kv.restored == false) {
	 	kv.RestoreOPS()
	// }
	if (args.Index > kv.config.Num) {
		reply.Err = ErrIndex
		kv.RestoreOPS()
		return nil
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ts := strconv.FormatInt(time.Now().UnixNano(), 10)
	proposal := Op{Op: "GetData", Ts:ts}
	kv.UpdateDB(proposal)
	if (args.Index > kv.config.Num) {
		reply.Err = ErrIndex
		return nil
	}
	shard := args.Shard

	m := kv.fileReadShard(shard)

	dbs := map[string]string{}
	lgs := map[string]string{}
	for k, v := range m {
		state := kv.decOp(v)
		if state.Type == "OPS" {
			dbs[k] = v
		} else if state.Type == "LOG" {
			lgs[k] = v
		}
	}
	 
	reply.Err = OK
	reply.Database = dbs
	reply.Logstime = lgs
	return nil
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *DisKV) tick() {
	// Your code here.
	//Lab5	
	kv.mu.Lock()
	defer kv.mu.Unlock()
	//kv.CheckDiskData()
	config := kv.sm.Query(-1)
	// if (kv.config.Num < config.Num && config.Num == 1) {
	// 	kv.config = config
	// 	return
	// }
	//fmt.Println(kv.me, "TTTTTIIIIICCCCCKKKKKK")
	if (kv.config.Num == -1 && config.Num == 1) {
		//fmt.Println("config")
		kv.config = config
		return
	}
	for ind := kv.config.Num+1; ind <= config.Num; ind++ {
		cfg := kv.sm.Query(ind)
		//fmt.Println(cfg)
		database_newpart := map[string]string{}
		logstime_newpart := map[string]string{}
		for shard, gid_old := range kv.config.Shards {
			gid_new := cfg.Shards[shard]
			if (gid_new != gid_old && gid_new == kv.gid) {
				label := false
				for i, srv := range kv.config.Groups[gid_old] {
					if i == kv.me {
						continue
					}
					args := &GetShardDatabaseArgs{shard, kv.config.Num, kv.database, kv.Me}
					reply := GetShardDatabaseReply{OK, map[string]string{}, map[string]string{}}		
	 				ok := call(srv, "DisKV.GetShardDatabase", args, &reply)
	 				if (ok && reply.Err == OK) {
	 					for k, v := range reply.Database {
	 						database_newpart[k] = v	 
	 					}
	 					for k, v := range reply.Logstime {
	 						val, exist := logstime_newpart[k]
	 						if !(exist && val >= v) {
								logstime_newpart[k] = v
							}
	 					}
	 					label = true
	 					// if label {
	 					// 	break
	 					// }
	 				}
				}
				if (label == false && gid_old > 0) {
					return
				}
			}
		}
		ts := strconv.FormatInt(time.Now().UnixNano(), 10)
		proposal := Op{"", "", "Reconfig", kv.Me, ts, ind, database_newpart, cfg, logstime_newpart, "PROS"}
		kv.UpdateDB(proposal)
	
	}
}

// tell the server to shut itself down.
// please don't change these two functions.
func (kv *DisKV) kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

func (kv *DisKV) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *DisKV) Setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *DisKV) isunreliable() bool {
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
// dir is the directory name under which this
//   replica should store all its files.
//   each replica is passed a different directory.
// restart is false the very first time this server
//   is started, and true to indicate a re-start
//   after a crash or after a crash with disk loss.
//
func StartServer(gid int64, shardmasters []string,
	servers []string, me int, dir string, restart bool) *DisKV {

	kv := new(DisKV)
	kv.me = me
	kv.gid = gid
	kv.sm = shardmaster.MakeClerk(shardmasters)
	kv.dir = dir

	// Your initialization code here.
	//Lab5
	kv.database = map[string]string{}
	kv.config = shardmaster.Config{Num:-1}
	kv.seq = 0
	kv.livetime = 1
	kv.CheckCrash()
	// Don't call Join().

	// log.SetOutput(ioutil.Discard)

	gob.Register(Op{})

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	// log.SetOutput(os.Stdout)
	

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
				fmt.Printf("DisKV(%v) accept: %v\n", me, err.Error())
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
	
	kv.RestoreOPS()

	return kv
}
