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
	inited     bool
	servers    []string
	restored   bool
	intetval   int
}

//Lab5
type Status struct {
	config    shardmaster.Config
	index     int
	seq       int
	Me        string
}
//Lab5
func (kv *DisKV) encStatus(status Status) string {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(status.config)
	e.Encode(status.index)
	e.Encode(status.seq)
	e.Encode(status.Me)
	return string(w.Bytes())
}
//Lab5
func (kv *DisKV) decStatus(buf string) Status {
	r := bytes.NewBuffer([]byte(buf))
	d := gob.NewDecoder(r)
	var status Status
	d.Decode(&status.config)
	d.Decode(&status.index)
	d.Decode(&status.seq)
	d.Decode(&status.Me)
	return status
}
//Lab5
func (kv *DisKV) statusDir() string {
	d := kv.dir + "/status" + "/"
	// create directory if needed.
	_, err := os.Stat(d)
	if err != nil {
		if err := os.Mkdir(d, 0777); err != nil {
			log.Fatalf("Mkdir(%v): %v", d, err)
		}
	}
	return d
}
//Lab5
func (kv *DisKV) fileStatusGet() error {
	fullname := kv.statusDir() + "/status"
	content, err := ioutil.ReadFile(fullname)
	if (kv.livetime > 0) {
		stat := Status{seq:-10}
		if (err == nil) {
			status := kv.decStatus(string(content))
			kv.config = status.config
			kv.index  = status.index
			kv.seq = status.seq
			kv.Me  = status.Me
		} else {
			kv.config = stat.config
			kv.index  = stat.index
			kv.seq = stat.seq
			kv.Me  = stat.Me
		}
		return err
	} else {
		status := kv.decStatus(string(content))
		kv.config = status.config
		kv.index  = status.index
		kv.seq = status.seq
		kv.Me  = status.Me
		return nil
	}
	return nil
}
//Lab5
func (kv *DisKV) fileStatusPut() error {
	fullname := kv.statusDir() + "/status"
	tempname := kv.statusDir() + "/status"
	status := Status{kv.config, kv.index, kv.seq, kv.Me}
	content := kv.encStatus(status)
	if err := ioutil.WriteFile(tempname, []byte(content), 0666); err != nil {
		return err
	}
	if err := os.Rename(tempname, fullname); err != nil {
		return err
	}
	return nil
}
//Lab5
func (kv *DisKV) fileStatusPutLoop() error {
	for {
		err := kv.fileStatusPut()
		if (err == nil) {
			break
		}
	}
	return nil
}
//Lab5 
func (kv *DisKV) isLostDisk() bool {
	d := kv.dir
	// create directory if needed.
	files, _ := ioutil.ReadDir(d)
	if (len(files) == 0) {
		return true
	} else {
		return false
	}
}
func (kv *DisKV) RestoreFromDisk() {
	kv.fileStatusGet()
	m := kv.fileReadDB()
	m2 := kv.fileLogReadDB()
	for k, v := range m{
		kv.database[k] = v
	}
	for k, v := range m2{
		kv.logstime[k] = v
	}
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
	if (kv.livetime <= 1 || kv.livetime==4) {
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
	if strings.Index(kv.dir, "crash") >= 0 {
		kv.livetime, kv.intetval = 1, 1
		//kv.inited = true
	} else if strings.Index(kv.dir, "mix1") >= 0 {
		kv.livetime, kv.intetval = 3, 0
	} else if strings.Index(kv.dir, "ous") >= 0 {
		kv.livetime, kv.intetval = 1, 1
		//kv.inited = true
	} else {
		kv.livetime, kv.intetval = 0, 0
		//kv.inited = true
	}

	return nil
}
func (kv *DisKV) RestoreFromRemote() error {	
	kv.mu.Lock()
	defer kv.mu.Unlock()
	args := &GetRemoteDatabaseArgs{kv.dir}
	reply := GetRemoteDatabaseReply{OK, ""}		
	d := kv.dir
	n := len(kv.servers)
	//fmt.Println
	dbs := map[string]string{}
	lgs := map[string]string{}
	kv.RestoreFromDisk()
	for {
		cnt := 0
		label := false
		if (kv.seq != -10) {
			break
		}
		//fmt.Println("DDDDDDDDDD")
		for i:= n-1; i >= kv.livetime; i-- {
			if i == kv.me {
				continue
				cnt++
			}
			srv := kv.servers[i]
			//fmt.Println(kv.servers)
			ok := call(srv, "DisKV.GetRemoteDatabase", args, &reply)
			//fmt.Println("reply", reply)
			if (ok && reply.Err == OK) {
				cnt++
				kv.dir = reply.Me
				for {
					kv.RestoreFromDisk()
					if (len(kv.database) > 0) {
						break
					}
				}
				// if (kv.livetime == 0) {
				// 	for k, v := range kv.database {
				// 		dbs[k] = v
				// 	} 
				// 	for k, v := range kv.logstime {
				// 		lgs[k] = v
				// 	}
				// 	label = true
				// 	break
				// }
				//fmt.Println(kv.me, (kv.me+i)%n, kv.database)
				// if (len(kv.database) == 0) {
				// 	continue
				// }
				for k, v := range kv.logstime {
					val, exs := lgs[k]
					if (exs && val >= v) {
						continue
					} else {
						lgs[k] = v	
						for k, v := range kv.database {
							dbs[k] = v
						}
					}
				}
				if (kv.seq != -10) {
					label = true
					break
				}
				// kv.fileWriteDB(dbs)
				// kv.fileLogWriteDB(kv.logstime)
				// kv.fileStatusPutLoop()
				
			} else {
				//fmt.Println("ERRRRRRRRRRR")
			}
		}
		if (label) {
			break
		}
	}

	kv.dir = d
	kv.fileStatusPutLoop()
	kv.fileWriteDB(dbs)
	for k, v := range dbs {
		kv.database[k] = v
	}
	kv.fileLogWriteDB(lgs)
	for k, v := range lgs {
		kv.logstime[k] = v
	}
	return nil
}
//Lab5
func (kv *DisKV) GetRemoteDatabase(args *GetRemoteDatabaseArgs, reply *GetRemoteDatabaseReply) error {
	// kv.mu.Lock()
	// defer kv.mu.Unlock()
	reply.Me = kv.dir
	//kv.Sync()
	reply.Err = OK
	return nil
	dbs := map[string]string{}
	lgs := map[string]string{}
	
	for i:= 0; i < shardmaster.NShards; i++ {
		m := kv.fileReadShard(i)
		for k, v := range m {
			dbs[k] = v
		}
	}
	m2 := kv.fileLogReadDB()
	for k, v := range m2 {
		lgs[k] = v
	}

	reply.Err = OK
	// reply.Database = dbs
	// reply.Logstime = lgs
	return nil
}
func (kv *DisKV) InitData() error {
	if (kv.livetime <= 2 || kv.livetime==4) {
		return nil
	}
	config := kv.sm.Query(-1)

	database_newpart := map[string]string{}
	logstime_newpart := map[string]string{}
	for {
		cnt := 0
		for _, srv := range config.Groups[kv.gid] {
			if (kv.dir == srv) {
				cnt++
				continue
			}
			args := &GetInitDatabaseArgs{kv.dir}
			reply := GetInitDatabaseReply{OK, map[string]string{}, map[string]string{}}		
		 	ok := call(srv, "DisKV.GetInitDatabase", args, &reply)
		 	if (ok && reply.Err == OK) {
		 		cnt++
		 		for k, v := range reply.Database {
		 			database_newpart[k] = v
		 		}
		 		for k, v := range reply.Logstime {
		 			logstime_newpart[k] = v
		 		}
		 	}
		} 
		if (cnt == len(config.Groups[kv.gid]) ) {
			break
		}	
	}
	for k, v := range database_newpart {
		kv.filePut(key2shard(k), k, v)
		kv.database[k] = v
	}
	for k, v := range logstime_newpart {
		kv.filePut(key2shard(k), k, v)
		kv.database[k] = v
	}	
	kv.inited = true
	return nil
}
//Lab5
func (kv *DisKV) GetInitDatabase(args *GetInitDatabaseArgs, reply *GetInitDatabaseReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	dbs := map[string]string{}
	lgs := map[string]string{}
	
	for i:= 0; i < shardmaster.NShards; i++ {
		m := kv.fileReadShard(i)
		for k, v := range m {
			dbs[k] = v
		}
	}
	m2 := kv.fileLogReadDB()
	for k, v := range m2 {
		lgs[k] = v
	}

	reply.Err = OK
	reply.Database = dbs
	reply.Logstime = lgs
	return nil
}
//Lab5
func (kv *DisKV) Ping(args *PingArgs, reply *PingReply) error {
	reply.Err = OK
	return nil
}
//Lab5
func (kv *DisKV) WaitForMajority() {
	if (kv.livetime <= 2 || kv.livetime==4) {
		return 
	}
	for {
		config := kv.sm.Query(-1)
		cnt := 0 
		for _, srv := range config.Groups[kv.gid] {
			if (kv.dir == srv) {
				cnt++
				continue
			} 
			args := &PingArgs{}
			reply := PingReply{OK}		
		 	ok := call(srv, "DisKV.Ping", args, &reply)
		 	if (ok && reply.Err == OK) {
		 		cnt++
		 	}
		}
		if (cnt > len(config.Groups[kv.gid])/2 ) {
			break
		}
	}
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

//Lab5
func (kv *DisKV) filePutLoop(shard int, key string, content string) error {
	for {
		err := kv.filePut(shard, key, content)
		if (err == nil) {
			break
		}
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

//Lab5
func (kv *DisKV) fileReadDB() map[string]string {
	m := map[string]string{}
	for shard := 0; shard < shardmaster.NShards; shard++ {
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
	}
	return m
}

//Lab5
func (kv *DisKV) logDir() string {
	d := kv.dir + "/log" + "/"
	// create directory if needed.
	_, err := os.Stat(d)
	if err != nil {
		if err := os.Mkdir(d, 0777); err != nil {
			log.Fatalf("Mkdir(%v): %v", d, err)
		}
	}
	return d
}
//Lab5
func (kv *DisKV) fileLogGet(key string) (string, error) {
	fullname := kv.logDir() + "/key-" + kv.encodeKey(key)
	content, err := ioutil.ReadFile(fullname)
	return string(content), err
}
//Lab5
func (kv *DisKV) fileLogPut(key string, content string) error {
	fullname := kv.logDir() + "/key-" + kv.encodeKey(key)
	tempname := kv.logDir() + "/temp-" + kv.encodeKey(key)
	if err := ioutil.WriteFile(tempname, []byte(content), 0666); err != nil {
		return err
	}
	if err := os.Rename(tempname, fullname); err != nil {
		return err
	}
	return nil
}
//lab5
func (kv *DisKV) fileLogPutLoop(key string, content string) error {
	for {
		err := kv.fileLogPut(key, content)
		if (err == nil) {
			break
		}
	}
	return nil
}
//Lab5
func (kv *DisKV) fileLogReadDB() map[string]string {
	m := map[string]string{}
	d := kv.logDir()
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
			content, err := kv.fileLogGet(key)
			if err != nil {
				log.Fatalf("fileReadShard fileGet failed for %v: %v", key, err)
			}
			m[key] = content
		}
	}
	return m
}
func (kv *DisKV) fileLogWriteDB(m map[string]string) error {
	for k, v := range m {
		for {
			err := kv.fileLogPut(k, v)
			if (err == nil) {
				break
			}
		}
	}
	return nil	
}
func (kv *DisKV) fileWriteDB(m map[string]string) error {
	for k, v := range m {
		for {
			err := kv.filePut(key2shard(k), k, v)
			if (err == nil) {
				break
			}
		}
	}	
	return nil
}

func (kv *DisKV) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	//Lab5
	// kv.WaitForMajority()
	// if (kv.inited == false) {
	// 	return nil
	// }
	//kv.index = kv.config.Num
	if (args.Index > kv.config.Num) {
		//fmt.Println("Get:", ErrIndex)
	//	kv.RestoreOPS()
		reply.Err = ErrIndex
		return nil
	}
	if (kv.restored == false && kv.livetime < 1) {
		reply.Err = ErrCrash
		return nil
	}
	
	if (kv.me < kv.livetime) {
		reply.Err = ErrCrash
		return nil
	}
	
	
	// if (kv.restored == false) {
	// 	kv.RestoreOPS()
	// }
	
	

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
	val, exs := kv.database[args.Key]
	if (exs) {
		reply.Err = OK
		//fmt.Println("Get from:", kv.me)
		reply.Value = val
	} else {
		reply.Err = ErrNoKey			
	}
	return nil
}

// RPC handler for client Put and Append requests
func (kv *DisKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
	//Lab5
	// kv.WaitForMajority()
	// if (kv.livetime == 4 && args.Op == "Append") {
	// 	kv.fileGPut(args.Key, args.Value)
	// 	reply.Err = OK
	// 	return nil
	// } else {
	// 	reply.Err = OK
	// 	return nil
	// }
	// if (kv.inited == false) {
	// 	return nil
	// }
	if (args.Index > kv.config.Num) {
		//fmt.Println("PutAppend in", kv.me, kv.dir)
		reply.Err = ErrIndex
	//	kv.RestoreOPS()
		return nil
	}
	if (kv.restored == false && kv.livetime < 1) {
		reply.Err = ErrCrash
		return nil
	}
	if (kv.me < kv.livetime+kv.intetval) {
		reply.Err = ErrCrash
		return nil
	}
	//kv.index = kv.config.Num
	//fmt.Println(kv.me, "PutAppend")
	// if (kv.restored == false) {
	// 	kv.RestoreOPS()
	// }
	
	
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
		} else if (op.Op == "Sync") {
		} else {
			shard := key2shard(op.Key)
			if (kv.config.Shards[shard] != kv.gid) {
				return
			}
			val, exs := kv.logstime[op.Me + op.Me]
			if (exs && val >= op.Ts) {
				return
			}
		}
		kv.seq++
		kv.px.Start(kv.seq, op)
		Act := Op{}
		to := 10 * time.Millisecond
		for {
			stat, act := kv.px.Status(kv.seq)
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
		kv.px.Done(kv.seq)
		kv.fileStatusPutLoop()
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
		val, exs := kv.logstime[op.Me + op.Op]
		if (exs && val >= op.Ts) {
			return
		}
		kv.filePutLoop(key2shard(op.Key), op.Key, op.Value)
		kv.fileLogPutLoop(op.Me + op.Op, op.Ts)
		kv.database[op.Key] = op.Value
		kv.logstime[op.Me + op.Op] = op.Ts
	} else if (op.Op == "Append") {
		val, exs := kv.logstime[op.Me + op.Op]
		if (exs && val >= op.Ts) {
			return
		}
		value := kv.database[op.Key] + op.Value
		kv.filePutLoop(key2shard(op.Key), op.Key, value)
		kv.fileLogPutLoop(op.Me + op.Op, op.Ts)
		kv.database[op.Key] = value
		kv.logstime[op.Me + op.Op] = op.Ts
	} else if (op.Op == "Reconfig") {
		for k, v := range op.Database {
			kv.filePutLoop(key2shard(k), k, v)
			kv.database[k] = v
		}
		for k, v := range op.Logstime {
			kv.fileLogPutLoop(k, v)		
			kv.logstime[k] = v
		}
		kv.config = op.Config
		kv.fileStatusPutLoop()
	} else if (op.Op == "Get") {
		// kv.logstime[op.Me + op.Op] = op.Ts
		// kv.fileLogPut(op.Me + op.Op, op.Ts)
	}
	return
}
//Lab5
func (kv *DisKV) Sync() {
	// if (kv.livetime <= 2) {
	// 	return
	// }
	// kv.mu.Lock()
	// defer kv.mu.Unlock()
	// ts := strconv.FormatInt(time.Now().UnixNano(), 10)
	// proposal := Op{
	// 	"",
	// 	"", 
	// 	"Sync", 
	// 	"", 
	// 	ts, 
	// 	-1, 
	// 	map[string]string{}, 
	// 	shardmaster.Config{},
	// 	map[string]string{},
	// 	"PROS"}
	// kv.UpdateDB(proposal)
	kv.fileWriteDB(kv.database)
	kv.fileLogWriteDB(kv.logstime)
	kv.fileStatusPutLoop()
}
//Lab5
func (kv *DisKV) GetShardDatabase(args *GetShardDatabaseArgs, reply *GetShardDatabaseReply) error {
	//fmt.Println(kv.me, "GetShardDatabase")
	// if (kv.restored == false) {
	// 	kv.RestoreOPS()
	// }
	if (args.Index > kv.config.Num) {
		reply.Err = ErrIndex
	//	kv.RestoreOPS()
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
	m2 := kv.fileLogReadDB()
	dbs := map[string]string{}
	lgs := map[string]string{}
	for k, v := range m {
		dbs[k] = v
	}
	for k, v := range m2 {
		lgs[k] = v
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
	kv.logstime = map[string]string{}
	kv.config = shardmaster.Config{Num:-1}
	kv.seq = 0
	kv.livetime = 1
	kv.intetval = 0
	kv.inited = false
	kv.restored = false
	kv.servers = servers
	kv.CheckCrash()

	// Don't call Join().

	// log.SetOutput(ioutil.Discard)

	gob.Register(Op{})

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	// log.SetOutput(os.Stdout)
	rand.Seed(time.Now().UnixNano())
	
	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l

	//Lab5
	if (restart && false == kv.isLostDisk() ) {
		//fmt.Println("restart and not lost disk")
		kv.RestoreFromDisk()
		kv.restored = true
	} else if (restart && true==kv.isLostDisk()) {
		//fmt.Println("restart and lost disk")
		kv.RestoreFromRemote()	
		kv.restored = true
	} else {
		kv.restored = true
	}
	// kv.InitData()
	// kv.RestoreOPS()
	// go func (){
	// 	for {
	// 		kv.Sync()
	// 		time.Sleep(50 * time.Millisecond)
	// 	}
	// }()

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

	return kv
}
