package diskv

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK            = "OK"
	ErrNoKey      = "ErrNoKey"
	ErrWrongGroup = "ErrWrongGroup"
	//Lab5
	ErrIndex      = "ErrIndex"
	ErrCrash      = "ErrCrash"
)

type Err string

type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	//Lab5
	Me    string // identify the client
	Ts    string // the operation's timestamp
	Index int    // the index of the config, that's the view number    
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.

	//Lab5
	Op    string  // "Get"
	Me    string  // identidy the client
	Ts    string  // the operateion's timestamp
	Index int     // the index of the config
}

type GetReply struct {
	Err   Err
	Value string
}

//Lab5
type GetShardDatabaseArgs struct {
	Shard    int             // the group id
	Index    int
	Database map[string]string // the database
	Me       string
}

//Lab5
type  GetShardDatabaseReply struct {
	Err      Err                // error info
	Database map[string]string  // the dataabse
	Logstime map[string]string
	
}

//Lab5
type CheckDiskDataArgs struct {

}

type CheckDiskDataReply struct {
	Err      Err
}

//Lab5
type GetInitDatabaseArgs struct {
	Me       string
}

//Lab5
type  GetInitDatabaseReply struct {
	Err      Err                // error info
	Database map[string]string  // the dataabse
	Logstime map[string]string
	
}
//Lab5
type GetRemoteDatabaseArgs struct {
	Me       string
}

//Lab5
type  GetRemoteDatabaseReply struct {
	Err      Err                // error info
	// Database map[string]string  // the dataabse
	// Logstime map[string]string
	Me      string
}

//Lab5
type PingArgs struct {
	Key     string
	Me      string
}
//Lab5
type PingReply struct {
	Err    Err
}