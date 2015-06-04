package shardkv

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
	//Lab4_PartB
	ErrIndex      = "ErrIndex"
)

type Err string

type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	//Lab4_PartB
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
	
	//Lab4_PartA
	Op    string  // "Get"
	Me    string  // identidy the client
	Ts    string  // the operateion's timestamp
	Index int     // the index of the config
}

type GetReply struct {
	Err   Err
	Value string
}

//Lab4_PartB
type GetShardDatabaseArgs struct {
	GID   int64 //the group id

}

//Lab4_PartB
type  GetShardDatabaseReply struct {
	Err      Err                // error info
	Database map[string]string  //the dataabse

}