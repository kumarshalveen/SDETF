package kvpaxos

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
	TimeOut  = "TimeOut"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	//Lab3_PartB
	Me    string //the id that stand for a uniq client
	Id    string //the id that stand for a uniq op
	Ts    int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	//Lab3_PartB
	Op    string //"Get"
	Me    string //the id that stand for a uniq client
	Id    string //the id that stand for a uniq op
	Ts    int64
}

type GetReply struct {
	Err   Err
	Value string
}


