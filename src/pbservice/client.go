package pbservice

import "viewservice"
import "net/rpc"
import "fmt"

import "crypto/rand"
import "math/big"

//Lab2_PartB
import "time"
import "strconv"

type Clerk struct {
	vs *viewservice.Clerk
	// Your declarations here
	//Lab2_PartB
	View     viewservice.View
	//Test: at-most-once Append
	//cache    map[string]string
	sent     bool
	me       string
}

// this may come in handy.
func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

//Lab2_PartB
func (ck *Clerk) update_view() {
	view, _ := ck.vs.Ping(ck.View.Viewnum)
	ck.View = view
}

func MakeClerk(vshost string, me string) *Clerk {
	ck := new(Clerk)
	ck.vs = viewservice.MakeClerk(me, vshost)
	// Your ck.* initializations here
	//Lab2_PartB
	ck.View = viewservice.View{0, "", ""}
	//ck.cache = make(map[string]string)
	ck.me = strconv.FormatInt(nrand(), 10)
	
	return ck
}


//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will return an
// error after a while if the server is dead.
// don't provide your own time-out mechanism.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
//
func call(srv string, rpcname string,
	args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("unix", srv)
	if errx != nil {
		return false
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

//
// fetch a key's value from the current primary;
// if they key has never been set, return "".
// Get() must keep trying until it either the
// primary replies with the value or the primary
// says the key doesn't exist (has never been Put().
//
func (ck *Clerk) Get(key string) string {

	// Your code here.
	//Lab2_PartB
	//prepare arguments
	if (ck.View.Viewnum == 0) {
		ck.update_view()
	}
	args := &GetArgs{}
	args.Key = key
	var reply GetReply
	//send a RPC
	for i := 0; ; i++ {
		ok := call(ck.View.Primary, "PBServer.Get", args, &reply)
		if (reply.Err == OK && ok == true) {
			break;
		}
		if (ok == false) {
			//fmt.Println("Get error")
		}
		time.Sleep(viewservice.PingInterval)
		ck.update_view()
		//fmt.Println(ck.View.Primary, "OK:", ok, "reply.Err", reply.Err, "View:",ck.View)
	}

	return reply.Value
}

//
// send a Put or Append RPC
//
func (ck *Clerk) PutAppend(key string, value string, op string) {

	// Your code here.
	//Lab2_PartB
	//prepare arguments
	if (ck.View.Viewnum == 0) {
	//Test: Count RPCs to viewserver
		ck.update_view()
	}
	
	args := &PutAppendArgs{}
	args.Key = key
	args.Value = value//ck.cache[key]
	args.Op = op
	//Test: at-most-once Append
	args.Me = ck.me 
	args.Id = strconv.FormatInt(nrand(), 10)
	var reply PutAppendReply
	//send a RPC
	//fmt.Println(args)
	for i := 0; ; i++ {
		//fmt.Println("Put Sever: ",ck.View.Primary)
		ok := call(ck.View.Primary, "PBServer.PutAppend", args, &reply)
		if (reply.Err == OK && ok == true) {
			//fmt.Println("Put or Append OK")
			break
		} 
		if (ok == false) {
			//fmt.Println("Put or Append error!")
		}
		//if (reply.Err != OK) {
			//fmt.Println(reply.Err)
		//}
		time.Sleep(viewservice.PingInterval)
		//Test: Count RPCs to viewserver
		ck.update_view()
	}
	
}

//
// tell the primary to update key's value.
// must keep trying until it succeeds.
//
func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

//
// tell the primary to append to key's value.
// must keep trying until it succeeds.
//
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
