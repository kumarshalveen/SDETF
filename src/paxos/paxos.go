package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (Fate, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import "net"
import "net/rpc"
import "log"

import "os"
import "syscall"
import "sync"
import "sync/atomic"
import "fmt"
import "math/rand"

//Lab3_PartA
import "time"

//Lab3_PartB
import "encoding/gob"

// px.Status() return values, indicating
// whether an agreement has been decided,
// or Paxos has not yet reached agreement,
// or it was agreed but forgotten (i.e. < Min()).
type Fate int

const (
	Decided   Fate = iota + 1
	Pending        // not yet decided.
	Forgotten      // decided but forgotten.
)

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	rpcCount   int32 // for testing
	peers      []string
	me         int // index into peers[]


	// Your data here.
	//Lab3_PartA
	n_servers  int   //number of servers
	database   map[int]interface{} //db, in fact, like a log db
	instance   map[int]*State
	done       map[int]bool
	Done_max   int
	servers_done map[string]int    //servers that have done
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

//Lab3_PartA
//struct state definitions
type State struct {
	n_p        int   //highest prepare seen
	n_a        int   //highest accept seen
	v_a        interface{}   //highest accept seen
	status     Fate  //this instance's status
}

//Lab3_PartA
//prepare RPCs' definitions
type PrepareArgs struct {
	Seq       int   //seq
	Num       int   //seq
}
type PrepareReply struct {
	Seq     int   //seq
	Num     int   //prepare n, (n_p)
	N_a     int   //highest accept seen, (n_a)
	V_a     interface{}   //highest accept seen, (v_a)
	OK      bool  //whether prepared
}

//Lab3_PartA
//accept RPCs' definitions
type AcceptArgs struct {
	Seq       int   //seq
	Num       int   //accept n
	Val       interface{}//value
}
type AcceptReply struct {
	Seq     int   //seq
	Num     int   //accept n
	OK      bool  //whether accept
}

//Lab3_PartA
//decide RPCs' definitions
type DecideArgs struct {
	Seq    int        //seq
	Num    int        //decide n
	Val    interface{}//decide value, send to all
	Done_max   int    //the Done number
	//Lab3_PartB
	Servers_Done map[string]int //servers done
	Database     map[int]interface{}//database
	//Instance     map[int]*State     //instance
}
type DecideReply struct {
	Seq    int   //seq
	OK     bool  //whether success
}

//Lab3_PartB
type DoneArgs struct {
	Seq       int
}
type DoneReply struct {
	OK        bool
}
type UpdateDBArgs struct {
	SeqMax    int //maxSeq
	Server    string //server name
}
type UpdateDBReply struct {
	Database  map[int]interface{} //db to update
	OK        bool                //whether is ok
}

//Lab3_PartA
func (px *Paxos) Proposer(seq int, v interface{}){
	//choose a n bigger than any n seen so far 
	n := 0
	to := 10*time.Millisecond
	for {
		preargs := &PrepareArgs{seq,n}
		var prereply PrepareReply
		n_prepare_ok := 0
		var v_h interface{}//v_a with highest n_a
		n_h := -1         //highest n_a
		for i1, v := range px.peers {
			if (i1 == px.me) {
				px.self_prepare(preargs, &prereply)
			} else {
				ok := call(v, "Paxos.Prepare", preargs, &prereply)
				if (ok == false) {//net failed, retry
					//fmt.Println("recv true:", prereply)
					//break
					continue
				}
			}
			if (prereply.OK == true) {
				//recv a prepare_ok
				n_prepare_ok++
				if (prereply.N_a > n_h) {
					n_h = prereply.N_a
					v_h = prereply.V_a
				}
				if (n < n_h) {
					n = n_h
				}
			} else {//choose a n bigger than any n seen so far
				if (n < prereply.Num) { 
					n = prereply.Num
				}
			}
		} 
		//recv prepare ok from majority
		n_accept_ok := 0
		if (n_prepare_ok > px.n_servers/2) {
			accargs := &AcceptArgs{seq, n, v_h}
			var accreply AcceptReply
			for i2, v2 := range px.peers {
				if (i2 == px.me) {
					px.self_accept(accargs, &accreply)
				} else {
					for {
						ok2 := call(v2, "Paxos.Accept", accargs, &accreply)
						if (ok2 == false) {//retry, if net failed
							//fmt.Println(accreply)
							//break
							continue
			 			} else {
			 				break
			 			}
			 		}
				}
				if (accreply.OK == true) {
					n_accept_ok++
			 	}
			}
			
			//recv accept ok from majority
			if (n_accept_ok > px.n_servers/2) {
				decargs := &DecideArgs{seq, n, v, px.Done_max, px.servers_done, px.database}//v or v_h
				var decreply DecideReply
				for i3, v3 := range px.peers {
					if (i3 == px.me) {
						px.self_decide(decargs, &decreply)
 					} else {
 						for {
							ok3 := call(v3, "Paxos.Decide", decargs, &decreply)
							if (ok3 == true) {//retry, if net failed
 								//fmt.Println(decreply)
								break
  							}
  						}
  					}
  				} 
			} else {
				//continue
			}
		} else {
			//continue
		}
		time.Sleep(to)
		stat, _ := px.Status(seq)
		if (stat == Decided) {//while not decided
			break
		} else {
			if (stat == Pending) {
				//fmt.Println("Pending")
			} else {
				//fmt.Println("forgotten")
				break
			}
			//fmt.Println(n_prepare_ok, n_accept_ok, px.n_servers)
			if (to < 2*time.Second) {
				n++
			} else {
				break
			}

		}
	}
}

//Lab3_PartA
func (px *Paxos) self_prepare(args *PrepareArgs, reply *PrepareReply) {
	px.mu.Lock()
	seq, n := args.Seq, args.Num
    _, ok := px.instance[seq]
    if (ok == false) {//init
		px.instance[seq] = &State{n, -1, -1, Pending}
    	reply.Seq, reply.Num, reply.N_a = seq, n, px.instance[seq].n_a
    	reply.V_a, reply.OK = px.instance[seq].n_p, true
    	//reply = &PrepareReply{seq, n, px.instance[seq].n_a, px.instance[seq].n_p, true}
    } else {
    	if (n > px.instance[seq].n_p) {
			px.instance[seq].n_p = n
			reply.Seq, reply.Num, reply.N_a = seq, n, px.instance[seq].n_a
    		reply.V_a, reply.OK = px.instance[seq].n_p, true
    		//reply = &PrepareReply{seq, n, px.instance[seq].n_a, px.instance[seq].n_p, true}
		} else {
			reply.Seq, reply.Num, reply.N_a = seq, n, px.instance[seq].n_a
    		reply.V_a, reply.OK = px.instance[seq].n_p, true
    		//reply = &PrepareReply{seq, px.instance[seq].n_p, px.instance[seq].n_a, px.instance[seq].n_p, false}
		}
    }
	px.mu.Unlock()
	return
}

//Lab3_PartA
func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareReply) error { 
	px.mu.Lock()
	seq, n := args.Seq, args.Num
    _, ok := px.instance[seq]
    if (ok == false) {//init
		px.instance[seq] = &State{n, -1, -1, Pending}
    	reply.Seq, reply.Num, reply.N_a = seq, n, px.instance[seq].n_a
    	reply.V_a, reply.OK = px.instance[seq].n_p, true
    	//reply = &PrepareReply{seq, n, px.instance[seq].n_a, px.instance[seq].n_p, true}
    } else {
    	if (n > px.instance[seq].n_p) {
			px.instance[seq].n_p = n
			reply.Seq, reply.Num, reply.N_a = seq, n, px.instance[seq].n_a
    		reply.V_a, reply.OK = px.instance[seq].n_p, true
    		//reply = &PrepareReply{seq, n, px.instance[seq].n_a, px.instance[seq].n_p, true}
		} else {
			reply.Seq, reply.Num, reply.N_a = seq, n, px.instance[seq].n_a
    		reply.V_a, reply.OK = px.instance[seq].n_p, true
    		//reply = &PrepareReply{seq, px.instance[seq].n_p, px.instance[seq].n_a, px.instance[seq].n_p, false}
		}
    }
	px.mu.Unlock()
	return nil
}

//Lab3_PartA
func (px *Paxos) self_accept(args *AcceptArgs, reply *AcceptReply) { 
	px.mu.Lock()
	seq, n, v := args.Seq, args.Num, args.Val
	_, ok := px.instance[seq]
	if (ok == false) {
		px.instance[seq] = &State{n, n, v, Pending}
		reply.Seq, reply.Num, reply.OK = seq, n, true
	} else {
		if (n >= px.instance[seq].n_p) {
			px.instance[seq].n_p = n
			px.instance[seq].n_a = n
			px.instance[seq].v_a = v
			reply.Seq, reply.Num, reply.OK = seq, n, true
		} else {
			reply.Seq, reply.Num, reply.OK = seq, n, false
		}
	}
	px.mu.Unlock()
	return
}

//Lab3_PartA
func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error { 
	px.mu.Lock()
	seq, n, v := args.Seq, args.Num, args.Val
	_, ok := px.instance[seq]
	if (ok == false) {
		px.instance[seq] = &State{n, n, v, Pending}
		reply.Seq, reply.Num, reply.OK = seq, n, true
	} else {
		if (n >= px.instance[seq].n_p) {
			px.instance[seq].n_p = n
			px.instance[seq].n_a = n
			px.instance[seq].v_a = v
			reply.Seq, reply.Num, reply.OK = seq, n, true
		} else {
			reply.Seq, reply.Num, reply.OK = seq, n, false
		}
	}
	px.mu.Unlock()
	return nil
}

//Lab3_PartA
func (px *Paxos) self_decide(args *DecideArgs, reply *DecideReply) {
	px.mu.Lock() 
	//fmt.Println("I am server:", px.me, "DECIDE!!!!!!!!!!!!")
	seq, n, v, Done := args.Seq, args.Num, args.Val, args.Done_max
	px.database[seq] = v
	_, ok := px.instance[seq]
	if (ok == false) {
		px.instance[seq] = &State{n, n, v, Decided}
	} else {
		px.instance[seq].status = Decided
	}
	px.done[seq] = true
	if (Done < px.Done_max) {
		px.Done_max = Done
	}
	for se, va := range args.Database {//get new logs
		_, is_in := px.database[se]
		if (is_in == false) {
			px.database[se] = va
		}
	}
	// for se2, va2 := range args.Instance {//get new logs
	// 	_, is_in2 := px.instance[se2]
	// 	if (is_in2 == false) {
	// 		px.instance[se2] = va2
	// 	}
	// }
	px.servers_done = args.Servers_Done//Lab3_PartB
	px.proc_done()                     //Lab3_PartB
	reply.Seq, reply.OK = args.Seq, true
	//fmt.Println(reply)
	px.mu.Unlock()
	return
}

//Lab3_PartA
func (px *Paxos) Decide(args *DecideArgs, reply *DecideReply) error {
	px.mu.Lock()
	//fmt.Println("I am server:", px.me, "DECIDE!!!!!!!!!!!!")
	seq, n, v, Done := args.Seq, args.Num, args.Val, args.Done_max
	px.database[seq] = v
	_, ok := px.instance[seq]
	if (ok == false) {
		px.instance[seq] = &State{n, n, v, Decided}
	} else {
		px.instance[seq].status = Decided
	}
	px.done[seq] = true
	if (Done < px.Done_max) {
		px.Done_max = Done
	}
	for se, va := range args.Database {//get new logs
		_, is_in := px.database[se]
		if (is_in == false) {
			px.database[se] = va
		}
	}
	// for se2, va2 := range args.Instance {//get new logs
	// 	_, is_in2 := px.instance[se2]
	// 	if (is_in2 == false) {
	// 		px.instance[se2] = va2
	// 	}
	// }
	px.servers_done = args.Servers_Done//Lab3_PartB
	px.proc_done()                     //Lab3_PartB   
	reply.Seq, reply.OK = args.Seq, true
	//fmt.Println(reply)
	px.mu.Unlock()
	return nil
}

//Lab3_PartB
func (px *Paxos) proc_done()  {
	cnt := len(px.servers_done)
	if (cnt >= len(px.peers)) {
		min := 99999999
		for _, v := range px.servers_done {
			//fmt.Println(k, v, px.me)
			if (v < min) {
				min = v
			}
		}
		px.Done(min - cnt*2)
	}
	return 
}

//Lab3_PartB
func (px *Paxos) UpdateDB(args *UpdateDBArgs, reply *UpdateDBReply) error {
	px.mu.Lock()
	px.servers_done[args.Server] = args.SeqMax
	reply.Database, reply.OK = px.database, true
	px.mu.Unlock()
	return nil
}

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v. Athour: lmhtq
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
	// Your code here.
	//Lab3_PartA
	go func() {
		if (seq <= px.Done_max) {
			return
		}
		px.Proposer(seq, v)
	}()
	return
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	// Your code here.
	//Lab3_PartA
	//px.mu.Lock()
	if (seq > px.Done_max) {
		px.Done_max = seq
	} else {
		//px.mu.Unlock()
		return
	}
	for k, _ := range px.database {
		if (k < seq) {
			//fmt.Println("DELETE...")
			delete(px.database, k)
		}
	}
	//px.mu.Unlock()
	return
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	// Your code here.
	//Lab_PartA
	//px.mu.Lock()
	if (len(px.instance) == 0) {
		//instance is empty
		//px.mu.Unlock()
		return px.Done_max + 1
	}
	max := -1
	for k, _ := range px.instance {
		if (max < k) {
			max = k
		}
	}
	//px.mu.Unlock()
	return max
	//return 0
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
//
func (px *Paxos) Min() int {
	// You code here.
	//Lab3_PartA
	//px.mu.Lock()
	if (len(px.done) == 0) {
		//px.mu.Unlock()
		return px.Done_max + 1
	}
	//min := 9999999//in fact, it is the max seq which has done
	//fmt.Println(px.done)
	//fmt.Println(px.instance)
	//fmt.Println(px.Done_max)
	/*for k, _ := range px.instance {
		if (k < min) {
			min = k
		}
	}*/
	min := px.Done_max + 1
	//px.mu.Unlock()
	return min
	//return 0
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (Fate, interface{}) {
	// Your code here.
	//px.mu.Lock()
	stat, ok := px.instance[seq]
	if (seq < px.Min()) {
		//px.mu.Unlock()
		return Forgotten, nil
	}
	if (ok == false) {
		//px.mu.Unlock()
		return Pending, nil
	}
	if (stat.status == Decided) {
		//px.mu.Unlock()
		return Decided, nil
	}
	//px.mu.Unlock()
	return Pending, nil
}



//
// tell the peer to shut itself down.
// for testing.
// please do not change these two functions.
//
func (px *Paxos) Kill() {
	atomic.StoreInt32(&px.dead, 1)
	if px.l != nil {
		px.l.Close()
	}
}

//
// has this peer been asked to shut down?
//
func (px *Paxos) isdead() bool {
	return atomic.LoadInt32(&px.dead) != 0
}

// please do not change these two functions.
func (px *Paxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&px.unreliable, 1)
	} else {
		atomic.StoreInt32(&px.unreliable, 0)
	}
}

func (px *Paxos) isunreliable() bool {
	return atomic.LoadInt32(&px.unreliable) != 0
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me


	// Your initialization code here.
	//Lab3_PartA
	px.instance = make(map[int]*State)
	px.database = make(map[int]interface{})
	px.n_servers = len(peers)
	px.done = make(map[int]bool)
	px.Done_max = -1
	px.servers_done = make(map[string]int)
	gob.Register(State{})

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.isdead() == false {
				conn, err := px.l.Accept()
				if err == nil && px.isdead() == false {
					if px.isunreliable() && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.isunreliable() && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					} else {
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.isdead() == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}


	return px
}
