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
//import "strconv"


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
	dones_max  []int
	seq_ins   map[int]bool
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
	n_p        int64   //highest prepare seen
	n_a        int64   //highest accept seen
	v_a        interface{}   //highest accept seen
	status     Fate  //this instance's status
}

//Lab3_PartA
//prepare RPCs' definitions
type PrepareArgs struct {
	Seq       int   //seq
	Num       int64   //seq
}
type PrepareReply struct {
	Seq     int   //seq
	Num     int64   //prepare n, (n_p)
	N_a     int64   //highest accept seen, (n_a)
	V_a     interface{}   //highest accept seen, (v_a)
	OK      bool  //whether prepared
}

//Lab3_PartA
//accept RPCs' definitions
type AcceptArgs struct {
	Seq       int   //seq
	Num       int64   //accept n
	Val       interface{}//value
}
type AcceptReply struct {
	Seq     int   //seq
	Num     int64   //accept n
	OK      bool  //whether accept
}

//Lab3_PartA
//decide RPCs' definitions
type DecideArgs struct {
	Seq    int        //seq
	Num    int64        //decide n
	Val    interface{}//decide value, send to all
	//Database  map[int]interface{}
	Dones_max  []int
	Me        int
}
type DecideReply struct {
	Seq    int   //seq
	//Database  map[int]interface{}
	Dones_max  []int
	OK     bool  //whether success
}

//Lab3_PartA
func (px *Paxos) Proposer(seq int, v interface{}){
	//choose a n bigger than any n seen so far 
	to := 10*time.Millisecond
	//n_try := 0
	for {
		n := time.Now().UnixNano()
		n_prepare_ok := 0
		var n_a int64
		var v_a interface{}
		n_a, v_a = -1, v
		for i1, v1 := range px.peers {
			preargs := &PrepareArgs{seq,n}
			var prereply PrepareReply
			prereply.OK = false
			if (i1 == px.me) {
				px.Prepare(preargs, &prereply)
			} else {
				call(v1, "Paxos.Prepare", preargs, &prereply)
			}
			if (prereply.OK == true) {
				//recv a prepare_ok
				n_prepare_ok++
				if (prereply.N_a > n_a) {
					n_a = prereply.N_a
					v_a = prereply.V_a
				}
			} 
		}
		n_a = n
		
		//recv prepare ok from majority
		n_accept_ok := 0
		if (n_prepare_ok > px.n_servers/2) {
			for i2, v2 := range px.peers {
				accargs := &AcceptArgs{seq, n_a, v_a}
				var accreply AcceptReply
				accreply.OK = false
				if (i2 == px.me) {
					px.Accept(accargs, &accreply)
				} else {
					call(v2, "Paxos.Accept", accargs, &accreply)
				}
				if (accreply.OK == true) {
					n_accept_ok++
			 	}
			}
			//fmt.Println(n_prepare_ok, n_accept_ok)
			//recv accept ok from majority
			if (n_accept_ok > px.n_servers/2) {
				for i3, v3 := range px.peers {
					decargs := &DecideArgs{seq, n_a, v_a, px.dones_max, px.me}//v or v_h
					var decreply DecideReply
					if (i3 == px.me) {
						px.Decide(decargs, &decreply)
 					} else {
 						call(v3, "Paxos.Decide", decargs, &decreply)
  					}
  					//fmt.Println("px.dones_max:",px.dones_max)
  				} 
  				return
			} else {//n_accept_ok < px.n_servers/2
				continue
			}
		} else {//n_accept_ok < px.n_servers/2
			continue
		}
		time.Sleep(to)
		stat, _ := px.Status(seq)
		if (stat == Decided) {//while not decided
			 return
		} else {
			//if (to < 200*time.Millisecond) {
			if (to < 10*time.Second) {
				//to *= 2			
			} else {
				break
			}
			// if (n_try < 100) {
			// 	//n_try++
			// } else {
			// 	break
			// }

		}
	}
}

//Lab3_PartA
func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareReply) error { 
	defer func() {
		//fmt.Println("Warning. Someting is wrong.")
		if err := recover(); err != nil {
			//fmt.Println(err)
		}
	}()
	px.mu.Lock()
	defer px.mu.Unlock()
	seq, n := args.Seq, args.Num
    _, ok := px.instance[seq]
    if (ok == false) {//init
		px.instance[seq] = &State{n, -1, nil, Pending}
    	reply.Seq, reply.Num, reply.N_a = seq, n, px.instance[seq].n_a
    	reply.OK = true
    	//reply = &PrepareReply{seq, n, px.instance[seq].n_a, px.instance[seq].n_p, true}
    } else {
    	if (n > px.instance[seq].n_p) {
			px.instance[seq].n_p = n
			reply.Seq, reply.Num, reply.N_a = seq, px.instance[seq].n_p, px.instance[seq].n_a
    		reply.V_a, reply.OK = px.instance[seq].v_a, true
    		//reply = &PrepareReply{seq, n, px.instance[seq].n_a, px.instance[seq].n_p, true}
		} else {
			reply.Seq, reply.Num, reply.N_a = seq, px.instance[seq].n_p, px.instance[seq].n_a
    		reply.V_a, reply.OK = px.instance[seq].v_a, true
    		//reply = &PrepareReply{seq, px.instance[seq].n_p, px.instance[seq].n_a, px.instance[seq].v_a, false}
		}
    }
	//px.mu.Unlock()
	return nil
}

//Lab3_PartA
func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error { 
	defer func() {
		//fmt.Println("Warning. Someting is wrong.")
		if err := recover(); err != nil {
			//fmt.Println(err)
		}
	}()
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
			reply.OK = false
		}
	}
	px.mu.Unlock()
	return nil
}

//Lab3_PartA
func (px *Paxos) Decide(args *DecideArgs, reply *DecideReply) error {
	defer func() {
		//fmt.Println("Warning. Someting is wrong.")
		if err := recover(); err != nil {
			//fmt.Println(err)
		}
	}()
	px.mu.Lock()
	//fmt.Println("I am server:", px.me, "DECIDE!!!!!!!!!!!!")
	//seq, n, v, Done, database, _, dones_max := args.Seq, args.Num, args.Val, args.Done_max, args.Database, args.Me, args.Dones
	seq, n, v, dones_max, me := args.Seq, args.Num, args.Val, args.Dones_max, args.Me
	px.database[seq] = v
	_, ok := px.instance[seq]
	if (ok == false) {
		px.instance[seq] = &State{n, n, v, Decided}
	} else {
		px.instance[seq].status = Decided
	}
	px.dones_max[me] = dones_max[me]
	reply.Seq, reply.Dones_max, reply.OK = args.Seq, px.dones_max, true
	//fmt.Println(reply)
	//px.delete_logs()
	px.mu.Unlock()
	return nil
}

func (px *Paxos) delete_logs() {
	min := px.dones_max[px.me]
	for _, v := range px.dones_max {
		if (min > v) {
			min = v
		}
	}
	//fmt.Println(px.dones_max)
	for i, ok :=range px.instance {
		if (i <= min && ok.status == Decided) {
			delete(px.database, i)
			delete(px.instance, i)
		}
	}
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
	if (seq > px.dones_max[px.me]) {
		px.dones_max[px.me] = seq
	}
	min := px.dones_max[px.me]
	for _, v := range px.dones_max {
		if (min > v) {
			min = v
		}
	}
	
	for k, ok := range px.instance {
		if (k <= min && ok.status == Decided) {
			delete(px.database, k)
			delete(px.instance, k)
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
	if (len(px.database) == 0) {
		//instance is empty
		//px.mu.Unlock()
		if (px.dones_max[px.me] == -1) {
			return 0
		} else {
			return px.dones_max[px.me]
		}
	}
	max := -1
	for k, _ := range px.database {
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
	px.mu.Lock()
	defer px.mu.Unlock()
	min := px.dones_max[px.me]
	for _, v := range px.dones_max {
		if (min > v) {
			min = v
		}
	}

	px.delete_logs()
	return min + 1
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
	min := px.Min()
	if (seq < min) {
		return Forgotten, nil
	}
	px.mu.Lock()
	defer px.mu.Unlock()
	stat, ok := px.instance[seq]
	if (ok && stat.status == Decided) {
		return Decided, px.database[seq]
	} else {
		return Pending, nil
	}
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
	px.Done_max = -1
	px.dones_max = make([]int, px.n_servers)
	for i, _ := range px.dones_max {
		px.dones_max[i] = -1
	}
	px.seq_ins = make(map[int]bool)
	
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
