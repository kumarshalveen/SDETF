package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"
import "sync/atomic"

type ViewServer struct {
	mu       sync.Mutex
	l        net.Listener
	dead     int32 // for testing
	rpccount int32 // for testing
	me       string


	// Your declarations here.
	//Lab2_PartA
	View           View
	newView        View
	servers        map[string]int
	server_idle    map[string]int
	server_ack     map[string]int
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

	// Your code here.
	//Lab2_PartA
	//fmt.Println(args)
	vs.servers[args.Me] = DeadPings
	if (args.Viewnum > 0 && args.Viewnum == vs.View.Viewnum) {
		vs.server_ack[args.Me] = 1
	}
	if (args.Viewnum == 0 && vs.newView.Viewnum == 0 && 
		vs.newView.Primary == "" && vs.newView.Backup == "") {
		//Test: First primary
		vs.mu.Lock()
		vs.newView.Viewnum += 1
		vs.newView.Primary = args.Me
		vs.server_idle[args.Me] = 0
		vs.mu.Unlock()		
		//fmt.Println(reply)
	} else if (args.Viewnum == 0 && vs.newView.Viewnum > 0 &&
		vs.newView.Primary == args.Me) {
		//Test: Restarted primary treated as dead
		//Test: Dead backup is removed from view
		//vs.View.Primary = ""
		vs.make_backup_to_primary()
		//reply.View = vs.View
	} else if (vs.newView.Primary != "" && vs.newView.Backup != "" &&
		vs.newView.Primary != args.Me && vs.newView.Backup != args.Me ) {
		//Test: Idle third server becomes backup if primary fails
		vs.mu.Lock()
		vs.server_idle[args.Me] = 1
		vs.mu.Unlock()
		//reply.View = vs.View
	} else if (args.Viewnum == 0 && vs.View.Primary != "" &&
	 vs.View.Primary != args.Me && vs.View.Backup == "") {
	 	//Test: First backup
	 	vs.mu.Lock()
		vs.newView.Viewnum += 1
		vs.newView.Backup = args.Me
		vs.server_idle[args.Me] = 0
		vs.mu.Unlock()	
	} else if (vs.newView.Primary != "" && vs.newView.Primary != args.Me &&
	 vs.newView.Backup == "") {
	 	//Test: First backup
		//fmt.Println(args)
		vs.mu.Lock()
		vs.newView.Viewnum += 1
		vs.newView.Backup = args.Me
		vs.server_idle[args.Me] = 0
		vs.mu.Unlock()
	}
	vs.mu.Lock()
	if (args.Viewnum == vs.View.Viewnum && args.Viewnum == 0) {
		vs.View = vs.newView
		vs.server_ack[args.Me] = 0
	} else if (args.Viewnum < vs.View.Viewnum) {
		vs.View = vs.newView
		vs.server_ack[args.Me] = 0
	} else if (args.Viewnum == vs.View.Viewnum && vs.server_ack[args.Me] == 1) {
		if (vs.View.Viewnum < vs.newView.Viewnum) {
			vs.View = vs.newView
			vs.server_ack[args.Me] = 0
		}		
	}
	vs.mu.Unlock()
	reply.View = vs.View
	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	//Lab2_PartA
	reply.View = vs.View

	return nil
}


//Lab2_PartA
//Test: Backup takes over if primary fails
//Test: Restarted server becomes backup
//Test: Idle third server becomes backup if primary fails
//Test: Restarted primary treated as dead
func (vs *ViewServer) make_backup_to_primary () {
	//time.Sleep(time.Millisecond*60)
	vs.mu.Lock()
	vs.newView.Viewnum += 1
	vs.newView.Primary = vs.View.Backup
	vs.newView.Backup = ""
	for k, v := range vs.server_idle {
		if (v == 1) {
			vs.newView.Backup = k
			vs.server_idle[k] = 0
		}
	}
	vs.mu.Unlock()
}


//Lab2_PartA
func (vs *ViewServer) remove_dead_backup() {
	vs.mu.Lock()
	vs.newView.Viewnum += 1
	vs.newView.Backup = ""
	vs.mu.Unlock()
}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {

	// Your code here.
	//Lab2_PartA
	for k, v := range vs.servers {
		vs.servers[k] = v - 1
		if (vs.servers[k] == 0) {
			if (k == vs.View.Primary) {
				//Lab2_PartB
				if (vs.server_ack[vs.View.Primary] == 1) {
					vs.make_backup_to_primary()//Lab2_PartA
					vs.View = vs.newView
				} 				
			} else if (k == vs.View.Backup) {
				vs.remove_dead_backup()
			}
			
		}
	}
}

//
// tell the server to shut itself down.
// for testing.
// please don't change these two functions.
//
func (vs *ViewServer) Kill() {
	atomic.StoreInt32(&vs.dead, 1)
	vs.l.Close()
}

//
// has this server been asked to shut down?
//
func (vs *ViewServer) isdead() bool {
	return atomic.LoadInt32(&vs.dead) != 0
}

// please don't change this function.
func (vs *ViewServer) GetRPCCount() int32 {
	return atomic.LoadInt32(&vs.rpccount)
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.
	//Lab2_PartA
	vs.View = View{0, "", ""}
	vs.servers = make(map[string]int)
	vs.server_idle = make(map[string]int)
	vs.server_ack = make(map[string]int)

	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.isdead() == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.isdead() == false {
				atomic.AddInt32(&vs.rpccount, 1)
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.isdead() == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.isdead() == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
