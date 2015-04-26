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
	servers        map[string]int
	server_ack     map[string]int
	server_idle    map[string]int
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

	// Your code here.
	//Lab2_PartA
	//fmt.Println(args)
	vs.mu.Lock()
	vs.servers[args.Me] = DeadPings
	if (args.Viewnum > 0 && args.Viewnum == vs.View.Viewnum) {
		vs.server_ack[args.Me] = 1
	}
	if (args.Viewnum == 0 && vs.View.Primary == "") {
		//first primary
		vs.View.Viewnum++
		vs.View.Primary = args.Me
		vs.server_ack[args.Me] = 0
	} else if (args.Me == vs.View.Primary) {
		//get ping from primary
		if (args.Viewnum == 0) {
			//restart
			vs.make_backup_to_primary()
		} else if (args.Viewnum == vs.View.Viewnum) {
			//acked
			vs.server_ack[args.Me] = 1
		} else {
			vs.server_ack[args.Me] = 0
		}
	} else if (vs.View.Backup == "" && vs.server_ack[vs.View.Primary] == 1) {
		//first backup
		vs.View.Viewnum++
		vs.View.Backup = args.Me
		vs.server_ack[args.Me] = 0
	} else if (args.Me == vs.View.Backup) {
		//get ping from backup
		if (args.Viewnum == 0) {
			//restart
			if (vs.server_ack[vs.View.Primary] == 1) {
				vs.View.Backup = args.Me
				vs.View.Viewnum++
				vs.server_ack[args.Me] = 0
			}
		}
	} 
	reply.View = vs.View
	vs.mu.Unlock()	
	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	//Lab2_PartA
	vs.mu.Lock()
	reply.View = vs.View
	vs.mu.Unlock()
	return nil
}


//Lab2_PartA
//Test: Backup takes over if primary fails
//Test: Restarted server becomes backup
//Test: Idle third server becomes backup if primary fails
//Test: Restarted primary treated as dead
func (vs *ViewServer) make_backup_to_primary () {
	if (vs.View.Backup == "") {
		vs.View.Primary = ""
	} else {
		vs.View.Viewnum++
		vs.View.Primary = vs.View.Backup
		vs.View.Backup = ""
	}
	vs.server_ack[vs.View.Primary] = 0
}


//Lab2_PartA
func (vs *ViewServer) remove_dead_backup() {
	//vs.mu.Lock()
	vs.View.Viewnum += 1
	vs.View.Backup = ""
	//vs.mu.Unlock()
}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {

	// Your code here.
	//Lab2_PartA
	vs.mu.Lock()
	p, b := vs.View.Primary, vs.View.Backup
	if (p != "") {
		vs.servers[p]--
	}
	if (b != "") {
		vs.servers[b]--
	}
	if (vs.servers[p] == 0 && vs.servers[b] == 0) {
		vs.mu.Unlock()
		return
	}
	if (vs.servers[p] == 0 && vs.server_ack[p] == 1 && p != "") {
		vs.make_backup_to_primary()
	} 
	if (vs.servers[b] == 0 && vs.server_ack[p] == 1 && b != "") {
		vs.remove_dead_backup()
		//vs.View.Viewnum++
	}
	vs.mu.Unlock()
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
