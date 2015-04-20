package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "math/rand"



type PBServer struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	me         string
	vs         *viewservice.Clerk
	// Your declarations here.
	//Lab2_PartB
	database   map[string]string
	View       viewservice.View
	Idmap      map[string]bool
}


func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	//Lab2_PartB
	//is primary
	if (pb.View.Primary == pb.me) {
		key := args.Key
		value, ok := pb.database[key]
		reply.Value = value
		if (ok) {
			reply.Err = OK
		} else {
			reply.Err = ErrNoKey
		}
	} else {
		reply.Err = ErrWrongServer
	}
	return nil
}

//Lab2_PartB
func (pb *PBServer) CopyToBackup(args *CopyArgs, reply *CopyReply) error {
	if (args.Backup == pb.me && pb.View.Backup == pb.me) {
		pb.database = args.Database
		reply.Err = OK
	} else {
		reply.Err = ErrWrongServer
		//return ErrWrongServer
	}
	return nil
}

func (pb *PBServer) ForwardToBackup(args *PutAppendArgs, reply *PutAppendReply) error {
	key := args.Key
	if (pb.View.Backup == pb.me) {
		if (args.Op == "Put") {
			pb.database[key] = args.Value
		} else if (args.Op == "Append") {
			pb.database[key] += args.Value
		}
		reply.Err = OK
	}
	return nil
}

func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {

	// Your code here.
	//Lab2_PartB
	//fmt.Println("args:", args, "reply:", reply)
	
	//is primary
	pb.mu.Lock()
	if (pb.View.Primary == pb.me) {
		//recv
		key := args.Key
		if (pb.database[args.Me] == args.Id) {
			//already in
			pb.mu.Unlock()
			return nil;
		}
		if (args.Op == "Put") {
			pb.database[key] = args.Value
		} else if (args.Op == "Append") {
			pb.database[key] += args.Value
		}
		reply.Err = OK
		pb.database[args.Me] = args.Id
		//copy to backup
		if (pb.View.Backup != "") {
			//has a backup
			//cpargs := &CopyArgs{}
			//cpargs.Backup = pb.View.Backup
			//cpargs.Database = pb.database
			//var cpreply CopyReply
			//ok := call(pb.View.Backup, "PBServer.CopyToBackup", cpargs, &cpreply);
			//if (ok == false) {
			//	fmt.Println("Copy to backup error!")
			//}
			ok := call(pb.View.Backup, "PBServer.ForwardToBackup", args, &reply)
			if (ok == false) {
				fmt.Println("Forward to backup error!")
			}
			
		}
	}

	pb.mu.Unlock()
	return nil
}


//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
func (pb *PBServer) tick() {

	// Your code here.
	//Lab2_PartB
	if pb.isdead() {
		fmt.Println("DEEE")
		return
	}
	view, ok := pb.vs.Ping(pb.View.Viewnum)
	if (ok != nil) {
		fmt.Println("ping err")
	}
	pb.View = view
}

// tell the server to shut itself down.
// please do not change these two functions.
func (pb *PBServer) kill() {
	atomic.StoreInt32(&pb.dead, 1)
	pb.l.Close()
}

func (pb *PBServer) isdead() bool {
	return atomic.LoadInt32(&pb.dead) != 0
}

// please do not change these two functions.
func (pb *PBServer) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&pb.unreliable, 1)
	} else {
		atomic.StoreInt32(&pb.unreliable, 0)
	}
}

func (pb *PBServer) isunreliable() bool {
	return atomic.LoadInt32(&pb.unreliable) != 0
}


func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	// Your pb.* initializations here.
	//Lab2_PartB
	pb.database = make(map[string]string)
	pb.View = viewservice.View{0,"",""}
	pb.Idmap = make(map[string]bool)

	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.isdead() == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.isdead() == false {
				if pb.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.isunreliable() && (rand.Int63()%1000) < 200 {
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
			if err != nil && pb.isdead() == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
	}()

	go func() {
		for pb.isdead() == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
	}()

	return pb
}
