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
	pb.mu.Lock()
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
	pb.mu.Unlock()
	return nil
}

//Lab2_PartB
func (pb *PBServer) CopyToBackup(args *CopyArgs, reply *CopyReply) error {
	pb.mu.Lock()
	if (args.Backup == pb.me && pb.View.Backup == pb.me) {
		pb.database = args.Database
		reply.Err = OK
		pb.mu.Unlock()
	} else {
		reply.Err = ErrWrongServer
		//return ErrWrongServer
		pb.mu.Unlock()
	}
	return nil
}

func (pb *PBServer) ForwardToBackup(args *PutAppendArgs, reply *PutAppendReply) error {
	pb.mu.Lock()
	key := args.Key
	if (pb.database[args.Me] == args.Id) {
		reply.Err = OK
		pb.mu.Unlock()
		return nil
	}
	if (pb.View.Backup == pb.me) {
		pb.database[args.Me] = args.Id
		if (args.Op == "Put") {
			pb.database[key] = args.Value
		} else {
			pb.database[key] += args.Value
		}
		reply.Err = OK
		pb.mu.Unlock()
	} else {
		reply.Err = ErrWrongServer
		//return ErrWrongServer
		pb.mu.Unlock()
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
			//fmt.Println("RREEEEEPPPEEAAAAAAAAAAAATTTTTTTTTTT")
			reply.Err = OK//the key
			pb.mu.Unlock()
			return nil;
		}
		
		//foward to backup
		if (pb.View.Backup != "") {
			//has a backup
			fbargs := &PutAppendArgs{}
			fbargs = args
			var fbreply PutAppendReply
			for i:= 0; ; i ++ {
				ok := call(pb.View.Backup, "PBServer.ForwardToBackup", fbargs, &fbreply)
				if (ok == false || fbreply.Err != OK) {
					//fmt.Println("Forward to backup error!")
					reply.Err = fbreply.Err
					pb.mu.Unlock()
					return nil
				} 
				if (ok == true && fbreply.Err == OK) {
					break
				}
			}
			
		}
		if (args.Op == "Put") {
			pb.database[key] = args.Value
		} else {
			pb.database[key] += args.Value
		}
		reply.Err = OK
		pb.database[args.Me] = args.Id
		pb.mu.Unlock()
	} else {
		pb.mu.Unlock()
	}
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
	pb.mu.Lock()
	view, _ := pb.vs.Ping(pb.View.Viewnum)
	
	//new backup
	if (pb.View.Primary == pb.me && // pb.View.Primary == "" && 
		//Test: Put() immediately after primary failure
		pb.View.Backup != view.Backup && 
		//Test: Repeated failures/restarts
		view.Backup != "" ) {
		pb.View = view
		cpargs := &CopyArgs{}
		cpargs.Backup = pb.View.Backup
		cpargs.Database = pb.database
		var cpreply CopyReply
		for i := 0; ; i++ {
			ok := call(view.Backup, "PBServer.CopyToBackup", cpargs, &cpreply);
			//fmt.Println(cpargs)
			if (cpreply.Err != OK || ok != true) {
				//fmt.Println("Copy to backup error: ",cpreply.Err)
			}
			if (cpreply.Err == OK && ok == true) {
				break
			}
			//time.Sleep(viewservice.PingInterval)
			//fmt.Println("view: ",view, "pb.View:",pb.View)
		}
	} else {
		pb.View = view
	}
	pb.mu.Unlock()
	return
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
