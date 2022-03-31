package shardctrler

//
// Shardctrler clerk.
//

import (
	"6.824/labrpc"
	mathrand "math/rand"
)
import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	clientId       int64
	requestId      int
	recentLeaderId int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.clientId = nrand()
	ck.recentLeaderId = mathrand.Intn(len(servers))
	return ck
}

func (ck *Clerk) Query(num int) Config {

	// Your code here.
	ck.requestId++
	sev := ck.recentLeaderId
	for {
		args := QueryArgs{
			Num:       num,
			ClientId:  ck.clientId,
			RequestId: ck.requestId,
		}
		reply := QueryReply{}
		ok := ck.servers[sev].Call("ShardCtrler.Query", &args, &reply)
		if !ok || reply.WrongLeader == true {
			sev = (sev + 1) % len(ck.servers)
			continue
		}
		if reply.Err == OK {
			ck.recentLeaderId = sev
			return reply.Config
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	ck.requestId++
	sev := ck.recentLeaderId
	// Your code here.
	for {
		args := JoinArgs{
			Servers:   servers,
			ClientId:  ck.clientId,
			RequestId: ck.requestId,
		}
		reply := JoinReply{}
		ok := ck.servers[sev].Call("ShardCtrler.Join", &args, &reply)
		if !ok || reply.WrongLeader == true {
			sev = (sev + 1) % len(ck.servers)
			continue
		}
		if reply.Err == OK {
			ck.recentLeaderId = sev
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	ck.requestId++
	sev := ck.recentLeaderId
	// Your code here.
	for {
		args := LeaveArgs{
			GIDs:      gids,
			ClientId:  ck.clientId,
			RequestId: ck.requestId,
		}
		reply := JoinReply{}
		ok := ck.servers[sev].Call("ShardCtrler.Leave", &args, &reply)
		if !ok || reply.WrongLeader == true {
			sev = (sev + 1) % len(ck.servers)
			continue
		}
		if reply.Err == OK {
			ck.recentLeaderId = sev
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	ck.requestId++
	sev := ck.recentLeaderId
	// Your code here.
	for {
		args := MoveArgs{
			Shard:     shard,
			GID:       gid,
			ClientId:  ck.clientId,
			RequestId: ck.requestId,
		}
		reply := JoinReply{}
		ok := ck.servers[sev].Call("ShardCtrler.Move", &args, &reply)
		if !ok || reply.WrongLeader == true {
			sev = (sev + 1) % len(ck.servers)
			continue
		}
		if reply.Err == OK {
			ck.recentLeaderId = sev
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
}
