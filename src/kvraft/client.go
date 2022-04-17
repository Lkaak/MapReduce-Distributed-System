package kvraft

import (
	"6.824/labrpc"
	mathrand "math/rand"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
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
	// You'll have to add code here.
	ck.clientId = nrand()
	ck.recentLeaderId = mathrand.Intn(len(servers))
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	//发起新请求，递增requestId
	ck.requestId++
	requestId := ck.requestId
	server := ck.recentLeaderId
	for {
		//初始化请求和回复参数
		args := GetArgs{
			Key:       key,
			ClientId:  ck.clientId,
			RequestId: requestId,
		}
		reply := GetReply{}
		ok := ck.servers[server].Call("KVServer.Get", &args, &reply)
		if !ok || reply.Err == ErrWrongLeader {
			//RPC请求失败或者发送的服务器不是leader，则重新选一个新的服务器发送
			server = (server + 1) % len(ck.servers)
			continue
		} else if reply.Err == ErrNoKey {
			//如果返回没有对应的key，组返回空值
			return ""
		} else if reply.Err == OK {
			//如果返回ok，则返回回复中的值，并修改recentLeaderId
			ck.recentLeaderId = server
			return reply.Value
		}
	}
	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	//发起新请求，递增请求id
	ck.requestId++
	requestId := ck.requestId
	server := ck.recentLeaderId
	for {

		args := PutAppendArgs{
			Key:       key,
			Value:     value,
			Op:        op,
			ClientId:  ck.clientId,
			RequestId: requestId,
		}
		reply := PutAppendReply{}
		ok := ck.servers[server].Call("KVServer.PutAppend", &args, &reply)
		if !ok || reply.Err == ErrWrongLeader {
			server = (server + 1) % len(ck.servers)
			continue
		}
		if reply.Err == OK {
			ck.recentLeaderId = server
			return
		}
	}

	//如果不是leader则重新请求
	//如果ok则直接返回，并修改recentLeaderid
	return
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
