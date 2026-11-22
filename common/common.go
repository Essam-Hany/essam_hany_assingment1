// Package common holds the RPC types shared by the chat server and client.
package common

import "time"

// Event kinds delivered from the server to clients.
const (
	KindMessage  = "message"
	KindJoin     = "join"
	KindLeave    = "leave"
	KindShutdown = "shutdown"
)

// Event is pushed by the server to every interested client.
type Event struct {
	Kind string
	From string
	Text string
	Time time.Time
}

type Empty struct{}

type JoinArgs struct {
	Username     string
	CallbackAddr string // host:port of the client's own RPC server
}

type JoinReply struct {
	Token string // must be presented on later calls
	Users []string
}

type SendArgs struct {
	Username string
	Token    string
	Text     string
}

type UsersArgs struct {
	Username string
	Token    string
}

type UsersReply struct {
	Users []string
}

type LeaveArgs struct {
	Username string
	Token    string
}
