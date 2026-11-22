// Chat server: manages users and pushes messages/notifications to clients
// through RPC callbacks (the server dials back each client's RPC server).
package main

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"chatroom/common"
)

const (
	queueSize     = 64
	callTimeout   = 3 * time.Second
	pingInterval  = 5 * time.Second
	maxNameLen    = 20
	maxMessageLen = 1000
)

// session is one connected user. Events are queued and delivered by a
// dedicated goroutine so one slow client never blocks the others.
type session struct {
	name   string
	token  string
	client *rpc.Client
	queue  chan common.Event
	done   chan struct{}
	once   sync.Once
}

func (s *session) close() {
	s.once.Do(func() {
		close(s.done)
		s.client.Close()
	})
}

type ChatServer struct {
	mu       sync.RWMutex
	sessions map[string]*session
}

func NewChatServer() *ChatServer {
	return &ChatServer{sessions: make(map[string]*session)}
}

func newToken() string {
	b := make([]byte, 8)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func validName(n string) bool {
	if n == "" || len(n) > maxNameLen {
		return false
	}
	for _, r := range n {
		if r <= ' ' || r == 0x7f {
			return false
		}
	}
	return true
}

// Join registers a user. Fails if the username is already taken.
func (c *ChatServer) Join(args *common.JoinArgs, reply *common.JoinReply) error {
	name := strings.TrimSpace(args.Username)
	if !validName(name) {
		return fmt.Errorf("invalid username (1-%d chars, no spaces)", maxNameLen)
	}
	cl, err := dialTimeout(args.CallbackAddr)
	if err != nil {
		return fmt.Errorf("cannot reach your callback address %s: %v", args.CallbackAddr, err)
	}
	s := &session{
		name:   name,
		token:  newToken(),
		client: cl,
		queue:  make(chan common.Event, queueSize),
		done:   make(chan struct{}),
	}

	c.mu.Lock()
	if _, exists := c.sessions[name]; exists {
		c.mu.Unlock()
		cl.Close()
		return fmt.Errorf("username %q is already taken", name)
	}
	c.sessions[name] = s
	c.mu.Unlock()

	go c.deliver(s)
	c.broadcast(common.Event{Kind: common.KindJoin, From: name, Text: name + " joined the chat", Time: time.Now()}, name)
	log.Printf("JOIN  %s (%s)", name, args.CallbackAddr)

	reply.Token = s.token
	reply.Users = c.userList()
	return nil
}

func (c *ChatServer) auth(name, token string) (*session, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	s, ok := c.sessions[name]
	if !ok || s.token != token {
		return nil, errors.New("not joined (or invalid token)")
	}
	return s, nil
}

// Send broadcasts a message to everyone except the sender.
func (c *ChatServer) Send(args *common.SendArgs, _ *common.Empty) error {
	if _, err := c.auth(args.Username, args.Token); err != nil {
		return err
	}
	text := strings.TrimSpace(args.Text)
	if text == "" {
		return errors.New("empty message")
	}
	if len(text) > maxMessageLen {
		return fmt.Errorf("message too long (max %d)", maxMessageLen)
	}
	c.broadcast(common.Event{Kind: common.KindMessage, From: args.Username, Text: text, Time: time.Now()}, args.Username)
	return nil
}

func (c *ChatServer) Users(args *common.UsersArgs, reply *common.UsersReply) error {
	if _, err := c.auth(args.Username, args.Token); err != nil {
		return err
	}
	reply.Users = c.userList()
	return nil
}

func (c *ChatServer) Leave(args *common.LeaveArgs, _ *common.Empty) error {
	if _, err := c.auth(args.Username, args.Token); err != nil {
		return err
	}
	c.remove(args.Username, "left")
	return nil
}

func (c *ChatServer) userList() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	list := make([]string, 0, len(c.sessions))
	for n := range c.sessions {
		list = append(list, n)
	}
	sort.Strings(list)
	return list
}

// broadcast queues an event for every session except `exclude`.
func (c *ChatServer) broadcast(ev common.Event, exclude string) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for name, s := range c.sessions {
		if name == exclude {
			continue
		}
		select {
		case s.queue <- ev:
		default:
			log.Printf("queue full for %s, dropping event", name)
		}
	}
}

// deliver pushes queued events to one client via its callback RPC.
func (c *ChatServer) deliver(s *session) {
	for {
		select {
		case <-s.done:
			return
		case ev := <-s.queue:
			if err := callTimeoutRPC(s.client, "ClientCallback.Receive", ev); err != nil {
				c.remove(s.name, "connection lost")
				return
			}
		}
	}
}

// remove deletes a user and notifies the others (idempotent).
func (c *ChatServer) remove(name, reason string) {
	c.mu.Lock()
	s, ok := c.sessions[name]
	if ok {
		delete(c.sessions, name)
	}
	c.mu.Unlock()
	if !ok {
		return
	}
	s.close()
	text := name + " left the chat"
	if reason != "left" {
		text += " (" + reason + ")"
	}
	c.broadcast(common.Event{Kind: common.KindLeave, From: name, Text: text, Time: time.Now()}, "")
	log.Printf("LEAVE %s (%s)", name, reason)
}

// heartbeat detects clients that vanished without saying goodbye.
func (c *ChatServer) heartbeat() {
	t := time.NewTicker(pingInterval)
	defer t.Stop()
	for range t.C {
		c.mu.RLock()
		list := make([]*session, 0, len(c.sessions))
		for _, s := range c.sessions {
			list = append(list, s)
		}
		c.mu.RUnlock()
		for _, s := range list {
			go func(s *session) {
				if err := callTimeoutRPC(s.client, "ClientCallback.Ping", common.Empty{}); err != nil {
					c.remove(s.name, "connection lost")
				}
			}(s)
		}
	}
}

// shutdown tells every client we are going away, then closes sessions.
func (c *ChatServer) shutdown() {
	c.mu.Lock()
	list := make([]*session, 0, len(c.sessions))
	for _, s := range c.sessions {
		list = append(list, s)
	}
	c.sessions = make(map[string]*session)
	c.mu.Unlock()

	var wg sync.WaitGroup
	for _, s := range list {
		wg.Add(1)
		go func(s *session) {
			defer wg.Done()
			callTimeoutRPC(s.client, "ClientCallback.Receive",
				common.Event{Kind: common.KindShutdown, Text: "Server is shutting down", Time: time.Now()})
			s.close()
		}(s)
	}
	wg.Wait()
}

func dialTimeout(addr string) (*rpc.Client, error) {
	conn, err := net.DialTimeout("tcp", addr, callTimeout)
	if err != nil {
		return nil, err
	}
	return rpc.NewClient(conn), nil
}

func callTimeoutRPC(cl *rpc.Client, method string, arg any) error {
	call := cl.Go(method, arg, &common.Empty{}, make(chan *rpc.Call, 1))
	select {
	case <-call.Done:
		return call.Error
	case <-time.After(callTimeout):
		return errors.New("timeout")
	}
}

func main() {
	addr := flag.String("addr", ":1234", "address to listen on")
	flag.Parse()

	srv := NewChatServer()
	rs := rpc.NewServer()
	if err := rs.RegisterName("Chat", srv); err != nil {
		log.Fatal(err)
	}
	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("chat server listening on %s (Ctrl+C to stop)", ln.Addr())
	go srv.heartbeat()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-stop
		log.Println("shutting down...")
		ln.Close()
		srv.shutdown()
		os.Exit(0)
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		go rs.ServeConn(conn) // each client connection is served concurrently
	}
}
