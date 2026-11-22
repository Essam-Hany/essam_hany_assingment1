// Chat client: joins with a unique username, prints pushed events
// automatically, and sends what you type to everyone else.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"net"
	"net/rpc"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"chatroom/common"
)

var printMu sync.Mutex

// say prints a line without garbling the user's prompt.
func say(format string, a ...any) {
	printMu.Lock()
	defer printMu.Unlock()
	fmt.Printf("\r\033[K"+format+"\n> ", a...)
}

// info prints a local reply; the main loop reprints the prompt afterwards.
func info(format string, a ...any) {
	printMu.Lock()
	defer printMu.Unlock()
	fmt.Printf(format+"\n", a...)
}

// ClientCallback is the RPC service the server calls to push events to us.
type ClientCallback struct {
	quit chan struct{}
	once sync.Once
}

func (c *ClientCallback) Receive(ev common.Event, _ *common.Empty) error {
	ts := ev.Time.Format("15:04:05")
	switch ev.Kind {
	case common.KindMessage:
		say("[%s] %s: %s", ts, ev.From, ev.Text)
	case common.KindJoin, common.KindLeave:
		say("[%s] *** %s", ts, ev.Text)
	case common.KindShutdown:
		say("[%s] *** %s. Bye!", ts, ev.Text)
		c.once.Do(func() { close(c.quit) })
	}
	return nil
}

func (c *ClientCallback) Ping(_ common.Empty, _ *common.Empty) error { return nil }

func main() {
	server := flag.String("server", "localhost:1234", "chat server address")
	name := flag.String("name", "", "username (asked interactively if empty)")
	flag.Parse()

	in := bufio.NewReader(os.Stdin)
	if *name == "" {
		fmt.Print("Username: ")
		line, _ := in.ReadString('\n')
		*name = strings.TrimSpace(line)
	}

	conn, err := net.DialTimeout("tcp", *server, 5*time.Second)
	if err != nil {
		fmt.Println("cannot connect to server:", err)
		os.Exit(1)
	}
	srv := rpc.NewClient(conn)
	defer srv.Close()

	// Our own RPC server so the server can push events to us.
	cb := &ClientCallback{quit: make(chan struct{})}
	rs := rpc.NewServer()
	rs.RegisterName("ClientCallback", cb)
	ln, err := net.Listen("tcp", ":0")
	if err != nil {
		fmt.Println("listen error:", err)
		os.Exit(1)
	}
	defer ln.Close()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go rs.ServeConn(c)
		}
	}()
	// Advertise the local IP that routes to the server + our listening port.
	host, _, _ := net.SplitHostPort(conn.LocalAddr().String())
	_, port, _ := net.SplitHostPort(ln.Addr().String())

	var jr common.JoinReply
	err = srv.Call("Chat.Join", &common.JoinArgs{Username: *name, CallbackAddr: net.JoinHostPort(host, port)}, &jr)
	if err != nil {
		fmt.Println("join failed:", err)
		os.Exit(1)
	}
	fmt.Printf("Welcome, %s! Online: %s\nCommands: /users  /quit  /help\n", *name, strings.Join(jr.Users, ", "))

	var once sync.Once
	leave := func() {
		once.Do(func() {
			srv.Go("Chat.Leave", &common.LeaveArgs{Username: *name, Token: jr.Token}, &common.Empty{}, nil)
			time.Sleep(200 * time.Millisecond) // let the request flush
		})
	}

	// Clean shutdown on Ctrl+C.
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sig
		fmt.Println("\nLeaving...")
		leave()
		os.Exit(0)
	}()

	lines := make(chan string)
	go func() {
		for {
			l, err := in.ReadString('\n')
			if err != nil {
				close(lines)
				return
			}
			lines <- strings.TrimSpace(l)
		}
	}()

	fmt.Print("> ")
	for {
		select {
		case <-cb.quit:
			os.Exit(0)
		case l, ok := <-lines:
			if !ok { // stdin closed
				leave()
				return
			}
			switch {
			case l == "":
			case l == "/quit":
				leave()
				fmt.Println("Goodbye!")
				return
			case l == "/help":
				info("/users  list connected users\n/quit   leave the chat\nanything else is sent as a message")
			case l == "/users":
				var ur common.UsersReply
				if err := srv.Call("Chat.Users", &common.UsersArgs{Username: *name, Token: jr.Token}, &ur); err != nil {
					info("error: %v", err)
					break
				}
				info("Online (%d): %s", len(ur.Users), strings.Join(ur.Users, ", "))
			default:
				if err := srv.Call("Chat.Send", &common.SendArgs{Username: *name, Token: jr.Token, Text: l}, &common.Empty{}); err != nil {
					info("error: %v", err)
					if err == rpc.ErrShutdown {
						os.Exit(1)
					}
				}
			}
			printMu.Lock()
			fmt.Print("> ")
			printMu.Unlock()
		}
	}
}
