# Go RPC Chat Room

Concurrent chat application in Go using `net/rpc` (standard library only).
Server and clients are separate programs. Communication approach: **Server Push** —
each client runs a small RPC server and registers its address on join; the server
calls `ClientCallback.Receive` on every client to deliver messages/notifications.

## Run
```
go run ./server -addr :1234              # terminal 1
go run ./client -name alice              # terminal 2  (omit -name to be prompted)
go run ./client -name bob -server localhost:1234   # terminal 3
```
Client commands: `/users` (list connected users), `/quit` (leave), `/help`.
Anything else is broadcast to all *other* users. Ctrl+C leaves cleanly on both sides.

## Design
- `common/` – shared RPC argument/reply/event types.
- `server/` – `Chat.Join/Send/Users/Leave` RPC methods. Sessions live in a map guarded
  by `sync.RWMutex`. Each user has a buffered queue + delivery goroutine, so a slow
  client never blocks others. Duplicate usernames are rejected. A random token per
  session prevents impersonation.
- Disconnect handling: failed/timeout callback or failed 5s heartbeat ping removes the
  user and notifies everyone ("connection lost"). Server Ctrl+C notifies clients, which exit.
- `client/` – prints pushed events automatically while you type.

## Test
Build with `go build -race ./...` and run several clients; verified join/leave
notifications, broadcast excluding sender, duplicate name rejection, killed-client
detection, and server shutdown, with no data races.
