# Go RPC Chat Room

A concurrent, terminal-based client/server chat application for the Distributed Systems chat-room assignment. The programs communicate with Go's standard-library `net/rpc` over TCP; there are no external dependencies.

## Run it

Install [Go 1.22+](https://go.dev/dl/) and open **separate terminals** in this repository:

```text
Terminal 1 (server):  go run ./cmd/server
Terminal 2 (Alice):   go run ./cmd/client -name alice
Terminal 3 (Bob):     go run ./cmd/client -name bob
Terminal 4 (Carol):   go run ./cmd/client -name carol
```

On PowerShell, Bash, and similar shells, run the command after the colon (not the `Terminal N` label). The default address is `127.0.0.1:9000`. To connect from another computer, run the server with `-addr :9000` and each client with `-addr SERVER_IP:9000`. You may omit `-name` to be prompted. Use `-poll 1s` to change the default 500 ms polling interval.

At a client prompt, type a line of text to send a message, `/users` to list members, `/help` for help, or `/quit` to leave. **Ctrl+C** and end-of-file also leave cleanly. Ctrl+C on the server closes all client connections.

## Demonstration checklist

1. Start the server, then Alice, Bob, and Carol in different terminals. Existing clients print join notifications automatically.
2. Send `Hello everyone` from Alice. Bob and Carol print the message; Alice does not receive her own broadcast.
3. Send a reply from Bob and use `/users` from Carol to show all three connected users.
4. Use `/quit` from Bob; Alice and Carol print his leave notification. Closing or killing a client without `/quit` also removes it, with a leave notification.
5. Stop the server with Ctrl+C; the remaining clients report disconnection and exit.

**Video:** [Watch the 29-second demo](demo/chat-demo.mp4). It shows four terminal views built from the **actual output** of separate Go server/client processes, including three users, messages, `/users`, a clean leave, and an unexpected disconnect. To avoid recording unrelated desktop windows, the video is rendered from live console output rather than captured from the whole desktop. To regenerate it, install Python's `Pillow` package and `ffmpeg`, then run `python demo/record.py` (Go must be on PATH).

## How it works

**Communication choice: polling.** Each client has a background goroutine that calls `Chat.Poll` every 500 ms while the main goroutine reads the terminal. The server queues messages and presence events for each recipient; each poll atomically retrieves and clears that user's queue. No callback or HTTP server is required.

The server accepts clients concurrently and gives each TCP connection its **own `Chat` RPC receiver**. A client joins once with a unique (case-insensitive) username. The receiver identifies its user from the connection, not from an untrusted username in `Send` or `Poll`. A mutex guards shared membership and event queues, so joins, broadcasts, polls, and leaves do not race. Events go to *other* users only. The queue is bounded; a client that stops consuming events is disconnected rather than losing messages silently or exhausting server memory.

An explicit `Leave`, a closed socket (including an abruptly terminated process), and a missing heartbeat all clean up membership. Every RPC refreshes a 30-second server-side read deadline; regular polls serve as heartbeats. Cleanup is idempotent, so a normal leave followed by a closed socket cannot generate two leave notifications.

## Test

```sh
go test ./...
go vet ./...
go test -race ./...  # where a CGO C compiler is installed
```

The integration tests use real TCP/RPC connections and cover broadcasts, sender exclusion, joining/leaving, duplicate usernames, invalid input, concurrent senders, idle timeouts, slow clients, unexpected disconnections, and server shutdown.

## Project layout

```text
cmd/server/main.go       TCP listener and graceful shutdown
cmd/client/main.go       terminal client, input and polling goroutines
internal/chat/protocol.go shared RPC types
internal/chat/room.go     thread-safe room and per-connection RPC service
internal/chat/room_test.go integration tests
```

The design follows the course's Go introduction and distributed-systems concepts; the exact chat features and `net/rpc` requirement come from the assignment brief.

## Publish on GitHub

The submission should be a public repository on the `main` branch. After signing in to GitHub, create an **empty** repository (for example, `go-rpc-chat-room`) and run:

```sh
git remote add origin https://github.com/YOUR_USERNAME/go-rpc-chat-room.git
git push -u origin main
```

Replace `YOUR_USERNAME` with your GitHub account. Submit that repository's URL and the video link (`demo/chat-demo.mp4` in the repository).
