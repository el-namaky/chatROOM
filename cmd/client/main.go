package main

import (
	"bufio"
	"context"
	"errors"
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

	"chatroom/internal/chat"
)

const callTimeout = 5 * time.Second

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	addr := flag.String("addr", "127.0.0.1:9000", "chat server address")
	name := flag.String("name", "", "username (prompted for if omitted)")
	interval := flag.Duration("poll", 500*time.Millisecond, "time between polling calls")
	flag.Parse()
	if *interval < 100*time.Millisecond {
		return errors.New("-poll must be at least 100ms")
	}

	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 4096), 64*1024)
	if strings.TrimSpace(*name) == "" {
		fmt.Print("Username: ")
		if !scanner.Scan() {
			if err := scanner.Err(); err != nil {
				return fmt.Errorf("read username: %w", err)
			}
			return errors.New("username is required")
		}
		*name = scanner.Text()
	}

	conn, err := net.DialTimeout("tcp", *addr, callTimeout)
	if err != nil {
		return fmt.Errorf("connect to %s: %w", *addr, err)
	}
	client := rpc.NewClient(conn)
	var joined chat.JoinReply
	if err := call(client, "Chat.Join", chat.JoinRequest{Username: *name}, &joined); err != nil {
		_ = client.Close()
		return fmt.Errorf("join: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	pollCtx, stopPolling := context.WithCancel(ctx)
	pollDone := make(chan struct{})
	problems := make(chan error, 1)
	output := &printer{}
	defer func() {
		stopPolling()
		// An explicit leave sends the notification immediately. If the network
		// is already gone, closing the socket triggers server-side cleanup.
		_ = call(client, "Chat.Leave", chat.LeaveRequest{}, &chat.Ack{})
		_ = client.Close()
		<-pollDone
	}()

	fmt.Printf("Connected as %s. Online: %s\n", strings.TrimSpace(*name), strings.Join(joined.Users, ", "))
	fmt.Println("Type a message, /users, /help, or /quit. Ctrl+C also leaves.")
	fmt.Print("> ")
	go poll(pollCtx, client, *interval, output, problems, pollDone)

	inputs := make(chan inputResult)
	go readInput(ctx, scanner, inputs)
	for {
		select {
		case <-ctx.Done():
			output.line("Leaving the chat...")
			return nil
		case err := <-problems:
			return fmt.Errorf("connection lost: %w", err)
		case input := <-inputs:
			if input.done {
				if input.err != nil {
					return fmt.Errorf("read input: %w", input.err)
				}
				return nil // EOF is a clean exit too.
			}
			text := strings.TrimSpace(input.line)
			switch text {
			case "":
				output.prompt()
			case "/quit":
				return nil
			case "/help":
				output.line("Commands: /users lists members; /quit exits; everything else is a message.")
			case "/users":
				var users chat.UsersReply
				if err := call(client, "Chat.Users", chat.UsersRequest{}, &users); err != nil {
					output.line("Error: " + err.Error())
				} else {
					output.line("Online: " + strings.Join(users.Users, ", "))
				}
			default:
				if strings.HasPrefix(text, "/") {
					output.line("Unknown command. Type /help for commands.")
					continue
				}
				if err := call(client, "Chat.Send", chat.SendRequest{Text: text}, &chat.Ack{}); err != nil {
					output.line("Error: " + err.Error())
				} else {
					output.line("[me] " + text)
				}
			}
		}
	}
}

// net/rpc.Client supports simultaneous calls from input and polling goroutines.
// Close the client on a timeout so every in-flight call unblocks.
func call(client *rpc.Client, method string, args, reply any) error {
	result := client.Go(method, args, reply, make(chan *rpc.Call, 1))
	select {
	case done := <-result.Done:
		return done.Error
	case <-time.After(callTimeout):
		_ = client.Close()
		return fmt.Errorf("%s timed out", method)
	}
}

type inputResult struct {
	line string
	err  error
	done bool
}

func readInput(ctx context.Context, scanner *bufio.Scanner, inputs chan<- inputResult) {
	for scanner.Scan() {
		select {
		case inputs <- inputResult{line: scanner.Text()}:
		case <-ctx.Done():
			return
		}
	}
	select {
	case inputs <- inputResult{done: true, err: scanner.Err()}:
	case <-ctx.Done():
	}
}

func poll(ctx context.Context, client *rpc.Client, interval time.Duration, output *printer, problems chan<- error, done chan<- struct{}) {
	defer close(done)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if ctx.Err() != nil {
			return
		}
		var reply chat.PollReply
		if err := call(client, "Chat.Poll", chat.PollRequest{}, &reply); err != nil {
			if ctx.Err() == nil {
				select {
				case problems <- err:
				default:
				}
			}
			return
		}
		for _, event := range reply.Events {
			output.line(formatEvent(event))
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			return
		}
	}
}

func formatEvent(event chat.Event) string {
	stamp := event.When.Local().Format("15:04:05")
	switch event.Kind {
	case chat.MessageEvent:
		return fmt.Sprintf("[%s] %s: %s", stamp, event.Username, event.Text)
	case chat.JoinedEvent:
		return fmt.Sprintf("[%s] %s joined the room", stamp, event.Username)
	case chat.LeftEvent:
		return fmt.Sprintf("[%s] %s left the room", stamp, event.Username)
	default:
		return fmt.Sprintf("[%s] unknown notification", stamp)
	}
}

// Guard output from the input and polling goroutines against interleaving.
type printer struct{ mu sync.Mutex }

func (p *printer) line(message string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	fmt.Printf("\n%s\n> ", message)
}

func (p *printer) prompt() {
	p.mu.Lock()
	defer p.mu.Unlock()
	fmt.Print("> ")
}
