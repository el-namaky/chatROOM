package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"chatroom/internal/chat"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:9000", "TCP address to listen on")
	flag.Parse()

	listener, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	room := chat.NewRoom()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		<-ctx.Done()
		_ = listener.Close()
		room.Close()
	}()

	log.Printf("chat server listening on %s (Ctrl+C to stop)", listener.Addr())
	var clients sync.WaitGroup
	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				break
			}
			log.Printf("accept: %v", err)
			continue
		}
		clients.Add(1)
		go func() {
			defer clients.Done()
			room.ServeConn(conn)
		}()
	}
	room.Close()
	clients.Wait()
	log.Print("chat server stopped")
}
