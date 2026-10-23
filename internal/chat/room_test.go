package chat

import (
	"fmt"
	"net"
	"net/rpc"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

func startTestRoom(t *testing.T) (*Room, string) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	room := NewRoom()
	var connections sync.WaitGroup
	acceptDone := make(chan struct{})
	go func() {
		defer close(acceptDone)
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			connections.Add(1)
			go func() {
				defer connections.Done()
				room.ServeConn(conn)
			}()
		}
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		room.Close()
		<-acceptDone
		connections.Wait()
	})
	return room, listener.Addr().String()
}

func dialTestClient(t *testing.T, address string) *rpc.Client {
	t.Helper()
	conn, err := net.DialTimeout("tcp", address, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatal(err)
	}
	client := rpc.NewClient(conn)
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func mustCall(t *testing.T, client *rpc.Client, method string, args, reply any) {
	t.Helper()
	if err := client.Call(method, args, reply); err != nil {
		t.Fatalf("%s: %v", method, err)
	}
}

func joinTestClient(t *testing.T, address, name string) *rpc.Client {
	t.Helper()
	client := dialTestClient(t, address)
	mustCall(t, client, "Chat.Join", JoinRequest{Username: name}, &JoinReply{})
	return client
}

func drain(t *testing.T, client *rpc.Client) []Event {
	t.Helper()
	var reply PollReply
	mustCall(t, client, "Chat.Poll", PollRequest{}, &reply)
	return reply.Events
}

func assertEvent(t *testing.T, events []Event, index int, kind EventKind, user, text string) {
	t.Helper()
	if index >= len(events) {
		t.Fatalf("expected event %d, got %v", index, events)
	}
	e := events[index]
	if e.Kind != kind || e.Username != user || e.Text != text || e.When.IsZero() {
		t.Fatalf("unexpected event: %+v", e)
	}
}

func awaitEvent(t *testing.T, client *rpc.Client, kind EventKind, user string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		for _, event := range drain(t, client) {
			if event.Kind == kind && event.Username == user {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("did not receive %q event for %q", kind, user)
}

func TestJoinMessageUsersAndDisconnect(t *testing.T) {
	_, address := startTestRoom(t)
	alice := dialTestClient(t, address)
	var joined JoinReply
	mustCall(t, alice, "Chat.Join", JoinRequest{Username: "alice"}, &joined)
	if !reflect.DeepEqual(joined.Users, []string{"alice"}) {
		t.Fatalf("join reply: %v", joined.Users)
	}
	bob := joinTestClient(t, address, "bob")
	carol := joinTestClient(t, address, "carol")

	aEvents := drain(t, alice)
	if len(aEvents) != 2 {
		t.Fatalf("alice's join notifications: %v", aEvents)
	}
	assertEvent(t, aEvents, 0, JoinedEvent, "bob", "")
	assertEvent(t, aEvents, 1, JoinedEvent, "carol", "")
	bEvents := drain(t, bob)
	if len(bEvents) != 1 {
		t.Fatalf("bob's join notifications: %v", bEvents)
	}
	assertEvent(t, bEvents, 0, JoinedEvent, "carol", "")
	if events := drain(t, carol); len(events) != 0 {
		t.Fatalf("sender received a join notification: %v", events)
	}

	mustCall(t, alice, "Chat.Send", SendRequest{Text: "hello everyone"}, &Ack{})
	for _, client := range []*rpc.Client{bob, carol} {
		events := drain(t, client)
		if len(events) != 1 {
			t.Fatalf("message delivery: %v", events)
		}
		assertEvent(t, events, 0, MessageEvent, "alice", "hello everyone")
	}
	if events := drain(t, alice); len(events) != 0 {
		t.Fatalf("sender received its own message: %v", events)
	}
	var users UsersReply
	mustCall(t, carol, "Chat.Users", UsersRequest{}, &users)
	if !reflect.DeepEqual(users.Users, []string{"alice", "bob", "carol"}) {
		t.Fatalf("online users: %v", users.Users)
	}

	mustCall(t, bob, "Chat.Leave", LeaveRequest{}, &Ack{})
	_ = bob.Close() // Leave plus EOF must not produce two notifications.
	for _, client := range []*rpc.Client{alice, carol} {
		events := drain(t, client)
		if len(events) != 1 {
			t.Fatalf("leave notification: %v", events)
		}
		assertEvent(t, events, 0, LeftEvent, "bob", "")
	}
	_ = carol.Close() // No Leave: server must detect a crashed/closed client.
	awaitEvent(t, alice, LeftEvent, "carol")
	mustCall(t, alice, "Chat.Users", UsersRequest{}, &users)
	if !reflect.DeepEqual(users.Users, []string{"alice"}) {
		t.Fatalf("users after unexpected disconnect: %v", users.Users)
	}
}

func TestNamesAndInputValidation(t *testing.T) {
	_, address := startTestRoom(t)
	alice := joinTestClient(t, address, " Alice ")
	other := dialTestClient(t, address)
	if err := other.Call("Chat.Join", JoinRequest{Username: "alice"}, &JoinReply{}); err == nil {
		t.Fatal("case-insensitive duplicate username was accepted")
	}
	for _, invalid := range []string{"  ", "bad\nname", strings.Repeat("x", 33)} {
		if err := other.Call("Chat.Join", JoinRequest{Username: invalid}, &JoinReply{}); err == nil {
			t.Fatalf("invalid username %q was accepted", invalid)
		}
	}
	if err := other.Call("Chat.Send", SendRequest{Text: "not joined"}, &Ack{}); err == nil {
		t.Fatal("unregistered sender was accepted")
	}
	if err := other.Call("Chat.Users", UsersRequest{}, &UsersReply{}); err == nil {
		t.Fatal("unregistered user could list the room")
	}
	mustCall(t, other, "Chat.Join", JoinRequest{Username: "bob"}, &JoinReply{})
	for _, invalid := range []string{"", "   ", "line\nfeed", strings.Repeat("x", 2001)} {
		if err := other.Call("Chat.Send", SendRequest{Text: invalid}, &Ack{}); err == nil {
			t.Fatalf("invalid message %q was accepted", invalid)
		}
	}
	mustCall(t, other, "Chat.Leave", LeaveRequest{}, &Ack{})
	if err := other.Call("Chat.Join", JoinRequest{Username: "bob"}, &JoinReply{}); err == nil {
		t.Fatal("a connection rejoined after leaving")
	}
	_ = drain(t, alice)
}

func TestConcurrentSenders(t *testing.T) {
	_, address := startTestRoom(t)
	const (
		users    = 6
		messages = 25
	)
	clients := make([]*rpc.Client, users)
	for i := range clients {
		clients[i] = joinTestClient(t, address, fmt.Sprintf("user%d", i))
	}
	for _, client := range clients {
		_ = drain(t, client) // Ignore join notifications before the message test.
	}

	var wg sync.WaitGroup
	errs := make(chan error, users*messages)
	for i, client := range clients {
		wg.Add(1)
		go func(i int, client *rpc.Client) {
			defer wg.Done()
			for n := 0; n < messages; n++ {
				text := fmt.Sprintf("%d-%d", i, n)
				if err := client.Call("Chat.Send", SendRequest{Text: text}, &Ack{}); err != nil {
					errs <- err
					return
				}
			}
		}(i, client)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent send: %v", err)
	}

	for i, client := range clients {
		events := drain(t, client)
		if len(events) != (users-1)*messages {
			t.Fatalf("user%d received %d messages instead of %d", i, len(events), (users-1)*messages)
		}
		seen := make(map[string]bool)
		for _, event := range events {
			if event.Kind != MessageEvent || event.Username == fmt.Sprintf("user%d", i) || seen[event.Text] {
				t.Fatalf("invalid/duplicate delivery to user%d: %+v", i, event)
			}
			seen[event.Text] = true
		}
	}
}

func TestIdleClientIsRemoved(t *testing.T) {
	room, address := startTestRoom(t)
	room.idleTimeout = 200 * time.Millisecond // Set before the first connection.
	alice := joinTestClient(t, address, "alice")
	_ = joinTestClient(t, address, "bob")
	awaitEvent(t, alice, LeftEvent, "bob") // Alice's polling keeps her alive.
	var reply UsersReply
	mustCall(t, alice, "Chat.Users", UsersRequest{}, &reply)
	if !reflect.DeepEqual(reply.Users, []string{"alice"}) {
		t.Fatalf("users after heartbeat timeout: %v", reply.Users)
	}
}

func TestSlowClientIsDisconnectedInsteadOfDroppingMessages(t *testing.T) {
	_, address := startTestRoom(t)
	alice := joinTestClient(t, address, "alice")
	_ = joinTestClient(t, address, "bob")
	_ = drain(t, alice)
	for n := 0; n <= maxPendingEvents; n++ {
		mustCall(t, alice, "Chat.Send", SendRequest{Text: "message"}, &Ack{})
	}
	awaitEvent(t, alice, LeftEvent, "bob")
}

func TestCloseTerminatesAllConnections(t *testing.T) {
	room, address := startTestRoom(t)
	alice := joinTestClient(t, address, "alice")
	beforeJoin := dialTestClient(t, address)
	room.Close()
	room.Close()
	if err := alice.Call("Chat.Users", UsersRequest{}, &UsersReply{}); err == nil {
		t.Fatal("active connection survived server shutdown")
	}
	if err := beforeJoin.Call("Chat.Join", JoinRequest{Username: "bob"}, &JoinReply{}); err == nil {
		t.Fatal("pre-join connection survived server shutdown")
	}
}
