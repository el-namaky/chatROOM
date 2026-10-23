package chat

import (
	"errors"
	"net"
	"net/rpc"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"
)

const (
	maxUsernameLength = 32
	maxMessageLength  = 2000
	maxPendingEvents  = 256
	defaultIdleTime   = 30 * time.Second
)

// Room protects all membership and pending-event state with a single mutex.
// A distinct Session/RPC service is created for each TCP connection so an RPC
// caller cannot impersonate another user by supplying a username or token.
type Room struct {
	mu          sync.Mutex
	users       map[string]*Session
	sessions    map[*Session]struct{}
	closed      bool
	idleTimeout time.Duration
}

// Session is the Chat RPC receiver bound to one client connection.
// Its username, finished flag, and event queue are guarded by Room.mu.
type Session struct {
	room     *Room
	conn     net.Conn
	username string
	finished bool
	pending  []Event
}

func NewRoom() *Room {
	return &Room{
		users:       make(map[string]*Session),
		sessions:    make(map[*Session]struct{}),
		idleTimeout: defaultIdleTime,
	}
}

// ServeConn serves one client until it leaves, closes the connection, or
// misses the heartbeat deadline. In every case its username is removed.
func (r *Room) ServeConn(conn net.Conn) {
	defer conn.Close()

	s := &Session{room: r, conn: conn}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}
	r.sessions[s] = struct{}{}
	r.mu.Unlock()
	defer r.disconnect(s)

	s.touch()
	server := rpc.NewServer()
	if err := server.RegisterName("Chat", s); err != nil {
		return // The fixed, registered service is invalid; close the connection.
	}
	server.ServeConn(conn)
}

// Close stops all clients (including clients that have not joined yet).
// It is safe to call more than once, including during concurrent RPC calls.
func (r *Room) Close() {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}
	r.closed = true
	conns := make([]net.Conn, 0, len(r.sessions))
	for s := range r.sessions {
		s.finished = true
		s.username = ""
		conns = append(conns, s.conn)
	}
	r.users = make(map[string]*Session)
	r.mu.Unlock()

	for _, conn := range conns {
		_ = conn.Close()
	}
}

func (r *Room) disconnect(s *Session) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.sessions, s)
	if s.username != "" {
		r.removeLocked(s)
	}
	s.finished = true
}

func (s *Session) touch() {
	_ = s.conn.SetReadDeadline(time.Now().Add(s.room.idleTimeout))
}

// Join registers a unique username and notifies everybody except the new user.
func (s *Session) Join(req JoinRequest, reply *JoinReply) error {
	s.touch()
	username := strings.TrimSpace(req.Username)
	if !validText(username, maxUsernameLength) {
		return errors.New("username must be 1-32 characters and contain no control characters")
	}

	r := s.room
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed || s.finished {
		return errors.New("room is closed")
	}
	if s.username != "" {
		return errors.New("this connection has already joined")
	}
	for name := range r.users {
		if strings.EqualFold(name, username) {
			return errors.New("username is already in use")
		}
	}

	s.username = username
	r.users[username] = s
	reply.Users = r.usernamesLocked()
	r.broadcastLocked(Event{Kind: JoinedEvent, Username: username, When: time.Now()}, s)
	return nil
}

// Send broadcasts a message to every connected user except its sender.
func (s *Session) Send(req SendRequest, _ *Ack) error {
	s.touch()
	text := strings.TrimSpace(req.Text)
	if !validText(text, maxMessageLength) {
		return errors.New("message must be 1-2000 characters and contain no control characters")
	}

	r := s.room
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.joinedLocked(s) {
		return errors.New("join the room before sending messages")
	}
	r.broadcastLocked(Event{Kind: MessageEvent, Username: s.username, Text: text, When: time.Now()}, s)
	return nil
}

// Poll returns and removes all events pending for this connection.
// The client calls it periodically from a goroutine, independently of input.
func (s *Session) Poll(_ PollRequest, reply *PollReply) error {
	s.touch()
	r := s.room
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.joinedLocked(s) {
		return errors.New("join the room before polling")
	}
	reply.Events = append([]Event(nil), s.pending...)
	s.pending = s.pending[:0]
	return nil
}

func (s *Session) Users(_ UsersRequest, reply *UsersReply) error {
	s.touch()
	r := s.room
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.joinedLocked(s) {
		return errors.New("join the room before listing users")
	}
	reply.Users = r.usernamesLocked()
	return nil
}

// Leave unregisters the caller. ServeConn also calls disconnect on EOF, so
// a normal exit and an unexpected disconnect each produce one notification.
func (s *Session) Leave(_ LeaveRequest, _ *Ack) error {
	s.touch()
	r := s.room
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.joinedLocked(s) {
		return errors.New("not connected to the room")
	}
	r.removeLocked(s)
	s.finished = true
	return nil
}

func (r *Room) joinedLocked(s *Session) bool {
	return !r.closed && !s.finished && s.username != "" && r.users[s.username] == s
}

func (r *Room) removeLocked(s *Session) {
	name := s.username
	delete(r.users, name)
	s.username = ""
	if !r.closed {
		r.broadcastLocked(Event{Kind: LeftEvent, Username: name, When: time.Now()}, s)
	}
}

func (r *Room) usernamesLocked() []string {
	names := make([]string, 0, len(r.users))
	for name := range r.users {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func (r *Room) broadcastLocked(event Event, sender *Session) {
	for _, recipient := range r.users {
		if recipient == sender || recipient.finished {
			continue
		}
		if len(recipient.pending) >= maxPendingEvents {
			// Do not silently lose messages or let a stalled peer use unlimited RAM.
			// Closing its socket makes ServeConn unregister it and announce its exit.
			recipient.finished = true
			_ = recipient.conn.Close()
			continue
		}
		recipient.pending = append(recipient.pending, event)
	}
}

func validText(text string, maxRunes int) bool {
	if text == "" || !utf8.ValidString(text) || utf8.RuneCountInString(text) > maxRunes {
		return false
	}
	for _, ch := range text {
		if unicode.IsControl(ch) {
			return false
		}
	}
	return true
}
