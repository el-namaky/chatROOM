// Package chat contains the RPC protocol and the concurrent chat room.
package chat

import "time"

// EventKind identifies a message or a change in room membership.
type EventKind string

const (
	MessageEvent EventKind = "message"
	JoinedEvent  EventKind = "joined"
	LeftEvent    EventKind = "left"
)

// Event is delivered to other clients by their next Chat.Poll RPC call.
type Event struct {
	Kind     EventKind
	Username string
	Text     string
	When     time.Time
}

type JoinRequest struct{ Username string }
type JoinReply struct{ Users []string }
type SendRequest struct{ Text string }
type PollRequest struct{}
type PollReply struct{ Events []Event }
type UsersRequest struct{}
type UsersReply struct{ Users []string }
type LeaveRequest struct{}
type Ack struct{}
