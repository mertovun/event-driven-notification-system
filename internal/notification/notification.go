package notification

// Channel is one of sms/email/push. See migrations/0001 status check.
type Channel string

const (
	ChannelSMS   Channel = "sms"
	ChannelEmail Channel = "email"
	ChannelPush  Channel = "push"
)

func (c Channel) Valid() bool {
	switch c {
	case ChannelSMS, ChannelEmail, ChannelPush:
		return true
	}
	return false
}

// Priority is the API-facing label. Mapped to int16 columns and AMQP priority bytes.
type Priority string

const (
	PriorityHigh   Priority = "high"
	PriorityNormal Priority = "normal"
	PriorityLow    Priority = "low"
)

// Int16 returns the smallint value persisted in notifications.priority.
// 1, 5, 9 — see docs/02 §3.3 (priority CHECK).
func (p Priority) Int16() int16 {
	switch p {
	case PriorityHigh:
		return 9
	case PriorityLow:
		return 1
	default:
		return 5
	}
}

func (p Priority) Valid() bool {
	switch p {
	case PriorityHigh, PriorityNormal, PriorityLow, "":
		return true
	}
	return false
}

// PriorityFromInt converts the persisted smallint back to the API label.
func PriorityFromInt(v int16) Priority {
	switch v {
	case 9:
		return PriorityHigh
	case 1:
		return PriorityLow
	default:
		return PriorityNormal
	}
}

// Status mirrors the notifications.status enum.
type Status string

const (
	StatusPending    Status = "pending"
	StatusScheduled  Status = "scheduled"
	StatusQueued     Status = "queued"
	StatusSending    Status = "sending"
	StatusSent       Status = "sent"
	StatusFailed     Status = "failed"
	StatusCancelled  Status = "cancelled"
	StatusDeadLetter Status = "dead_letter"
)
