package api

import (
	"fmt"
	"net/mail"
	"regexp"

	"github.com/mertovun/event-driven-notification-system/internal/notification"
)

// E.164 recipient format for SMS.
var e164 = regexp.MustCompile(`^\+[1-9]\d{1,14}$`)

// Content size caps.
const (
	smsMaxChars   = 1000
	emailMaxBytes = 256 * 1024
	pushMaxBytes  = 4 * 1024
)

// validateRecipient returns the per-field error if the recipient does not match
// the channel's expected shape.
func validateRecipient(channel notification.Channel, recipient string) []FieldError {
	if recipient == "" {
		return []FieldError{{Field: "recipient", Message: "must not be empty"}}
	}
	switch channel {
	case notification.ChannelSMS:
		if !e164.MatchString(recipient) {
			return []FieldError{{Field: "recipient", Message: "must be E.164 (e.g. +905551234567)"}}
		}
	case notification.ChannelEmail:
		if _, err := mail.ParseAddress(recipient); err != nil {
			return []FieldError{{Field: "recipient", Message: "must be a valid email address"}}
		}
	case notification.ChannelPush:
		if len(recipient) < 16 || len(recipient) > 512 {
			return []FieldError{{Field: "recipient", Message: "device token length must be 16..512"}}
		}
	default:
		return []FieldError{{Field: "channel", Message: "must be sms | email | push"}}
	}
	return nil
}

// validateContent returns the per-field error if the content violates the
// channel-specific length cap.
func validateContent(channel notification.Channel, content string) []FieldError {
	if content == "" {
		return []FieldError{{Field: "content", Message: "must not be empty"}}
	}
	switch channel {
	case notification.ChannelSMS:
		if len(content) > smsMaxChars {
			return []FieldError{{Field: "content", Message: fmt.Sprintf("max %d chars for SMS", smsMaxChars)}}
		}
	case notification.ChannelEmail:
		if len(content) > emailMaxBytes {
			return []FieldError{{Field: "content", Message: fmt.Sprintf("max %d bytes for email", emailMaxBytes)}}
		}
	case notification.ChannelPush:
		if len(content) > pushMaxBytes {
			return []FieldError{{Field: "content", Message: fmt.Sprintf("max %d bytes for push", pushMaxBytes)}}
		}
	}
	return nil
}
