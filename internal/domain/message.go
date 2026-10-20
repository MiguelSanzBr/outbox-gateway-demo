package domain

import "time"

const (
	maxKeyLen       = 200
	maxRecipientLen = 64
	maxBodyLen      = 4096
)

// Message é o pedido de envio, já validado.
type Message struct {
	IdempotencyKey string
	Recipient      string
	Body           string
}

func CheckKey(key string) error {
	if key == "" || len(key) > maxKeyLen {
		return ErrInvalidKey
	}
	return nil
}

func NewMessage(key, recipient, body string) (Message, error) {
	if err := CheckKey(key); err != nil {
		return Message{}, err
	}
	if recipient == "" || len(recipient) > maxRecipientLen || body == "" || len(body) > maxBodyLen {
		return Message{}, ErrInvalidContent
	}
	return Message{IdempotencyKey: key, Recipient: recipient, Body: body}, nil
}

// MessageState é o q GET /v1/messages/{id} devolve.
type MessageState struct {
	ID        int64      `json:"id"`
	Recipient string     `json:"recipient"`
	Status    Status     `json:"status"`
	Attempts  int        `json:"attempts"`
	LastError *string    `json:"last_error,omitempty"`
	CreatedAt time.Time  `json:"created_at"`
	SentAt    *time.Time `json:"sent_at,omitempty"`
}

// Change é uma mudança de estado da outbox, como o painel recebe na lista e no SSE.
type Change struct {
	ID        int64   `json:"id"`
	Recipient string  `json:"recipient"`
	Status    Status  `json:"status"`
	Attempts  int     `json:"attempts"`
	Worker    *int    `json:"worker"`
	LastError *string `json:"last_error"`
}
