package app_test

import (
	"context"
	"errors"
	"testing"

	"github.com/MiguelSanzBr/outbox-gateway-demo/internal/app"
	"github.com/MiguelSanzBr/outbox-gateway-demo/internal/domain"
)

// Transição inválida para no caso de uso, antes de chegar em qualquer porta (por isso as portas nil).
func TestDeliveryRejectsInvalidTransition(t *testing.T) {
	svc := app.NewService(nil, nil, nil, app.Config{})
	done := &domain.OutboxEntry{ID: 1, Status: domain.Sent}
	if err := svc.CompleteDelivery(context.Background(), done); !errors.Is(err, domain.ErrInvalidTransition) {
		t.Fatalf("CompleteDelivery: err=%v", err)
	}
	if _, err := svc.FailDelivery(context.Background(), done, errors.New("x")); !errors.Is(err, domain.ErrInvalidTransition) {
		t.Fatalf("FailDelivery: err=%v", err)
	}
}
