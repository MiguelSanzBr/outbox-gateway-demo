// mockprovider: provedor de mensagens simulado, configurado por ambiente.
package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/MiguelSanzBr/outbox-gateway-demo/internal/adapters/mockprovider"
)

// exit é variável só pra o teste conseguir ver o código de saída sem matar o processo.
var exit = os.Exit

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx); err != nil {
		log.Print(err)
		exit(1)
	}
}

func run(ctx context.Context) error {
	latency, _ := time.ParseDuration(os.Getenv("LATENCY"))
	jitter, _ := time.ParseDuration(os.Getenv("JITTER"))
	failRate, _ := strconv.ParseFloat(os.Getenv("FAIL_RATE"), 64)
	addr := os.Getenv("ADDR")
	if addr == "" {
		addr = ":9090"
	}

	p := mockprovider.New(mockprovider.Config{Latency: latency, Jitter: jitter, FailRate: failRate})
	log.Printf("mockprovider em %s latency=%s jitter=%s fail_rate=%.2f", addr, latency, jitter, failRate)
	srv := &http.Server{Addr: addr, Handler: p.Handler(), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		_ = srv.Shutdown(context.Background())
	}()
	if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
