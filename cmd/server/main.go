package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Jasonbourne723/platodb/internal/network"
)

func main() {

	processor := network.NewCommandProcessor()

	ctx, cancel := context.WithCancel(context.Background())

	srv, err := network.NewServer(ctx, processor, network.WithAddress("0.0.0.0:6399"))
	if err != nil {
		log.Fatal(err)
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		if err = srv.Listen(); err != nil {
			log.Fatal(err)
		}
	}()

	<-stop
	cancel()
	log.Println("Received shutdown signal. Initiating graceful shutdown...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Fatalf("Server shutdown failed: %v", err)
	}

	log.Println("Server gracefully stopped")
}
