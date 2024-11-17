package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Jasonbourne723/platodb/config"
	"github.com/Jasonbourne723/platodb/internal/database"
	"github.com/Jasonbourne723/platodb/internal/network"
)

func main() {

	cfg, err := config.LoadConfig(".")
	if err != nil {
		log.Fatal(fmt.Errorf("配置加载失败:%w", err))
	}

	db, err := database.NewDB(database.WithDir(cfg.Database.DataDir, cfg.Database.WalDir))
	if err != nil {
		log.Fatal(err)
	}
	processor := network.NewCommandProcessor(db)

	ctx, cancel := context.WithCancel(context.Background())
	srv, err := network.NewServer(ctx, processor, network.WithAddress(cfg.Network.Address))
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
