package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/007vasy/aleno"
	"github.com/joho/godotenv"
)

func main() {
	_ = godotenv.Load()

	apiKey := os.Getenv("API_KEY")
	if apiKey == "" {
		log.Println("Warning: API_KEY environment variable not set")
		apiKey = "your-api-key" // Replace with actual API key for testing
	}

	wsEndpoint := os.Getenv("WS_API_ENDPOINT")
	if wsEndpoint == "" {
		wsEndpoint = "wss://api.example.com" // Replace with actual endpoint
	}

	config := aleno.Config{
		APIKey:                apiKey,
		WSAPIEndpoint:         wsEndpoint,
		APITimeout:            time.Second * 30,
		BackgroundExecuteMS:   time.Second * 5,
		WarmupSubscriptionTTL: 300,
	}

	transport := aleno.NewSocketIOTransport(config)
	if err := transport.Initialize(); err != nil {
		log.Fatalf("Failed to initialize transport: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := transport.Start(ctx); err != nil {
		log.Fatalf("Failed to start transport: %v", err)
	}
	defer transport.Stop()

	desiredPairs := []string{"BTC/USD", "ETH/USD", "LINK/USD"}
	if err := transport.ReconcileSubscriptions(desiredPairs); err != nil {
		log.Printf("Failed to reconcile subscriptions: %v", err)
	}

	ticker := time.NewTicker(time.Second * 10)
	defer ticker.Stop()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	log.Println("Socket.IO transport started. Press Ctrl+C to exit.")

	for {
		select {
		case <-ticker.C:
			allPrices := transport.GetAllPriceData()
			log.Printf("Current prices (%d pairs):", len(allPrices))
			for pair, data := range allPrices {
				log.Printf("  %s: %f", pair, data.Response.Result)
			}

		case err := <-transport.GetErrorChannel():
			log.Printf("Error from transport: %v", err)

		case <-sigChan:
			log.Println("Received shutdown signal. Stopping...")
			return
		}
	}
}
