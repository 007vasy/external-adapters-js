# Go Implementation of price-socketio.ts with Multi-threading

This is a Go implementation of the Aleno price-socketio.ts transport with multi-threading support. The implementation maintains the same functionality as the TypeScript version while leveraging Go's concurrency features (goroutines and channels) for multi-threading.

## Implementation Overview

The implementation consists of two main files:

1. `aleno/price_socketio.go` - The core implementation with multi-threading support
2. `aleno/cmd/main.go` - A demonstration of how to use the implementation

### Key Features

- **Socket.IO Client Connection**: Uses the `github.com/graarh/golang-socketio` library to establish and maintain a WebSocket connection to the price data server.
- **Subscription Management**: Implements methods to add, remove, and reconcile subscriptions to cryptocurrency pairs.
- **Response Data Parsing**: Parses incoming price data and updates an in-memory cache.
- **Multi-threading Support**: Uses goroutines and channels to handle connections, data processing, and subscription management concurrently.
- **Thread-Safe Operations**: Uses mutex locks to ensure thread safety for shared resources.

### Concurrency Design

The implementation uses several Go concurrency patterns:

1. **Worker Goroutines**:

   - `dataProcessor`: Processes incoming price data in a separate goroutine
   - `subscriptionManager`: Manages subscription changes in a separate goroutine

2. **Communication Channels**:

   - `dataChannel`: For passing price data between goroutines
   - `errorChannel`: For passing errors between goroutines
   - `subscriptionChannel`: For signaling subscription changes
   - `shutdownChannel`: For signaling graceful shutdown

3. **Thread Safety**:
   - `responseCacheMutex`: Protects access to the price data cache
   - `subscriptionsMutex`: Protects access to the subscription list
   - `connectionMutex`: Protects access to the connection status

### Comparison with TypeScript Implementation

The Go implementation maintains the same functionality as the TypeScript version while adapting to Go's concurrency paradigms:

| Feature                 | TypeScript Implementation      | Go Implementation                        |
| ----------------------- | ------------------------------ | ---------------------------------------- |
| WebSocket Connection    | Uses socket.io-client          | Uses golang-socketio                     |
| Subscription Management | Uses callbacks                 | Uses goroutines and channels             |
| Data Processing         | Single-threaded with callbacks | Multi-threaded with goroutines           |
| Error Handling          | Uses promises and callbacks    | Uses channels and explicit error returns |
| Thread Safety           | Handled by JavaScript runtime  | Explicitly implemented with mutexes      |

## Usage Example

```go
package main

import (
    "context"
    "log"
    "os"
    "time"

    "github.com/007vasy/aleno"
)

func main() {
    config := aleno.Config{
        APIKey:                "your-api-key",
        WSAPIEndpoint:         "wss://api.example.com",
        APITimeout:            time.Second * 30,
        BackgroundExecuteMS:   time.Second * 5,
        WarmupSubscriptionTTL: 300,
    }

    transport := aleno.NewSocketIOTransport(config)

    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    if err := transport.Start(ctx); err != nil {
        log.Fatalf("Failed to start transport: %v", err)
    }
    defer transport.Stop()

    // Subscribe to cryptocurrency pairs
    desiredPairs := []string{"BTC/USD", "ETH/USD", "LINK/USD"}
    if err := transport.ReconcileSubscriptions(desiredPairs); err != nil {
        log.Printf("Failed to reconcile subscriptions: %v", err)
    }

    // Get price data
    if priceData, exists := transport.GetPriceData("BTC", "USD"); exists {
        log.Printf("BTC/USD price: %f", priceData.Response.Result)
    }
}
```

## Benefits of the Go Implementation

1. **True Concurrency**: Unlike JavaScript's event loop, Go provides true concurrency with goroutines running in parallel.
2. **Memory Efficiency**: Go's lightweight goroutines use less memory than JavaScript threads.
3. **Type Safety**: Go's static typing helps catch errors at compile time rather than runtime.
4. **Performance**: Go's compiled nature provides better performance for CPU-intensive tasks.
5. **Simplified Error Handling**: Go's explicit error handling makes the code more robust and easier to debug.

## Dependencies

- `github.com/graarh/golang-socketio`: For Socket.IO client functionality
- `github.com/joho/godotenv`: For loading environment variables (used in the example)

## Building and Running

```bash
# Build the library
cd aleno
go build

# Build and run the example
cd cmd
go build
./cmd
```
