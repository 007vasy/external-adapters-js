package aleno

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	socketio "github.com/graarh/golang-socketio"
	"github.com/graarh/golang-socketio/transport"
)

type ResponseItem struct {
	ID                                        string  `json:"id"`
	BaseSymbol                                string  `json:"baseSymbol"`
	QuoteSymbol                               string  `json:"quoteSymbol"`
	ProcessTimestamp                          int64   `json:"processTimestamp"`
	ProcessBlockChainID                       string  `json:"processBlockChainId"`
	ProcessBlockNumber                        int64   `json:"processBlockNumber"`
	ProcessBlockTimestamp                     int64   `json:"processBlockTimestamp"`
	AggregatedLast7DaysBaseVolume             float64 `json:"aggregatedLast7DaysBaseVolume"`
	Price                                     float64 `json:"price"`
	AggregatedMarketDepthMinusOnePercentUSD   float64 `json:"aggregatedMarketDepthMinusOnePercentUsdAmount"`
	AggregatedMarketDepthPlusOnePercentUSD    float64 `json:"aggregatedMarketDepthPlusOnePercentUsdAmount"`
	AggregatedMarketDepthUSD                  float64 `json:"aggregatedMarketDepthUsdAmount"`
	AggregatedLast7DaysUSDVolume              float64 `json:"aggregatedLast7DaysUsdVolume"`
}

type PriceResponse struct {
	Params struct {
		Base  string `json:"base"`
		Quote string `json:"quote"`
	} `json:"params"`
	Response struct {
		Data struct {
			Result float64 `json:"result"`
		} `json:"data"`
		Result     float64 `json:"result"`
		Timestamps struct {
			ProviderDataStreamEstablishedUnixMs int64 `json:"providerDataStreamEstablishedUnixMs"`
			ProviderDataReceivedUnixMs          int64 `json:"providerDataReceivedUnixMs"`
			ProviderIndicatedTimeUnixMs         int64 `json:"providerIndicatedTimeUnixMs"`
		} `json:"timestamps"`
	} `json:"response"`
}

type SubscriptionResponse struct {
	Status                 string   `json:"status"`
	SubscriptionsAfterUpdate []string `json:"subscriptionsAfterUpdate"`
}

type Config struct {
	APIKey                string
	WSAPIEndpoint         string
	APITimeout            time.Duration
	BackgroundExecuteMS   time.Duration
	WarmupSubscriptionTTL int
}

type SocketIOTransport struct {
	client                 *socketio.Client
	responseCache          map[string]PriceResponse
	responseCacheMutex     sync.RWMutex
	confirmedSubscriptions map[string]bool
	subscriptionsMutex     sync.RWMutex
	config                 Config
	dataStreamEstablished  int64
	isConnected            bool
	connectionMutex        sync.RWMutex
	
	dataChannel            chan []ResponseItem
	errorChannel           chan error
	subscriptionChannel    chan struct{} // Signal channel for subscription changes
	shutdownChannel        chan struct{} // Signal channel for shutdown
}

func NewSocketIOTransport(config Config) *SocketIOTransport {
	return &SocketIOTransport{
		responseCache:          make(map[string]PriceResponse),
		confirmedSubscriptions: make(map[string]bool),
		config:                 config,
		dataChannel:            make(chan []ResponseItem, 100),
		errorChannel:           make(chan error, 10),
		subscriptionChannel:    make(chan struct{}, 1),
		shutdownChannel:        make(chan struct{}),
	}
}

func (t *SocketIOTransport) Initialize() error {
	return nil
}

func (t *SocketIOTransport) Start(ctx context.Context) error {
	tr := transport.GetDefaultWebsocketTransport()
	tr.RequestHeader = map[string][]string{
		"Authorization": {fmt.Sprintf("Bearer %s", t.config.APIKey)},
	}

	var err error
	t.client, err = socketio.Dial(t.config.WSAPIEndpoint, tr)
	if err != nil {
		return fmt.Errorf("failed to create Socket.IO client: %w", err)
	}

	t.client.On(socketio.OnConnection, func(c *socketio.Channel) {
		t.setConnected(true)
		log.Println("Connection open")
		
		t.subscriptionsMutex.Lock()
		t.confirmedSubscriptions = make(map[string]bool)
		t.subscriptionsMutex.Unlock()
		
		t.dataStreamEstablished = time.Now().UnixMilli()
	})

	t.client.On(socketio.OnDisconnection, func(c *socketio.Channel) {
		t.setConnected(false)
		log.Println("Connection closed")
	})

	t.client.On(socketio.OnError, func(c *socketio.Channel, err error) {
		if t.isClientActive() {
			log.Println("Temporary failure, the socket will automatically try to reconnect")
		} else {
			log.Printf("Connection error: %v", err)
			t.errorChannel <- err
		}
	})

	t.client.On("initial_token_states", func(c *socketio.Channel, msg string) {
		var responseItems []ResponseItem
		if err := json.Unmarshal([]byte(msg), &responseItems); err != nil {
			log.Printf("Error parsing initial_token_states: %v", err)
			return
		}
		
		log.Printf("Received initial data: %d items", len(responseItems))
		t.dataChannel <- responseItems
	})

	t.client.On("new_token_states", func(c *socketio.Channel, msg string) {
		var responseItems []ResponseItem
		if err := json.Unmarshal([]byte(msg), &responseItems); err != nil {
			log.Printf("Error parsing new_token_states: %v", err)
			return
		}
		
		t.dataChannel <- responseItems
	})

	go t.dataProcessor(ctx)
	go t.subscriptionManager(ctx)

	return nil
}

func (t *SocketIOTransport) Stop() {
	close(t.shutdownChannel)
	if t.client != nil {
		t.client.Close()
	}
}

func (t *SocketIOTransport) setConnected(status bool) {
	t.connectionMutex.Lock()
	defer t.connectionMutex.Unlock()
	t.isConnected = status
}

func (t *SocketIOTransport) isConnectedSafe() bool {
	t.connectionMutex.RLock()
	defer t.connectionMutex.RUnlock()
	return t.isConnected
}

func (t *SocketIOTransport) isClientActive() bool {
	return t.isConnectedSafe()
}

func (t *SocketIOTransport) dataProcessor(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.shutdownChannel:
			return
		case data := <-t.dataChannel:
			t.parseResponseData(data)
		}
	}
}

func (t *SocketIOTransport) parseResponseData(data []ResponseItem) {
	t.responseCacheMutex.Lock()
	defer t.responseCacheMutex.Unlock()

	for _, row := range data {
		key := fmt.Sprintf("%s/%s", row.BaseSymbol, row.QuoteSymbol)
		
		response := PriceResponse{}
		response.Params.Base = row.BaseSymbol
		response.Params.Quote = row.QuoteSymbol
		response.Response.Data.Result = row.Price
		response.Response.Result = row.Price
		response.Response.Timestamps.ProviderDataStreamEstablishedUnixMs = t.dataStreamEstablished
		response.Response.Timestamps.ProviderDataReceivedUnixMs = time.Now().UnixMilli()
		response.Response.Timestamps.ProviderIndicatedTimeUnixMs = row.ProcessTimestamp * 1000
		
		t.responseCache[key] = response
	}
}

func (t *SocketIOTransport) subscriptionManager(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.shutdownChannel:
			return
		case <-t.subscriptionChannel:
		}
	}
}

func (t *SocketIOTransport) AddSubscriptions(subscriptions []string) error {
	return t.emitAndUpdateSubscriptions("subscribe", subscriptions)
}

func (t *SocketIOTransport) RemoveSubscriptions(subscriptions []string) error {
	return t.emitAndUpdateSubscriptions("unsubscribe", subscriptions)
}

func (t *SocketIOTransport) emitAndUpdateSubscriptions(event string, subscriptions []string) error {
	if !t.isConnectedSafe() {
		return fmt.Errorf("not connected")
	}

	responseChan := make(chan SubscriptionResponse, 1)
	errorChan := make(chan error, 1)
	
	responseHandler := fmt.Sprintf("%s_response_%d", event, time.Now().UnixNano())
	
	t.client.On(responseHandler, func(c *socketio.Channel, msg string) {
		var subscribeResponse SubscriptionResponse
		if err := json.Unmarshal([]byte(msg), &subscribeResponse); err != nil {
			errorChan <- fmt.Errorf("failed to parse subscription response: %w", err)
			return
		}

		if subscribeResponse.Status == "ok" {
			log.Printf("Subscription update successful: %v", subscribeResponse)
			responseChan <- subscribeResponse
		} else {
			errorChan <- fmt.Errorf("subscription update failed: %v", subscribeResponse)
		}
		
	})

	err := t.client.Emit(event, subscriptions)
	if err != nil {
		return fmt.Errorf("failed to emit %s event: %w", event, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), t.config.APITimeout)
	defer cancel()
	
	select {
	case <-ctx.Done():
		return fmt.Errorf("timed out waiting for subscription confirmation")
	case err := <-errorChan:
		t.subscriptionsMutex.Lock()
		t.confirmedSubscriptions = nil
		t.subscriptionsMutex.Unlock()
		return err
	case response := <-responseChan:
		t.subscriptionsMutex.Lock()
		t.confirmedSubscriptions = make(map[string]bool)
		for _, sub := range response.SubscriptionsAfterUpdate {
			t.confirmedSubscriptions[sub] = true
		}
		t.subscriptionsMutex.Unlock()
		
		select {
		case t.subscriptionChannel <- struct{}{}:
		default:
		}
		
		return nil
	}
}

func (t *SocketIOTransport) ReconcileSubscriptions(desired []string) error {
	t.subscriptionsMutex.Lock()
	
	if t.confirmedSubscriptions == nil {
		t.subscriptionsMutex.Unlock()
		if err := t.AddSubscriptions([]string{}); err != nil {
			return fmt.Errorf("unable to get current subscriptions: %w", err)
		}
		t.subscriptionsMutex.Lock()
	}
	
	if t.confirmedSubscriptions == nil {
		t.subscriptionsMutex.Unlock()
		return fmt.Errorf("unable to get current subscriptions")
	}
	
	desiredMap := make(map[string]bool)
	for _, sub := range desired {
		desiredMap[sub] = true
	}
	
	var toAdd, toRemove []string
	
	for sub := range desiredMap {
		if !t.confirmedSubscriptions[sub] {
			toAdd = append(toAdd, sub)
		}
	}
	
	for sub := range t.confirmedSubscriptions {
		if !desiredMap[sub] {
			toRemove = append(toRemove, sub)
		}
	}
	
	t.subscriptionsMutex.Unlock()
	
	if len(toAdd) > 0 || len(toRemove) > 0 {
		log.Printf("Changing subscriptions - to add: %v, to remove: %v", toAdd, toRemove)
	}
	
	if len(toAdd) > 0 {
		if err := t.AddSubscriptions(toAdd); err != nil {
			return fmt.Errorf("failed to add subscriptions: %w", err)
		}
	}
	
	if len(toRemove) > 0 {
		if err := t.RemoveSubscriptions(toRemove); err != nil {
			return fmt.Errorf("failed to remove subscriptions: %w", err)
		}
	}
	
	return nil
}

func (t *SocketIOTransport) GetPriceData(base, quote string) (PriceResponse, bool) {
	t.responseCacheMutex.RLock()
	defer t.responseCacheMutex.RUnlock()
	
	key := fmt.Sprintf("%s/%s", base, quote)
	response, exists := t.responseCache[key]
	return response, exists
}

func (t *SocketIOTransport) GetAllPriceData() map[string]PriceResponse {
	t.responseCacheMutex.RLock()
	defer t.responseCacheMutex.RUnlock()
	
	result := make(map[string]PriceResponse, len(t.responseCache))
	for k, v := range t.responseCache {
		result[k] = v
	}
	
	return result
}

func (t *SocketIOTransport) GetErrorChannel() <-chan error {
	return t.errorChannel
}

func ExampleUsage() {
	config := Config{
		APIKey:                "your-api-key",
		WSAPIEndpoint:         "wss://api.example.com",
		APITimeout:            time.Second * 30,
		BackgroundExecuteMS:   time.Second * 5,
		WarmupSubscriptionTTL: 300,
	}
	
	transport := NewSocketIOTransport(config)
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
	
	time.Sleep(time.Second * 10)
	
	if priceData, exists := transport.GetPriceData("BTC", "USD"); exists {
		log.Printf("BTC/USD price: %f", priceData.Response.Result)
	}
	
	allPrices := transport.GetAllPriceData()
	for pair, data := range allPrices {
		log.Printf("%s price: %f", pair, data.Response.Result)
	}
	
	newDesiredPairs := []string{"BTC/USD", "SOL/USD", "AVAX/USD"}
	if err := transport.ReconcileSubscriptions(newDesiredPairs); err != nil {
		log.Printf("Failed to reconcile subscriptions: %v", err)
	}
	
	time.Sleep(time.Second * 10)
}
