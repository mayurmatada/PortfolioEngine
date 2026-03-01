package main

import (
	"context"
	"encoding/json"
	"log"
	"net/url"
	"os"
	"strings"

	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
)

func main() {
	// ── Redis (needed for both backfill and streaming) ───────────────
	redisAddr := os.Getenv("REDIS_ADDR")
	rdb := redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		Password: "",
		DB:       0,
	})

	ctx := context.Background()

	pong, err := rdb.Ping(ctx).Result()
	if err != nil {
		log.Fatalf("Failed to connect to Redis: %v", err)
	}
	log.Printf("Connected to Redis: %s\n", pong)
	defer rdb.Close()

	// ── Connect WebSocket FIRST so no live candle is missed ──────────
	streams := make([]string, len(symbols))
	for i, s := range symbols {
		streams[i] = strings.ToLower(s) + "@kline_1m"
	}

	u := url.URL{
		Scheme:   "wss",
		Host:     "fstream.binance.com",
		Path:     "/stream",
		RawQuery: "streams=" + strings.Join(streams, "/"),
	}

	conn, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		log.Fatal("dial error:", err)
	}
	defer conn.Close()

	log.Println("Connected to Binance kline stream (buffering while backfill runs)…")

	// ── Run historical backfill concurrently ─────────────────────────
	// The WebSocket is already connected, so live candles are queued in
	// the kernel TCP buffer while we push historical data. Any overlap
	// between the tail of the REST data and early WebSocket candles is
	// deduplicated by the downstream DB layer (ON CONFLICT DO NOTHING).
	go backfillHistorical(ctx, rdb)

	// ── WebSocket consume loop ───────────────────────────────────────
	for {
		_, message, err := conn.ReadMessage()
		if err != nil {
			log.Println("read error:", err)
			return
		}

		var frame WSFrame
		if err := json.Unmarshal(message, &frame); err != nil {
			log.Println("Failed to unmarshall")
		}

		k := frame.Data.K

		//log.Printf("Kline cur time: %d, Kline close time: %d", frame.Data.EventTime, k.CloseTime)
		if k.IsClosed {
			log.Println("Kline is closed!")
			log.Printf(
				"%s | %d | O:%s H:%s L:%s C:%s V:%s\n",
				k.Symbol,
				k.OpenTime,
				k.Open,
				k.High,
				k.Low,
				k.Close,
				k.Volume,
			)

			klineJSON, err := json.Marshal(k)
			if err != nil {
				log.Printf("Failed to marshal kline: %v", err)
				continue
			}

			_, err = rdb.XAdd(ctx, &redis.XAddArgs{
				Stream: "kline_stream",
				MaxLen: 10000, // cap stream size; ~ keeps last ~7 days of 1-min candles for 3 symbols
				Approx: true,  // use ~ (approximate) trimming for performance
				Values: map[string]interface{}{
					"symbol":    k.Symbol,
					"open_time": k.OpenTime,
					"kline":     string(klineJSON),
				},
			}).Result()
			if err != nil {
				log.Printf("Failed to add to Redis stream: %v", err)
			}
		}
	}
}
