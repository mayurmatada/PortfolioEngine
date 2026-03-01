package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/redis/go-redis/v9"
)

// ---------------------------------------------------------------------------
// Historical backfill via Binance Futures REST API
// GET /fapi/v1/klines  —  https://developers.binance.com/docs/derivatives/
//   usds-margined-futures/market-data/rest-api/Kline-Candlestick-Data
// ---------------------------------------------------------------------------

// backfillHistorical fetches up to 1500 closed 1-minute candles per symbol
// from the Binance Futures REST API and publishes them to the Redis stream
// so downstream services (feature engineering) can warm up immediately.
func backfillHistorical(ctx context.Context, rdb *redis.Client) {
	const (
		baseURL  = "https://fapi.binance.com/fapi/v1/klines"
		interval = "1m"
		limit    = 1500
	)

	client := &http.Client{Timeout: 30 * time.Second}

	for _, symbol := range symbols {
		// Set endTime to just before the current minute so every returned
		// candle is guaranteed to be closed.
		endTime := time.Now().UTC().Truncate(time.Minute).UnixMilli() - 1

		apiURL := fmt.Sprintf("%s?symbol=%s&interval=%s&limit=%d&endTime=%d",
			baseURL, symbol, interval, limit, endTime)

		log.Printf("[backfill] Fetching up to %d historical candles for %s …", limit, symbol)

		body, err := retryableGet(client, apiURL)
		if err != nil {
			log.Printf("[backfill] giving up on %s: %v", symbol, err)
			continue
		}

		// The response is an array of arrays. Each inner array:
		//  [0] OpenTime  (int64 ms)     [1] Open   (string)
		//  [2] High      (string)       [3] Low    (string)
		//  [4] Close     (string)       [5] Volume (string)
		//  [6] CloseTime (int64 ms)     [7] QuoteAssetVolume (string)
		//  [8] NumTrades (int64)        [9] TakerBuyBaseVol  (string)
		// [10] TakerBuyQuoteVol (string) [11] Ignore (string)
		var raw [][]json.RawMessage
		if err := json.Unmarshal(body, &raw); err != nil {
			log.Printf("[backfill] JSON parse error for %s: %v", symbol, err)
			continue
		}

		published := 0
		for _, row := range raw {
			if len(row) < 12 {
				continue
			}

			var openTime, closeTime, numTrades int64
			var open, high, low, close, volume string
			var quoteVol, takerBuyBase, takerBuyQuote, ignore string

			json.Unmarshal(row[0], &openTime)
			json.Unmarshal(row[1], &open)
			json.Unmarshal(row[2], &high)
			json.Unmarshal(row[3], &low)
			json.Unmarshal(row[4], &close)
			json.Unmarshal(row[5], &volume)
			json.Unmarshal(row[6], &closeTime)
			json.Unmarshal(row[7], &quoteVol)
			json.Unmarshal(row[8], &numTrades)
			json.Unmarshal(row[9], &takerBuyBase)
			json.Unmarshal(row[10], &takerBuyQuote)
			json.Unmarshal(row[11], &ignore)

			k := Kline{
				OpenTime:                 openTime,
				CloseTime:                closeTime,
				Symbol:                   symbol,
				Interval:                 interval,
				Open:                     open,
				Close:                    close,
				High:                     high,
				Low:                      low,
				Volume:                   volume,
				NumberOfTrades:           numTrades,
				IsClosed:                 true,
				QuoteAssetVolume:         quoteVol,
				TakerBuyBaseAssetVolume:  takerBuyBase,
				TakerBuyQuoteAssetVolume: takerBuyQuote,
				Ignore:                   ignore,
			}

			klineJSON, err := json.Marshal(k)
			if err != nil {
				log.Printf("[backfill] marshal error: %v", err)
				continue
			}

			_, err = rdb.XAdd(ctx, &redis.XAddArgs{
				Stream: "kline_stream",
				MaxLen: 10000,
				Approx: true,
				Values: map[string]interface{}{
					"symbol":    k.Symbol,
					"open_time": k.OpenTime,
					"kline":     string(klineJSON),
				},
			}).Result()
			if err != nil {
				log.Printf("[backfill] Redis XADD error: %v", err)
			} else {
				published++
			}
		}

		log.Printf("[backfill] %s — published %d historical candles", symbol, published)
	}

	log.Println("[backfill] Historical backfill complete.")
}
