package main

// Symbols to track — used by both the REST backfill and the WebSocket stream.
var symbols = []string{"BTCUSDT", "ETHUSDT", "SOLUSDT"}

// Kline represents a single candlestick received from Binance (WebSocket or REST).
type Kline struct {
	OpenTime                 int64  `json:"t"`
	CloseTime                int64  `json:"T"`
	Symbol                   string `json:"s"`
	Interval                 string `json:"i"`
	FirstTradeID             int64  `json:"f"`
	LastTradeID              int64  `json:"L"`
	Open                     string `json:"o"`
	Close                    string `json:"c"`
	High                     string `json:"h"`
	Low                      string `json:"l"`
	Volume                   string `json:"v"`
	NumberOfTrades           int64  `json:"n"`
	IsClosed                 bool   `json:"x"`
	QuoteAssetVolume         string `json:"q"`
	TakerBuyBaseAssetVolume  string `json:"V"`
	TakerBuyQuoteAssetVolume string `json:"Q"`
	Ignore                   string `json:"B"`
}

// KlineEvent is the wrapper Binance sends for each kline update.
type KlineEvent struct {
	EventType string `json:"e"`
	EventTime int64  `json:"E"`
	Symbol    string `json:"s"`
	K         Kline  `json:"k"`
}

// WSFrame is the top-level envelope for combined-stream WebSocket messages.
type WSFrame struct {
	Stream string     `json:"stream"`
	Data   KlineEvent `json:"data"`
}
