package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"math"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/lib/pq"
	"github.com/redis/go-redis/v9"
)

type App struct {
	db                 *sql.DB
	redis              *redis.Client
	symbols            []string
	klineStream        string
	portfolioStream    string
	portfolioLatestKey string

	connections int64
	clientsMu    sync.RWMutex
	clients      map[*websocket.Conn]struct{}
	upgrader     websocket.Upgrader
}

type PricePoint struct {
	Symbol string    `json:"symbol"`
	Time   time.Time `json:"time"`
	Close  float64   `json:"close"`
}

type ForecastPoint struct {
	Symbol      string     `json:"symbol"`
	Time        time.Time  `json:"time"`
	GarchVol    *float64   `json:"garch_vol"`
	XgbVol      *float64   `json:"xgb_vol"`
	RealizedVol *float64   `json:"realized_vol"`
}

type PortfolioPoint struct {
	Time           time.Time          `json:"time"`
	Weights        map[string]float64 `json:"weights"`
	PortfolioVol   *float64           `json:"portfolio_vol"`
	SharpeProxy    *float64           `json:"sharpe_proxy"`
	Equity         *float64           `json:"equity"`
	TransactionCost *float64          `json:"transaction_cost"`
	Turnover       *float64           `json:"turnover"`
	PeriodPnL      *float64           `json:"period_pnl"`
	CumulativePnL  *float64           `json:"cumulative_pnl"`
}

type ModelMetricPoint struct {
	Symbol       string     `json:"symbol"`
	Time         time.Time  `json:"time"`
	GarchMSE     *float64   `json:"garch_mse"`
	GarchQLIKE   *float64   `json:"garch_qlike"`
	XgbMSE       *float64   `json:"xgb_mse"`
	XgbQLIKE     *float64   `json:"xgb_qlike"`
	GarchSamples int        `json:"garch_samples"`
	XgbSamples   int        `json:"xgb_samples"`
	WindowPoints int        `json:"window_points"`
}

type RiskMetrics struct {
	AsOf             *time.Time `json:"as_of"`
	PortfolioVol     *float64   `json:"portfolio_vol"`
	SharpeProxy      *float64   `json:"sharpe_proxy"`
	RealizedSharpe   float64    `json:"realized_sharpe"`
	MaxDrawdown      float64    `json:"max_drawdown"`
	AvgTurnover      float64    `json:"avg_turnover"`
	AvgFeeDrag       float64    `json:"avg_fee_drag"`
	Observations     int        `json:"observations"`
	WebsocketClients int64      `json:"active_websocket_connections"`
}

type wsEnvelope struct {
	Topic string      `json:"topic"`
	Data  interface{} `json:"data"`
}

func mustGetEnv(name, fallback string) string {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	return value
}

func parseSymbols(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		sym := strings.ToUpper(strings.TrimSpace(part))
		if sym != "" {
			out = append(out, sym)
		}
	}
	if len(out) == 0 {
		return []string{"BTCUSDT", "ETHUSDT", "SOLUSDT"}
	}
	return out
}

func ptrFloat(v sql.NullFloat64) *float64 {
	if !v.Valid {
		return nil
	}
	f := v.Float64
	return &f
}

func clampLimit(value string, fallback, max int) int {
	if value == "" {
		return fallback
	}
	n, err := strconv.Atoi(value)
	if err != nil || n <= 0 {
		return fallback
	}
	if n > max {
		return max
	}
	return n
}

func main() {
	ctx := context.Background()
	dsn := mustGetEnv("TIMESCALEDB_DSN", "postgresql://postgres:postgres@timescaledb:5432/portfolio?sslmode=disable")
	redisAddr := mustGetEnv("REDIS_ADDR", "redis:6379")
	symbols := parseSymbols(mustGetEnv("SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT"))
	listenAddr := mustGetEnv("API_ADDR", ":8080")

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		log.Fatalf("failed to open db: %v", err)
	}
	if err := db.PingContext(ctx); err != nil {
		log.Fatalf("failed to ping db: %v", err)
	}

	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	if _, err := rdb.Ping(ctx).Result(); err != nil {
		log.Fatalf("failed to ping redis: %v", err)
	}

	app := &App{
		db:                 db,
		redis:              rdb,
		symbols:            symbols,
		klineStream:        mustGetEnv("KLINE_STREAM", "kline_stream"),
		portfolioStream:    mustGetEnv("PORTFOLIO_STREAM", "portfolio_state_stream"),
		portfolioLatestKey: mustGetEnv("PORTFOLIO_LATEST_KEY", "portfolio:latest"),
		clients:            make(map[*websocket.Conn]struct{}),
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool { return true },
		},
	}

	go app.streamRedisEvents(ctx)
	go app.streamPortfolioSnapshots(ctx)

	r := gin.Default()
	r.GET("/health", app.handleHealth)

	api := r.Group("/api/v1")
	{
		api.GET("/prices/live", app.handleLivePrices)
		api.GET("/forecasts/latest", app.handleLatestForecasts)
		api.GET("/portfolio/latest", app.handleLatestPortfolio)
		api.GET("/portfolio/equity_curve", app.handleEquityCurve)
		api.GET("/portfolio/risk_metrics", app.handleRiskMetrics)
		api.GET("/model/metrics", app.handleModelMetrics)
		api.GET("/metrics/summary", app.handleSummaryMetrics)
	}

	r.GET("/ws/live", app.handleLiveWS)

	log.Printf("API gateway listening on %s", listenAddr)
	if err := r.Run(listenAddr); err != nil {
		log.Fatalf("server error: %v", err)
	}
}

func (a *App) handleHealth(c *gin.Context) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
	defer cancel()

	dbErr := a.db.PingContext(ctx)
	_, redisErr := a.redis.Ping(ctx).Result()

	status := http.StatusOK
	if dbErr != nil || redisErr != nil {
		status = http.StatusServiceUnavailable
	}

	c.JSON(status, gin.H{
		"status":                 map[bool]string{true: "ok", false: "degraded"}[status == http.StatusOK],
		"database_ok":            dbErr == nil,
		"redis_ok":               redisErr == nil,
		"active_ws_connections":  atomic.LoadInt64(&a.connections),
		"symbols":                a.symbols,
		"timestamp":              time.Now().UTC(),
	})
}

func (a *App) handleLivePrices(c *gin.Context) {
	rows, err := a.db.QueryContext(c.Request.Context(), `
		SELECT DISTINCT ON (symbol) symbol, time, close
		FROM prices
		WHERE symbol = ANY($1)
		ORDER BY symbol, time DESC;
	`, pq.Array(a.symbols))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	defer rows.Close()

	out := make([]PricePoint, 0, len(a.symbols))
	for rows.Next() {
		var row PricePoint
		if err := rows.Scan(&row.Symbol, &row.Time, &row.Close); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		out = append(out, row)
	}
	c.JSON(http.StatusOK, gin.H{"data": out})
}

func (a *App) handleLatestForecasts(c *gin.Context) {
	rows, err := a.db.QueryContext(c.Request.Context(), `
		SELECT DISTINCT ON (symbol)
			symbol, time, garch_vol, xgb_vol, realized_vol
		FROM forecasts
		WHERE symbol = ANY($1)
		ORDER BY symbol, time DESC;
	`, pq.Array(a.symbols))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	defer rows.Close()

	out := make([]ForecastPoint, 0, len(a.symbols))
	for rows.Next() {
		var row ForecastPoint
		var garch, xgb, rv sql.NullFloat64
		if err := rows.Scan(&row.Symbol, &row.Time, &garch, &xgb, &rv); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		row.GarchVol = ptrFloat(garch)
		row.XgbVol = ptrFloat(xgb)
		row.RealizedVol = ptrFloat(rv)
		out = append(out, row)
	}
	c.JSON(http.StatusOK, gin.H{"data": out})
}

func (a *App) handleLatestPortfolio(c *gin.Context) {
	data, err := a.fetchLatestPortfolio(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if data == nil {
		c.JSON(http.StatusOK, gin.H{"data": nil})
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": data})
}

func (a *App) handleEquityCurve(c *gin.Context) {
	limit := clampLimit(c.Query("limit"), 200, 5000)

	rows, err := a.db.QueryContext(c.Request.Context(), `
		SELECT time, equity, period_pnl, cumulative_pnl, transaction_cost, turnover
		FROM portfolio_state
		ORDER BY time DESC
		LIMIT $1;
	`, limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	defer rows.Close()

	type point struct {
		Time            time.Time `json:"time"`
		Equity          float64   `json:"equity"`
		PeriodPnL       float64   `json:"period_pnl"`
		CumulativePnL   float64   `json:"cumulative_pnl"`
		TransactionCost float64   `json:"transaction_cost"`
		Turnover        float64   `json:"turnover"`
	}

	out := make([]point, 0, limit)
	for rows.Next() {
		var t time.Time
		var equity, periodPnL, cumulativePnL, tc, turnover sql.NullFloat64
		if err := rows.Scan(&t, &equity, &periodPnL, &cumulativePnL, &tc, &turnover); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		out = append(out, point{
			Time:            t,
			Equity:          valueOrZero(equity),
			PeriodPnL:       valueOrZero(periodPnL),
			CumulativePnL:   valueOrZero(cumulativePnL),
			TransactionCost: valueOrZero(tc),
			Turnover:        valueOrZero(turnover),
		})
	}

	for left, right := 0, len(out)-1; left < right; left, right = left+1, right-1 {
		out[left], out[right] = out[right], out[left]
	}

	c.JSON(http.StatusOK, gin.H{"data": out})
}

func (a *App) handleRiskMetrics(c *gin.Context) {
	limit := clampLimit(c.Query("lookback"), 90, 2000)

	rows, err := a.db.QueryContext(c.Request.Context(), `
		SELECT time, equity, portfolio_vol, sharpe_proxy, turnover, transaction_cost
		FROM portfolio_state
		ORDER BY time DESC
		LIMIT $1;
	`, limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	defer rows.Close()

	type rowPoint struct {
		time          time.Time
		equity        float64
		portfolioVol  sql.NullFloat64
		sharpeProxy   sql.NullFloat64
		turnover      float64
		transaction   float64
	}

	series := make([]rowPoint, 0, limit)
	for rows.Next() {
		var p rowPoint
		var eq, turnover, tc sql.NullFloat64
		if err := rows.Scan(&p.time, &eq, &p.portfolioVol, &p.sharpeProxy, &turnover, &tc); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		p.equity = valueOrZero(eq)
		p.turnover = valueOrZero(turnover)
		p.transaction = valueOrZero(tc)
		series = append(series, p)
	}

	if len(series) == 0 {
		c.JSON(http.StatusOK, gin.H{"data": RiskMetrics{WebsocketClients: atomic.LoadInt64(&a.connections)}})
		return
	}

	for left, right := 0, len(series)-1; left < right; left, right = left+1, right-1 {
		series[left], series[right] = series[right], series[left]
	}

	returns := make([]float64, 0, len(series)-1)
	peak := series[0].equity
	maxDrawdown := 0.0
	turnoverSum := 0.0
	feeDragSum := 0.0
	for i := 0; i < len(series); i++ {
		eq := series[i].equity
		if eq > peak {
			peak = eq
		}
		if peak > 0 {
			dd := (peak - eq) / peak
			if dd > maxDrawdown {
				maxDrawdown = dd
			}
		}
		turnoverSum += series[i].turnover
		if eq > 0 {
			feeDragSum += series[i].transaction / eq
		}
		if i > 0 && series[i-1].equity > 0 {
			returns = append(returns, (eq/series[i-1].equity)-1.0)
		}
	}

	realizedSharpe := 0.0
	if len(returns) > 1 {
		mean := average(returns)
		std := stdDev(returns)
		if std > 1e-12 {
			realizedSharpe = mean / std
		}
	}

	latest := series[len(series)-1]
	result := RiskMetrics{
		AsOf:             &latest.time,
		PortfolioVol:     ptrFloat(latest.portfolioVol),
		SharpeProxy:      ptrFloat(latest.sharpeProxy),
		RealizedSharpe:   realizedSharpe,
		MaxDrawdown:      maxDrawdown,
		AvgTurnover:      turnoverSum / float64(len(series)),
		AvgFeeDrag:       feeDragSum / float64(len(series)),
		Observations:     len(series),
		WebsocketClients: atomic.LoadInt64(&a.connections),
	}
	c.JSON(http.StatusOK, gin.H{"data": result})
}

func (a *App) handleModelMetrics(c *gin.Context) {
	rows, err := a.db.QueryContext(c.Request.Context(), `
		SELECT DISTINCT ON (symbol)
			symbol, time,
			garch_mse, garch_qlike,
			xgb_mse, xgb_qlike,
			garch_samples, xgb_samples, window_points
		FROM model_metrics
		WHERE symbol = ANY($1)
		ORDER BY symbol, time DESC;
	`, pq.Array(a.symbols))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	defer rows.Close()

	out := make([]ModelMetricPoint, 0, len(a.symbols))
	for rows.Next() {
		var row ModelMetricPoint
		var gmse, gqlike, xmse, xqlike sql.NullFloat64
		if err := rows.Scan(&row.Symbol, &row.Time, &gmse, &gqlike, &xmse, &xqlike, &row.GarchSamples, &row.XgbSamples, &row.WindowPoints); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		row.GarchMSE = ptrFloat(gmse)
		row.GarchQLIKE = ptrFloat(gqlike)
		row.XgbMSE = ptrFloat(xmse)
		row.XgbQLIKE = ptrFloat(xqlike)
		out = append(out, row)
	}
	c.JSON(http.StatusOK, gin.H{"data": out})
}

func (a *App) handleSummaryMetrics(c *gin.Context) {
	var latestInferenceLatencyMs sql.NullFloat64
	_ = a.db.QueryRowContext(c.Request.Context(), `
		SELECT AVG(EXTRACT(EPOCH FROM (NOW() - time)) * 1000.0)
		FROM forecasts
		WHERE time >= NOW() - INTERVAL '30 minute';
	`).Scan(&latestInferenceLatencyMs)

	riskCtx := c.Request.Context()
	risk, err := a.computeRiskMetrics(riskCtx, 90)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	modelRows, err := a.fetchLatestModelMetrics(riskCtx)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"active_websocket_connections": atomic.LoadInt64(&a.connections),
		"inference_latency_ms":         ptrFloat(latestInferenceLatencyMs),
		"rolling_model_metrics":        modelRows,
		"portfolio_risk":              risk,
		"timestamp":                   time.Now().UTC(),
	})
}

func (a *App) handleLiveWS(c *gin.Context) {
	conn, err := a.upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		return
	}

	a.registerClient(conn)
	defer a.unregisterClient(conn)

	_ = conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	conn.SetPongHandler(func(string) error {
		_ = conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})

	if portfolio, err := a.fetchLatestPortfolio(c.Request.Context()); err == nil && portfolio != nil {
		_ = conn.WriteJSON(wsEnvelope{Topic: "portfolio", Data: portfolio})
	}

	errCh := make(chan error, 1)
	go func() {
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				errCh <- err
				return
			}
		}
	}()

	ticker := time.NewTicker(20 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case err := <-errCh:
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("websocket read error: %v", err)
			}
			return
		case <-ticker.C:
			if err := conn.WriteControl(websocket.PingMessage, []byte("ping"), time.Now().Add(5*time.Second)); err != nil {
				return
			}
		}
	}
}

func (a *App) streamRedisEvents(ctx context.Context) {
	lastKlineID := "$"
	lastPortfolioID := "$"
	for {
		streams, err := a.redis.XRead(ctx, &redis.XReadArgs{
			Streams: []string{a.klineStream, lastKlineID, a.portfolioStream, lastPortfolioID},
			Count:   200,
			Block:   2 * time.Second,
		}).Result()
		if err != nil {
			if err == redis.Nil {
				continue
			}
			log.Printf("redis stream read error: %v", err)
			time.Sleep(1 * time.Second)
			continue
		}
		for _, stream := range streams {
			for _, msg := range stream.Messages {
				if stream.Stream == a.klineStream {
					lastKlineID = msg.ID
					a.broadcast(wsEnvelope{Topic: "price", Data: msg.Values})
				} else if stream.Stream == a.portfolioStream {
					lastPortfolioID = msg.ID
					a.broadcast(wsEnvelope{Topic: "portfolio", Data: msg.Values})
				}
			}
		}
	}
}

func (a *App) streamPortfolioSnapshots(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			portfolio, err := a.fetchLatestPortfolio(ctx)
			if err != nil || portfolio == nil {
				continue
			}
			a.broadcast(wsEnvelope{Topic: "portfolio_snapshot", Data: portfolio})
		}
	}
}

func (a *App) broadcast(payload wsEnvelope) {
	a.clientsMu.RLock()
	dead := make([]*websocket.Conn, 0)
	for conn := range a.clients {
		_ = conn.SetWriteDeadline(time.Now().Add(3 * time.Second))
		if err := conn.WriteJSON(payload); err != nil {
			dead = append(dead, conn)
		}
	}
	a.clientsMu.RUnlock()

	for _, conn := range dead {
		a.unregisterClient(conn)
	}
}

func (a *App) registerClient(conn *websocket.Conn) {
	a.clientsMu.Lock()
	a.clients[conn] = struct{}{}
	a.clientsMu.Unlock()
	atomic.AddInt64(&a.connections, 1)
}

func (a *App) unregisterClient(conn *websocket.Conn) {
	a.clientsMu.Lock()
	if _, exists := a.clients[conn]; exists {
		delete(a.clients, conn)
		atomic.AddInt64(&a.connections, -1)
	}
	a.clientsMu.Unlock()
	_ = conn.Close()
}

func (a *App) fetchLatestPortfolio(ctx context.Context) (*PortfolioPoint, error) {
	if raw, err := a.redis.Get(ctx, a.portfolioLatestKey).Result(); err == nil && raw != "" {
		var payload map[string]interface{}
		if err := json.Unmarshal([]byte(raw), &payload); err == nil {
			data := &PortfolioPoint{Weights: map[string]float64{}}
			if v, ok := payload["time"].(string); ok {
				if ts, err := time.Parse(time.RFC3339, v); err == nil {
					data.Time = ts
				}
			}
			if weights, ok := payload["weights"].(map[string]interface{}); ok {
				for symbol, w := range weights {
					if fv, ok := w.(float64); ok {
						data.Weights[symbol] = fv
					}
				}
			}
			data.PortfolioVol = asFloatPtr(payload["portfolio_vol"])
			data.SharpeProxy = asFloatPtr(payload["sharpe_proxy"])
			data.Equity = asFloatPtr(payload["equity"])
			data.TransactionCost = asFloatPtr(payload["transaction_cost"])
			data.Turnover = asFloatPtr(payload["turnover"])
			data.PeriodPnL = asFloatPtr(payload["period_pnl"])
			data.CumulativePnL = asFloatPtr(payload["cumulative_pnl"])
			return data, nil
		}
	}

	row := a.db.QueryRowContext(ctx, `
		SELECT time, weights, portfolio_vol, sharpe_proxy, equity, transaction_cost, turnover, period_pnl, cumulative_pnl
		FROM portfolio_state
		ORDER BY time DESC
		LIMIT 1;
	`)

	var out PortfolioPoint
	var weightsRaw []byte
	var pVol, sProxy, equity, txCost, turnover, periodPnL, cumulativePnL sql.NullFloat64
	if err := row.Scan(&out.Time, &weightsRaw, &pVol, &sProxy, &equity, &txCost, &turnover, &periodPnL, &cumulativePnL); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	if len(weightsRaw) > 0 {
		_ = json.Unmarshal(weightsRaw, &out.Weights)
	}
	if out.Weights == nil {
		out.Weights = map[string]float64{}
	}
	out.PortfolioVol = ptrFloat(pVol)
	out.SharpeProxy = ptrFloat(sProxy)
	out.Equity = ptrFloat(equity)
	out.TransactionCost = ptrFloat(txCost)
	out.Turnover = ptrFloat(turnover)
	out.PeriodPnL = ptrFloat(periodPnL)
	out.CumulativePnL = ptrFloat(cumulativePnL)
	return &out, nil
}

func (a *App) computeRiskMetrics(ctx context.Context, lookback int) (RiskMetrics, error) {
	rows, err := a.db.QueryContext(ctx, `
		SELECT time, equity, portfolio_vol, sharpe_proxy, turnover, transaction_cost
		FROM portfolio_state
		ORDER BY time DESC
		LIMIT $1;
	`, lookback)
	if err != nil {
		return RiskMetrics{}, err
	}
	defer rows.Close()

	type rowPoint struct {
		time       time.Time
		equity     float64
		pvol       sql.NullFloat64
		sharpe     sql.NullFloat64
		turnover   float64
		txCost     float64
	}
	series := make([]rowPoint, 0, lookback)
	for rows.Next() {
		var r rowPoint
		var eq, turnover, tx sql.NullFloat64
		if err := rows.Scan(&r.time, &eq, &r.pvol, &r.sharpe, &turnover, &tx); err != nil {
			return RiskMetrics{}, err
		}
		r.equity = valueOrZero(eq)
		r.turnover = valueOrZero(turnover)
		r.txCost = valueOrZero(tx)
		series = append(series, r)
	}

	result := RiskMetrics{WebsocketClients: atomic.LoadInt64(&a.connections)}
	if len(series) == 0 {
		return result, nil
	}
	for left, right := 0, len(series)-1; left < right; left, right = left+1, right-1 {
		series[left], series[right] = series[right], series[left]
	}

	returns := make([]float64, 0, len(series)-1)
	peak := series[0].equity
	maxDrawdown := 0.0
	turnoverSum := 0.0
	feeDragSum := 0.0
	for i := 0; i < len(series); i++ {
		eq := series[i].equity
		if eq > peak {
			peak = eq
		}
		if peak > 0 {
			dd := (peak - eq) / peak
			if dd > maxDrawdown {
				maxDrawdown = dd
			}
		}
		turnoverSum += series[i].turnover
		if eq > 0 {
			feeDragSum += series[i].txCost / eq
		}
		if i > 0 && series[i-1].equity > 0 {
			returns = append(returns, (eq/series[i-1].equity)-1.0)
		}
	}

	realizedSharpe := 0.0
	if len(returns) > 1 {
		mean := average(returns)
		std := stdDev(returns)
		if std > 1e-12 {
			realizedSharpe = mean / std
		}
	}

	latest := series[len(series)-1]
	result.AsOf = &latest.time
	result.PortfolioVol = ptrFloat(latest.pvol)
	result.SharpeProxy = ptrFloat(latest.sharpe)
	result.RealizedSharpe = realizedSharpe
	result.MaxDrawdown = maxDrawdown
	result.AvgTurnover = turnoverSum / float64(len(series))
	result.AvgFeeDrag = feeDragSum / float64(len(series))
	result.Observations = len(series)
	return result, nil
}

func (a *App) fetchLatestModelMetrics(ctx context.Context) ([]ModelMetricPoint, error) {
	rows, err := a.db.QueryContext(ctx, `
		SELECT DISTINCT ON (symbol)
			symbol, time,
			garch_mse, garch_qlike,
			xgb_mse, xgb_qlike,
			garch_samples, xgb_samples, window_points
		FROM model_metrics
		WHERE symbol = ANY($1)
		ORDER BY symbol, time DESC;
	`, pq.Array(a.symbols))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]ModelMetricPoint, 0, len(a.symbols))
	for rows.Next() {
		var row ModelMetricPoint
		var gmse, gqlike, xmse, xqlike sql.NullFloat64
		if err := rows.Scan(&row.Symbol, &row.Time, &gmse, &gqlike, &xmse, &xqlike, &row.GarchSamples, &row.XgbSamples, &row.WindowPoints); err != nil {
			return nil, err
		}
		row.GarchMSE = ptrFloat(gmse)
		row.GarchQLIKE = ptrFloat(gqlike)
		row.XgbMSE = ptrFloat(xmse)
		row.XgbQLIKE = ptrFloat(xqlike)
		out = append(out, row)
	}
	return out, nil
}

func average(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	total := 0.0
	for _, value := range values {
		total += value
	}
	return total / float64(len(values))
}

func stdDev(values []float64) float64 {
	if len(values) < 2 {
		return 0
	}
	mean := average(values)
	sum := 0.0
	for _, value := range values {
		d := value - mean
		sum += d * d
	}
	variance := sum / float64(len(values)-1)
	if variance < 0 {
		return 0
	}
	return math.Sqrt(variance)
}

func asFloatPtr(value interface{}) *float64 {
	switch v := value.(type) {
	case float64:
		f := v
		return &f
	case float32:
		f := float64(v)
		return &f
	case int:
		f := float64(v)
		return &f
	case int64:
		f := float64(v)
		return &f
	case json.Number:
		if f, err := v.Float64(); err == nil {
			return &f
		}
	case string:
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return &f
		}
	}
	return nil
}

func valueOrZero(v sql.NullFloat64) float64 {
	if !v.Valid {
		return 0
	}
	return v.Float64
}

