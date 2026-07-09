# Portfolio Engine - Real-Time Crypto Data & Portfolio Simulation Platform

A microservice-based quantitative infrastructure platform for streaming crypto data ingestion, feature engineering, ML-based volatility forecasting, and risk-aware portfolio simulation.

## Table of Contents

- [Project Overview](#project-overview)
- [High-Level Architecture](#high-level-architecture)
- [System Components](#system-components)
- [Data Flow](#data-flow)
- [Technology Stack](#technology-stack)
- [Setup & Deployment](#setup--deployment)
- [API Documentation](#api-documentation)
- [Configuration](#configuration)
- [Performance Considerations](#performance-considerations)

---

## Project Overview

This system demonstrates quantitative infrastructure engineering patterns used in algorithmic trading platforms. It implements a complete pipeline from real-time market data ingestion to ML model training and portfolio simulation.

### Key Features

- **Real-Time Data Streaming**: 1-minute Binance OHLCV data via WebSocket
- **Feature Engineering Pipeline**: Streaming computation of rolling technical indicators
- **ML Model Training**: GARCH and XGBoost volatility forecasting models
- **Portfolio Optimization**: Daily risk-aware portfolio rebalancing with transaction cost modeling
- **Production Monitoring**: Real-time metrics and model performance tracking
- **Time-Series Storage**: TimescaleDB for efficient historical data management

### Assets & Scope

- **Trading Pairs**: BTCUSDT, ETHUSDT, SOLUSDT (3 crypto spot pairs)
- **Time Granularity**: 1-minute OHLCV candles
- **Historical Horizon**: ~1 year of data storage
- **Forecasting Target**: 5-minute forward realized volatility
- **Portfolio Constraints**: Long-only, fully invested, daily rebalance, 0.1% flat fee

---

## High-Level Architecture

### System Overview

```mermaid
graph TB
    Binance["Binance WebSocket"]
    
    subgraph Ingestion["Ingestion"]
        IngSvc["Service (Go)"]
    end
    
    subgraph Streaming["Streaming"]
        Redis["Redis Streams"]
    end
    
    subgraph Storage["Storage"]
        TimescaleDB["TimescaleDB"]
    end
    
    subgraph Compute["Compute Layer"]
        FeatEng["Feature Engineering"]
        Forecasting["Forecasting"]
        Portfolio["Portfolio Engine"]
    end
    
    subgraph API["API Layer"]
        APIGateway["API Gateway"]
        Clients["Web Clients"]
    end
    
    Binance -->|WebSocket Stream| IngSvc
    IngSvc -->|Publish klines| Redis
    IngSvc -->|Write OHLCV| TimescaleDB
    
    Redis -->|Subscribe| FeatEng
    FeatEng -->|Write features| TimescaleDB
    
    TimescaleDB -->|Query| Forecasting
    Forecasting -->|Write forecasts| TimescaleDB
    Forecasting -->|Publish| Redis
    
    TimescaleDB -->|Query| Portfolio
    Portfolio -->|Publish| Redis
    Portfolio -->|Write state| TimescaleDB
    
    Redis -->|Query streams| APIGateway
    TimescaleDB -->|Query data| APIGateway
    APIGateway -->|Real-time| Clients
```

### Service Interactions & Data Flow

```mermaid
sequenceDiagram
    actor Binance
    participant Ingestion as Ingestion Service
    participant Redis
    participant FeatEng as Feature Engineering
    participant DB as TimescaleDB
    participant Forecasting
    participant Portfolio
    participant API as API Gateway
    
    Binance->>Ingestion: 1-min kline updates
    Ingestion->>Redis: Publish to kline_stream
    Ingestion->>DB: Store OHLCV
    
    FeatEng->>Redis: Consume klines
    FeatEng->>DB: Query recent candles
    FeatEng->>FeatEng: Compute rolling features
    FeatEng->>DB: Write features
    
    loop Training
        Forecasting->>DB: Query features + targets
        Forecasting->>Forecasting: Train GARCH & XGBoost
        Forecasting->>DB: Store model metadata
    end
    
    loop Inference
        Forecasting->>DB: Get latest features
        Forecasting->>Forecasting: Generate predictions
        Forecasting->>DB: Write forecasts
    end
    
    loop Daily
        Portfolio->>DB: Query prices & forecasts
        Portfolio->>Portfolio: Optimize weights
        Portfolio->>Redis: Publish portfolio state
        Portfolio->>DB: Log rebalance
    end
    
    API->>DB: Query historical data
    API->>Redis: Subscribe to streams
    API-->>Clients: Broadcast via WebSocket
```

---

## System Components

### 1. **Ingestion Service** (Go)

**Responsibilities**:
- Connects to Binance WebSocket for real-time market data
- Subscribes to 1-minute klines for all configured symbols
- Publishes closed klines to Redis Stream (`kline_stream`)
- Persists raw OHLCV data to TimescaleDB
- Handles reconnection logic and fault tolerance

**Key Features**:
- Reliable streaming ingestion with backpressure handling
- Per-symbol subscription management
- Graceful shutdown with connection cleanup
- Real-time data validation

**Output Streams**:
- `kline_stream` → Redis (JSON encoded klines)
- `prices` table → TimescaleDB (OHLCV data)

---

### 2. **Feature Engineering Service** (Python)

**Responsibilities**:
- Consumes closed klines from Redis Stream
- Computes rolling technical indicators over multiple time windows (5, 15, 30, 60 min)
- Calculates realized volatility and other statistical features
- Persists computed features to TimescaleDB
- Maintains per-symbol in-memory ring buffers for efficient windowed computation

**Features Computed**:
- Log returns and moment statistics
- Realized volatility (various estimators)
- Parkinson volatility
- Volume-weighted metrics
- High-low range statistics

**Output**:
- `features` table → TimescaleDB (rolling computed features)

**Stateless Design**:
- Minimal state: only per-symbol ring buffers for active window computation
- Easily horizontally scalable with shared Redis stream

---

### 3. **Forecasting Service** (Python)

**Responsibilities**:
- Trains volatility forecasting models on a schedule
- Performs real-time inference on new data
- Manages model versioning and metadata
- Handles backfill and partial training scenarios

**Models**:
- **GARCH Model**: Univariate volatility forecasting
- **XGBoost Model**: Multivariate feature-based predictions with gradient boosting

**Key Operations**:

1. **Training Loop**: Runs at configurable intervals (e.g., every 4 hours)
   - Queries historical feature data
   - Trains GARCH and XGBoost models per symbol
   - Validates on holdout sets
   - Stores model artifacts and metrics

2. **Inference Loop**: Runs continuously (configurable polling frequency)
   - Queries latest features for each symbol
   - Generates volatility predictions
   - Persists forecasts to TimescaleDB

**Output**:
- `forecast` table → TimescaleDB (GARCH & XGBoost predictions)
- Model artifacts → Local filesystem + metadata to DB

---

### 4. **Portfolio Engine** (Python)

**Responsibilities**:
- Daily portfolio rebalancing based on risk metrics
- Computes optimal weights using predicted volatilities
- Tracks portfolio P&L and transaction costs
- Maintains allocation history for backtest analysis

**Optimization Approach**:
- **Risk-Aware Allocation**: Inverse-volatility weighting with optimization refinement
- **Covariance Estimation**: Historical covariance with diagonal shrinkage
- **Constraints**: Long-only, fully invested, optional weight caps
- **Rebalance Timing**: Daily at configured UTC time

**Metrics Tracked**:
- Portfolio volatility
- Sharpe ratio proxy
- Equity curve
- Transaction costs (turnover × fee rate)
- Period P&L and cumulative P&L

**Output Streams**:
- `portfolio_state_stream` → Redis (daily rebalance state)
- `portfolio_state` table → TimescaleDB (historical portfolio states)

---

### 5. **API Gateway** (Go)

**Responsibilities**:
- Provides WebSocket-based real-time data feed
- Aggregates price, forecast, and portfolio data
- Manages client connections and broadcasts updates
- Supports both streaming and snapshot queries

**Endpoints**:

- **WebSocket**: `/ws` - Real-time streaming updates
  - Subscribes to multiple data streams simultaneously
  - Broadcasts price, forecast, and portfolio updates
  - Handles connection lifecycle management

- **REST Query**: (Extensible)
  - Historical price data
  - Model forecasts
  - Portfolio allocation history
  - Performance metrics

**Data Types**:
```json
{
  "price": {"symbol": "BTCUSDT", "time": "2025-01-15T10:30:00Z", "close": 45000.0},
  "forecast": {"symbol": "BTCUSDT", "garch_vol": 0.015, "xgb_vol": 0.016, "realized_vol": 0.014},
  "portfolio": {"time": "2025-01-15T00:00:00Z", "weights": {...}, "portfolio_vol": 0.020}
}
```

---

## Data Flow

### Complete Event Pipeline

```mermaid
graph LR
    A["Raw Kline<br/>Binance"]
    B["Ingestion<br/>Service"]
    C["Redis Stream<br/>kline_stream"]
    D["Feature Eng<br/>Ring Buffer"]
    E["Compute<br/>Features"]
    F["TimescaleDB<br/>features"]
    
    G["Forecasting<br/>Training"]
    H["Model<br/>Inference"]
    I["TimescaleDB<br/>forecast"]
    
    J["TimescaleDB<br/>prices"]
    K["Portfolio<br/>Optimizer"]
    L["Optimal<br/>Weights"]
    M["Redis Stream<br/>portfolio_state"]
    
    N["API Gateway"]
    O["Dashboard"]
    
    A --> B
    B --> C
    B --> J
    C --> D
    D --> E
    E --> F
    
    F --> G
    G --> H
    H --> I
    
    I --> K
    J --> K
    K --> L
    L --> M
    
    M --> N
    F --> N
    J --> N
    N --> O
```

### Data Storage Schema (TimescaleDB)

```mermaid
graph TB
    subgraph "TimescaleDB Schema"
        Prices["prices<br/>- timestamp<br/>- symbol<br/>- open, high, low, close<br/>- volume"]
        Features["features<br/>- timestamp<br/>- symbol<br/>- realized_vol_5m, 15m, 30m, 60m<br/>- parkinson_vol<br/>- volume_sma, returns_kurtosis"]
        Forecast["forecast<br/>- timestamp<br/>- symbol<br/>- garch_vol<br/>- xgb_vol<br/>- realized_vol"]
        Portfolio["portfolio_state<br/>- timestamp<br/>- weights<br/>- portfolio_vol<br/>- sharpe_proxy<br/>- equity, pnl, turnover"]
    end
    
    Prices ---|time,symbol| Features
    Features ---|time,symbol| Forecast
    Prices ---|join| Forecast
    Forecast ---|time| Portfolio
```

---

## Technology Stack

| Component | Language | Framework | Purpose |
|-----------|----------|-----------|---------|
| Ingestion | Go | stdlib, Binance API | Real-time data streaming |
| Feature Engineering | Python 3.11+ | Redis, Pandas | Streaming pipelines |
| Forecasting | Python 3.11+ | scikit-learn, XGBoost, statsmodels | ML model training & inference |
| Portfolio Engine | Python 3.11+ | NumPy, SciPy | Portfolio optimization |
| API Gateway | Go | Gin, Gorilla WebSocket | Real-time API |
| **Data Stores** | | | |
| Message Queue | Redis 7 | Streams | Event streaming |
| Time-Series DB | PostgreSQL 17 | TimescaleDB 2.17 | Historical data storage |
| **Orchestration** | | Docker Compose | Local development & deployment |

---

## Setup & Deployment

### Prerequisites

- Docker & Docker Compose
- Go 1.21+
- Python 3.11+
- 4GB+ available RAM

### Quick Start

```bash
# Clone repository and navigate to project
cd /path/to/PortfolioEngine

# Start all services
./start.sh

# Services will be available at:
# - API Gateway WebSocket: ws://localhost:8080/ws
# - Redis: localhost:6379
# - TimescaleDB: localhost:5432 (postgres/postgres)
```

### Docker Compose Services

The `infra/docker-compose.yml` orchestrates 7 services:

```yaml
Services:
  ├── ingestion          # Binance data source → Redis + DB
  ├── redis              # Event streaming & caching
  ├── timescaledb        # Time-series data storage
  ├── feature_engineering# Compute rolling features
  ├── forecasting        # ML model training & inference
  ├── portfolio_engine   # Daily rebalancing logic
  └── api_gateway        # WebSocket API server (port 8080)
```

### Health Checks

- **TimescaleDB**: pg_isready probe (5s interval, 5 retries)
- **Redis**: Connection probe on port 6379
- **Services**: Wait for upstream dependencies before starting

---

## API Documentation

### WebSocket API: Real-Time Updates

**Endpoint**: `ws://localhost:8080/ws`

**Message Format**:
```json
{
  "type": "price|forecast|portfolio",
  "timestamp": "2025-01-15T10:30:00Z",
  "data": {...}
}
```

**Price Update**:
```json
{
  "type": "price",
  "symbol": "BTCUSDT",
  "time": "2025-01-15T10:30:00Z",
  "close": 45000.00
}
```

**Forecast Update**:
```json
{
  "type": "forecast",
  "symbol": "BTCUSDT",
  "time": "2025-01-15T10:30:00Z",
  "garch_vol": 0.0150,
  "xgb_vol": 0.0156,
  "realized_vol": 0.0145
}
```

**Portfolio Update** (Daily):
```json
{
  "type": "portfolio",
  "time": "2025-01-15T00:00:00Z",
  "weights": {
    "BTCUSDT": 0.40,
    "ETHUSDT": 0.35,
    "SOLUSDT": 0.25
  },
  "portfolio_vol": 0.0180,
  "sharpe_proxy": 1.45,
  "equity": 100000.00,
  "transaction_cost": 50.00,
  "turnover": 0.08,
  "period_pnl": 1250.00,
  "cumulative_pnl": 12500.00
}
```

### Connection Management

```python
import asyncio
import websockets
import json

async def connect_websocket():
    uri = "ws://localhost:8080/ws"
    async with websockets.connect(uri) as websocket:
        while True:
            message = await websocket.recv()
            data = json.loads(message)
            print(f"Update: {data['type']} - {data['symbol']}")
```

---

## Configuration

### Environment Variables

#### Ingestion Service
```bash
REDIS_ADDR=redis:6379
TIMESCALEDB_DSN=postgresql://postgres:postgres@timescaledb:5432/portfolio
SYMBOLS=BTCUSDT,ETHUSDT,SOLUSDT
KLINE_STREAM=kline_stream
```

#### Feature Engineering
```bash
REDIS_ADDR=redis:6379
TIMESCALEDB_DSN=postgresql://postgres:postgres@timescaledb:5432/portfolio
REDIS_STREAM=kline_stream
ROLLING_WINDOWS=5,15,30,60  # Minutes
BUFFER_SIZE=100  # Max candles per symbol
```

#### Forecasting
```bash
TIMESCALEDB_DSN=postgresql://postgres:postgres@timescaledb:5432/portfolio
SYMBOLS=BTCUSDT,ETHUSDT,SOLUSDT
RETRAIN_INTERVAL_HOURS=4
INFERENCE_POLL_SECONDS=30
```

#### Portfolio Engine
```bash
REDIS_ADDR=redis:6379
TIMESCALEDB_DSN=postgresql://postgres:postgres@timescaledb:5432/portfolio
SYMBOLS=BTCUSDT,ETHUSDT,SOLUSDT
REBALANCE_HOUR_UTC=00
REBALANCE_MINUTE_UTC=00
TURNOVER_FEE_RATE=0.001  # 0.1%
COVARIANCE_LOOKBACK_DAYS=60
```

#### API Gateway
```bash
API_ADDR=:8080
REDIS_ADDR=redis:6379
TIMESCALEDB_DSN=postgresql://postgres:postgres@timescaledb:5432/portfolio
SYMBOLS=BTCUSDT,ETHUSDT,SOLUSDT
KLINE_STREAM=kline_stream
PORTFOLIO_STREAM=portfolio_state_stream
PORTFOLIO_LATEST_KEY=portfolio:latest
```

---

## Performance Considerations

### Throughput & Latency

| Operation | Frequency | Latency Target | Notes |
|-----------|-----------|----------------|-------|
| Kline ingestion | 1/min/symbol | <100ms | Real-time from Binance |
| Feature computation | 1/min/symbol | <500ms | In-memory rolling windows |
| Model inference | 1/min/symbol | <1s | Fast forward pass |
| Portfolio rebalance | 1/day | <5s | Heavy optimization compute |
| WebSocket broadcast | Real-time | <100ms | Fan-out to connected clients |

### Scaling Considerations

**Horizontal Scaling**:
- **Feature Engineering**: Stateless pipelines; scale with Redis stream consumer groups
- **Portfolio Engine**: Single instance (decision maker); benefits from faster compute, not parallelization

**Vertical Scaling**:
- **Forecasting**: Model training is CPU-intensive; benefits from multi-core systems
- **TimescaleDB**: Query performance scales with compression and proper indexing

### Database Optimization

- **Hypertables**: Automatic time-based partitioning for price and feature data
- **Compression**: TimescaleDB native compression for older data
- **Indexing**: Time + symbol composite indexes for common queries
- **Retention**: Configure data retention policies (e.g., keep 1 year of data)

---

## Deployment Architecture

```mermaid
graph TB
    subgraph Dev["Local Development"]
        DC["Docker Compose<br/>All Services"]
    end
    
    subgraph Cloud["Cloud Deployment"]
        K8s["Kubernetes<br/>Cluster"]
    end
    
    subgraph Stateless["Stateless Services"]
        Ing["Ingestion<br/>Pods"]
        Feat["Feature Eng<br/>Pods"]
        Forecast["Forecasting<br/>Pods"]
        API["API Gateway<br/>Pods"]
    end
    
    subgraph Stateful["Stateful Services"]
        Portfolio["Portfolio<br/>StatefulSet"]
    end
    
    subgraph Persist["Persistence"]
        Redis["Redis<br/>Managed"]
        DB["TimescaleDB<br/>Managed"]
    end
    
    Dev -.->|Scale to| K8s
    K8s --> Ing
    K8s --> Feat
    K8s --> Forecast
    K8s --> Portfolio
    K8s --> API
    
    Ing --> Redis
    Ing --> DB
    Feat --> Redis
    Feat --> DB
    Forecast --> DB
    Portfolio --> DB
    API --> Redis
    API --> DB
```

---

## Project Structure

```
PortfolioEngine/
├── README.md                          # This file
├── specs.md                           # Detailed specifications
├── start.sh                           # Quick start script
│
├── infra/
│   └── docker-compose.yml             # Service orchestration
│
├── ingestion/                         # Go service
│   ├── main.go                        # Binance WebSocket client
│   ├── httpclient.go                  # HTTP utilities
│   ├── types.go                       # Data structures
│   ├── backfill.go                    # Historical data backfill
│   ├── Dockerfile
│   └── go.mod
│
├── feature_engineering/               # Python service
│   ├── main.py                        # Streaming pipeline
│   ├── features.py                    # Feature computation logic
│   ├── db.py                          # TimescaleDB operations
│   ├── config.py                      # Configuration
│   ├── Dockerfile
│   └── requirements.txt
│
├── forecasting/                       # Python service
│   ├── main.py                        # Training & inference loops
│   ├── train.py                       # Model training logic
│   ├── inference.py                   # Prediction generation
│   ├── labels.py                      # Target computation
│   ├── db.py                          # Database operations
│   ├── config.py                      # Configuration
│   ├── models/
│   │   ├── garch_model.py             # GARCH implementation
│   │   └── xgb_model.py               # XGBoost wrapper
│   ├── Dockerfile
│   └── requirements.txt
│
├── portfolio_engine/                  # Python service
│   ├── main.py                        # Rebalance loop
│   ├── allocator.py                   # Portfolio optimization
│   ├── db.py                          # Database operations
│   ├── config.py                      # Configuration
│   ├── Dockerfile
│   └── requirements.txt
│
└── api_gateway/                       # Go service
    ├── main.go                        # WebSocket server
    ├── Dockerfile
    └── go.mod
```

---

## Educational Value

This platform demonstrates:

1. **Microservices Architecture**: Loosely coupled services with well-defined contracts
2. **Event Streaming**: Real-time data processing with Redis Streams
3. **Time-Series Data Management**: Efficient storage and querying with TimescaleDB
4. **ML Ops**: Model training/inference in production workflows
5. **Portfolio Optimization**: Quantitative risk management techniques
6. **Real-Time APIs**: WebSocket-based server implementation
7. **DevOps**: Docker containerization and orchestration

---

**Last Updated**: January 2025
