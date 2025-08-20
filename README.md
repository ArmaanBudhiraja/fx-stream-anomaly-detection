# Real-Time FX Anomaly Detection System

A comprehensive real-time foreign exchange (FX) anomaly detection system using machine learning, Apache Kafka, Apache Spark, and multiple data sources.

## 🚀 Features

- **Real-time FX Data**: Multiple data sources (Alpha Vantage, Exchange Rate Host)
- **Machine Learning**: Isolation Forest anomaly detection model
- **Stream Processing**: Apache Kafka for real-time data streaming
- **Big Data Processing**: Apache Spark for scalable data processing
- **Database Storage**: PostgreSQL/SQLite support for anomaly storage
- **Web Dashboard**: Real-time visualization of anomalies
- **Docker Support**: Containerized deployment

## 📁 Project Structure

```
fx-anomaly/
├── config/                 # Configuration files
│   └── config.yaml        # Main configuration
├── consumer/              # Kafka consumer and anomaly detection
│   ├── fx_anomaly_detector.py
│   ├── database_storage.py
│   └── utils.py
├── producer/              # Data producers
│   ├── kafka_producer.py
│   └── real_time_fx_producer.py
├── dashboard/             # Web dashboard
│   ├── app.py
│   ├── anomaly_dashboard.py
│   └── anomaly_dashboard.html
├── model/                 # ML model files
│   ├── fx_anomaly_model.pkl
│   ├── fx_anomaly_model_info.json
│   └── train_model.py
├── docker/                # Docker configuration
│   ├── docker-compose.yml
│   └── init.sql/
├── tests/                 # Unit tests
├── run_pipeline.py        # Main pipeline runner (recommended)
├── run_working_pipeline.py # Simplified working pipeline
├── run_full_pipeline.py   # Full Kafka streaming pipeline
├── PIPELINE_GUIDE.md      # Detailed pipeline selection guide
├── setup.py              # Project setup
├── requirements.txt      # Python dependencies
└── README.md            # This file
```

## 🛠️ Installation

### Prerequisites

- Python 3.8+
- Docker and Docker Compose
- Apache Kafka (via Docker)

### Setup

1. **Clone the repository**
   ```bash
   git clone <your-repo-url>
   cd fx-anomaly
   ```

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Set up environment variables**
   Create a `.env` file in the root directory:
   ```env
   ALPHA_VANTAGE_API_KEY=your_alpha_vantage_key
   EXCHANGE_RATE_HOST_API_KEY=your_exchange_rate_host_key
   ```

4. **Start Kafka and PostgreSQL (Docker)**
   ```bash
   docker-compose -f docker/docker-compose.yml up -d
   ```

## 🚀 Quick Start

### Easiest Way to Get Started

**Use the main runner script (recommended):**
```bash
python run_pipeline.py
```

This will show you a menu to choose between the two pipeline options and guide you through the process.

### Which Pipeline Should You Use?

**For Most Users (Recommended):**
```bash
python run_working_pipeline.py
```
- ✅ **Simplified and reliable** - bypasses Kafka consumer issues
- ✅ **Direct processing** - processes data without Kafka streaming
- ✅ **Faster startup** - fewer moving parts
- ✅ **Better for development and testing**

**For Advanced Users (Full Kafka Streaming):**
```bash
python run_full_pipeline.py
```
- ⚠️ **Complete Kafka streaming** - uses full Kafka producer/consumer architecture
- ⚠️ **More complex** - requires all Kafka services to be working properly
- ⚠️ **Production-ready** - full streaming pipeline with all components
- ⚠️ **May have Kafka connectivity issues** - requires proper Kafka setup

### What Happens When You Run the Pipeline

The recommended pipeline will:
1. Start Docker infrastructure (Kafka + PostgreSQL)
2. Process FX data directly (bypassing Kafka consumer issues)
3. Detect anomalies in real-time
4. Store results in database
5. Start web dashboard at http://localhost:5000

### Individual Components (Advanced)

If you want to run components separately:

1. **Start the producer** (in one terminal):
   ```bash
   python producer/real_time_fx_producer.py
   ```

2. **Start the consumer** (in another terminal):
   ```bash
   python consumer/fx_anomaly_detector.py
   ```

3. **Start the dashboard** (in a third terminal):
   ```bash
   python dashboard/app.py
   ```

### 📖 Need More Details?

For a complete guide on choosing between pipelines, see: **[PIPELINE_GUIDE.md](PIPELINE_GUIDE.md)**

## 📊 How It Works

### 1. Data Flow Architecture

```
Real-time FX Data Sources
         ↓
   Kafka Producer
         ↓
   Kafka Topic (fx-trades)
         ↓
   Kafka Consumer
         ↓
   Anomaly Detection (ML Model)
         ↓
   Database Storage
         ↓
   Web Dashboard
```

### 2. Components Explained

#### **Producer (`producer/real_time_fx_producer.py`)**
- Fetches real-time FX data from multiple sources
- Implements fallback mechanisms (Alpha Vantage → Exchange Rate Host → Synthetic)
- Publishes data to Kafka topic `fx-trades`
- Handles API rate limits and errors gracefully

#### **Consumer (`consumer/fx_anomaly_detector.py`)**
- Consumes FX data from Kafka
- Extracts 10 features from each data point
- Uses pre-trained Isolation Forest model for anomaly detection
- Stores anomalies in database (PostgreSQL/SQLite)
- Publishes anomaly alerts to Kafka topic `fx-anomalies`

#### **Dashboard (`dashboard/app.py`)**
- Flask web application for real-time visualization
- Displays current FX rates and detected anomalies
- Provides historical anomaly data
- Auto-refreshes every 30 seconds

#### **ML Model (`model/`)**
- Isolation Forest algorithm for unsupervised anomaly detection
- Trained on historical FX data with 10 engineered features
- Features include: price, volume, technical indicators, volatility measures

### 3. Data Sources

#### **Alpha Vantage API**
- Primary data source for real-time FX rates
- Requires API key (free tier available)
- Rate limit: 5 requests per minute (free tier)

#### **Exchange Rate Host API**
- Backup data source
- More generous rate limits
- Fallback when Alpha Vantage is unavailable

#### **Synthetic Data**
- Generated when all external APIs fail
- Maintains system functionality during outages
- Realistic FX rate patterns

### 4. Anomaly Detection

The system uses an **Isolation Forest** model that:
- Works on 10 engineered features per data point
- Detects anomalies based on data point isolation
- Requires minimal training data
- Handles high-dimensional data efficiently

**Features used:**
1. Current price
2. Price change
3. Price volatility
4. Moving averages (5, 10, 20 periods)
5. RSI (Relative Strength Index)
6. MACD (Moving Average Convergence Divergence)
7. Bollinger Bands position

## 🔧 Configuration

Edit `config/config.yaml` to customize:

```yaml
kafka:
  bootstrap_servers: localhost:9092
  topic: fx-trades

data_sources:
  alpha_vantage:
    enabled: true
    symbols: ["EUR/USD", "GBP/USD", "USD/JPY", "USD/CHF", "AUD/USD"]
  
  exchange_rate_host:
    enabled: true
    symbols: ["EUR/USD", "GBP/USD", "USD/JPY", "USD/CHF", "AUD/USD"]

database:
  type: postgresql  # or sqlite
  host: localhost
  port: 5432
  database: fx_anomalies
  username: postgres
  password: password

dashboard:
  host: 0.0.0.0
  port: 5000
  refresh_interval: 30
```

## 📈 Monitoring and Troubleshooting

### Logs
- Producer logs: Check terminal output
- Consumer logs: Check terminal output
- Dashboard logs: Check terminal output

### Common Issues

1. **Kafka Connection Issues**
   - Ensure Docker containers are running
   - Check if Kafka is accessible on localhost:9092

2. **API Rate Limits**
   - System automatically falls back to alternative sources
   - Check API key validity

3. **Model Loading Errors**
   - Ensure `model/fx_anomaly_model.pkl` exists
   - Check file permissions

4. **Database Connection Issues**
   - Verify PostgreSQL is running
   - Check database credentials in config

## 🧪 Testing

Run the test suite:
```bash
python -m pytest tests/
```

## 📝 API Documentation

### Producer API
- **Start**: `python producer/real_time_fx_producer.py`
- **Stop**: Ctrl+C
- **Output**: FX data to Kafka topic

### Consumer API
- **Start**: `python consumer/fx_anomaly_detector.py`
- **Stop**: Ctrl+C
- **Output**: Anomaly alerts to database and Kafka

### Dashboard API
- **Start**: `python dashboard/app.py`
- **Access**: http://localhost:5000
- **Endpoints**:
  - `/`: Main dashboard
  - `/api/anomalies`: JSON anomaly data
  - `/api/rates`: JSON current rates

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License.

## 🙏 Acknowledgments

- Alpha Vantage for FX data API
- Exchange Rate Host for backup data
- Apache Kafka and Spark communities
- Scikit-learn for ML algorithms # fx-stream-anomaly-detection
