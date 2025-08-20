# FX Anomaly Detection Project - Complete Explanation

## 🎯 Project Overview

This is a **real-time foreign exchange (FX) anomaly detection system** that monitors currency exchange rates, detects unusual patterns using machine learning, and provides real-time alerts through a web dashboard.

## 🏗️ System Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Real-time     │    │   Apache        │    │   Anomaly       │
│   FX Data       │───▶│   Kafka         │───▶│   Detection     │
│   Sources       │    │   Stream        │    │   Engine        │
│                 │    │                 │    │   (ML Model)    │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Alpha         │    │   PostgreSQL    │    │   Web Dashboard │
│   Vantage       │    │   Database      │    │   (Flask)       │
│   API           │    │   Storage       │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Exchange      │    │   Anomaly       │    │   Real-time     │
│   Rate Host     │    │   Alerts        │    │   Visualization │
│   API           │    │   (Kafka)       │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## 🔄 Data Flow Process

### Step 1: Data Collection (Producer)
1. **Multiple Data Sources**: The system tries to get real FX data from:
   - **Alpha Vantage API** (primary source)
   - **Exchange Rate Host API** (backup source)
   - **Synthetic Data** (fallback when APIs fail)

2. **Fallback Mechanism**: If one source fails, it automatically tries the next:
   ```
   Alpha Vantage → Exchange Rate Host → Synthetic Data
   ```

3. **Data Publishing**: FX rates are published to Kafka topic `fx-trades`

### Step 2: Data Processing (Consumer)
1. **Kafka Consumption**: Consumer reads FX data from Kafka
2. **Feature Engineering**: Extracts 10 features from each data point:
   - Current price
   - Price change
   - Price volatility
   - Moving averages (5, 10, 20 periods)
   - RSI (Relative Strength Index)
   - MACD (Moving Average Convergence Divergence)
   - Bollinger Bands position

3. **Anomaly Detection**: Uses pre-trained Isolation Forest model
4. **Database Storage**: Stores anomalies in PostgreSQL/SQLite
5. **Alert Publishing**: Sends anomaly alerts to Kafka topic `fx-anomalies`

### Step 3: Visualization (Dashboard)
1. **Web Interface**: Flask web application
2. **Real-time Updates**: Auto-refreshes every 30 seconds
3. **Data Display**: Shows current rates and detected anomalies

## 🧠 Machine Learning Model

### Isolation Forest Algorithm
- **Type**: Unsupervised anomaly detection
- **Principle**: Anomalies are easier to isolate than normal data points
- **Advantages**: 
  - No need for labeled training data
  - Works well with high-dimensional data
  - Fast training and prediction

### Feature Engineering
The model uses 10 carefully engineered features:

1. **Price Features**:
   - `current_price`: Current exchange rate
   - `price_change`: Change from previous value
   - `price_volatility`: Rolling standard deviation

2. **Technical Indicators**:
   - `ma_5`: 5-period moving average
   - `ma_10`: 10-period moving average
   - `ma_20`: 20-period moving average
   - `rsi`: Relative Strength Index
   - `macd`: MACD indicator
   - `bollinger_position`: Position within Bollinger Bands
   - `volume`: Trading volume (synthetic)

### Model Training
- **Training Data**: Historical FX data with synthetic anomalies
- **Model File**: `model/fx_anomaly_model.pkl`
- **Configuration**: `model/fx_anomaly_model_info.json`

## 📊 Data Sources Explained

### 1. Alpha Vantage API
```python
# Example API call
url = f"https://www.alphavantage.co/query?function=CURRENCY_EXCHANGE_RATE&from_currency=EUR&to_currency=USD&apikey={api_key}"
```
- **Rate Limit**: 5 requests/minute (free tier)
- **Data**: Real-time FX rates
- **Format**: JSON response

### 2. Exchange Rate Host API
```python
# Example API call
url = f"https://api.exchangerate.host/convert?from=EUR&to=USD&amount=1"
```
- **Rate Limit**: More generous than Alpha Vantage
- **Data**: Real-time FX rates
- **Format**: JSON response

### 3. Synthetic Data
```python
# Generated when APIs fail
base_price = 1.1000  # EUR/USD base
noise = random.uniform(-0.001, 0.001)
current_price = base_price + noise
```
- **Purpose**: Maintain system functionality during outages
- **Realism**: Based on real FX patterns
- **Variability**: Random noise added to base prices

## 🗄️ Database Schema

### Anomalies Table
```sql
CREATE TABLE anomalies (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP,
    currency_pair VARCHAR(10),
    price DECIMAL(10,6),
    anomaly_score DECIMAL(10,6),
    features JSONB,
    data_source VARCHAR(50),
    created_at TIMESTAMP DEFAULT NOW()
);
```

### Trades Table
```sql
CREATE TABLE trades (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP,
    currency_pair VARCHAR(10),
    price DECIMAL(10,6),
    volume DECIMAL(15,2),
    data_source VARCHAR(50),
    is_anomaly BOOLEAN,
    created_at TIMESTAMP DEFAULT NOW()
);
```

## 🔧 Key Components Deep Dive

### 1. Producer (`producer/real_time_fx_producer.py`)

**Purpose**: Fetches and publishes FX data to Kafka

**Key Functions**:
```python
def fetch_alpha_vantage_data(self):
    # Fetches real FX data from Alpha Vantage API
    
def fetch_exchange_rate_host_data(self):
    # Fetches backup data from Exchange Rate Host
    
def generate_synthetic_data(self):
    # Generates realistic synthetic data when APIs fail
    
def publish_to_kafka(self, data):
    # Publishes FX data to Kafka topic
```

**Error Handling**:
- API rate limit management
- Network timeout handling
- Automatic fallback to alternative sources

### 2. Consumer (`consumer/fx_anomaly_detector.py`)

**Purpose**: Processes FX data and detects anomalies

**Key Functions**:
```python
def extract_features(self, data):
    # Extracts 10 features from FX data
    
def detect_anomaly(self, features):
    # Uses ML model to detect anomalies
    
def store_anomaly(self, anomaly_data):
    # Stores anomalies in database
```

**Feature Extraction Process**:
1. Calculate price changes and volatility
2. Compute technical indicators
3. Normalize features for ML model
4. Ensure exactly 10 features match training data

### 3. Dashboard (`dashboard/app.py`)

**Purpose**: Web interface for real-time monitoring

**Key Endpoints**:
```python
@app.route('/')
def dashboard():
    # Main dashboard page
    
@app.route('/api/anomalies')
def get_anomalies():
    # JSON API for anomaly data
    
@app.route('/api/rates')
def get_rates():
    # JSON API for current rates
```

**Real-time Features**:
- Auto-refreshing data
- Interactive charts
- Historical anomaly display

## 🚀 Running the System

### Option 1: Full Pipeline
```bash
python run_full_pipeline.py
```
- Starts all components automatically
- Handles component coordination
- Best for production use

### Option 2: Individual Components
```bash
# Terminal 1: Producer
python producer/real_time_fx_producer.py

# Terminal 2: Consumer
python consumer/fx_anomaly_detector.py

# Terminal 3: Dashboard
python dashboard/app.py
```
- More control over each component
- Better for debugging
- Manual component management

### Option 3: Simplified Pipeline
```bash
python run_working_pipeline.py
```
- Bypasses Kafka for direct processing
- Faster setup and testing
- Good for development

## 🔍 Monitoring and Debugging

### Logs
- **Producer**: Terminal output with FX data
- **Consumer**: Terminal output with anomaly detection
- **Dashboard**: Terminal output with web server status

### Key Metrics to Monitor
1. **Data Source Health**: Which API is being used
2. **Anomaly Detection Rate**: How many anomalies detected
3. **Kafka Message Flow**: Data flowing through topics
4. **Database Storage**: Anomalies being stored

### Common Issues and Solutions

1. **Kafka Connection Issues**
   ```bash
   # Check if Kafka is running
   docker ps | grep kafka
   
   # Restart Kafka if needed
   docker-compose -f docker/docker-compose.yml restart
   ```

2. **API Rate Limits**
   - System automatically falls back to alternative sources
   - Check API key validity
   - Consider upgrading to paid API plans

3. **Model Loading Errors**
   ```bash
   # Ensure model file exists
   ls -la model/fx_anomaly_model.pkl
   
   # Retrain model if needed
   python model/train_model.py
   ```

## 🎯 Use Cases

### 1. Financial Trading
- Monitor currency pairs for unusual movements
- Detect potential market manipulation
- Alert traders to abnormal price movements

### 2. Risk Management
- Identify potential trading risks
- Monitor market volatility
- Track unusual trading patterns

### 3. Compliance Monitoring
- Detect suspicious trading activities
- Monitor for regulatory violations
- Track market abuse patterns

## 🔮 Future Enhancements

### Potential Improvements
1. **Additional Data Sources**: More FX APIs, news feeds
2. **Advanced ML Models**: Deep learning, ensemble methods
3. **Real-time Alerts**: Email, SMS, Slack notifications
4. **Advanced Analytics**: Trend analysis, prediction models
5. **Scalability**: Kubernetes deployment, cloud integration

### Performance Optimizations
1. **Caching**: Redis for frequently accessed data
2. **Parallel Processing**: Multiple consumer instances
3. **Data Compression**: Efficient storage and transmission
4. **Load Balancing**: Distribute processing across nodes

## 📚 Learning Resources

### Technologies Used
- **Apache Kafka**: Distributed streaming platform
- **Apache Spark**: Big data processing
- **Scikit-learn**: Machine learning library
- **Flask**: Web framework
- **PostgreSQL**: Database
- **Docker**: Containerization

### Related Concepts
- **Stream Processing**: Real-time data processing
- **Anomaly Detection**: Identifying unusual patterns
- **Feature Engineering**: Creating ML model inputs
- **Microservices**: Distributed system architecture
- **Real-time Analytics**: Live data analysis

This project demonstrates a complete real-time data processing pipeline with machine learning, making it an excellent learning resource for data engineering, machine learning, and distributed systems concepts. 