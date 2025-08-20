#!/usr/bin/env python3
"""
Real-time FX Data Producer

This module fetches real-time FX data from various sources and sends it to Kafka
for real-time anomaly detection processing.
"""

import json
import time
import requests
import websocket
import threading
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
import sys
import os
import uuid

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from kafka import KafkaProducer
from consumer.utils import load_config, setup_logging


class RealFXDataProducer:
    """Producer class for fetching and sending real-time FX data to Kafka."""
    
    def __init__(self, config_path: str = "config/config.yaml"):
        """
        Initialize the real-time FX data producer.
        
        Args:
            config_path: Path to configuration file
        """
        self.config = load_config(config_path)
        self.logger = setup_logging(self.config)
        
        # Kafka configuration
        kafka_config = self.config.get('kafka', {})
        self.bootstrap_servers = kafka_config.get('bootstrap_servers', 'localhost:9092')
        self.topic = kafka_config.get('producer_topic', 'fx-trades')
        
        # FX data sources configuration
        data_config = self.config.get('data', {})
        self.fx_sources = data_config.get('fx_data_sources', {})
        self.real_time_enabled = data_config.get('real_time_enabled', False)
        
        # Initialize Kafka producer
        self.producer = None
        self._initialize_producer()
        
        # Statistics
        self.total_trades_sent = 0
        self.last_prices = {}
        self.running = False
        
    def _initialize_producer(self) -> None:
        """Initialize Kafka producer with proper configuration."""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                acks='all',
                retries=3,
                batch_size=16384,
                linger_ms=10,
                buffer_memory=33554432
            )
            self.logger.info(f"Kafka producer initialized for topic: {self.topic}")
        except Exception as e:
            self.logger.error(f"Failed to initialize Kafka producer: {e}")
            raise
    
    def fetch_alpha_vantage_data(self, currency_pair: str) -> Optional[Dict[str, Any]]:
        """
        Fetch FX data from Alpha Vantage API.
        
        Args:
            currency_pair: Currency pair to fetch (e.g., 'EUR/USD')
            
        Returns:
            Dictionary containing FX data or None if failed
        """
        try:
            alpha_config = self.fx_sources.get('alpha_vantage', {})
            api_key = alpha_config.get('api_key')
            
            if not api_key or api_key == "YOUR_ALPHA_VANTAGE_API_KEY":
                self.logger.warning("Alpha Vantage API key not configured. Please get a free key from https://www.alphavantage.co/support/#api-key")
                return None
            
            # Convert currency pair format (EUR/USD -> EURUSD)
            from_currency, to_currency = currency_pair.split('/')
            
            url = alpha_config.get('base_url')
            params = {
                'function': 'CURRENCY_EXCHANGE_RATE',
                'from_currency': from_currency,
                'to_currency': to_currency,
                'apikey': api_key
            }
            
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            
            if 'Realtime Currency Exchange Rate' in data:
                rate_data = data['Realtime Currency Exchange Rate']
                
                # Generate synthetic trade data based on real price
                price = float(rate_data['5. Exchange Rate'])
                amount = 100000  # Standard lot size
                
                trade = {
                    'trade_id': str(uuid.uuid4()),
                    'timestamp': datetime.now().isoformat(),
                    'currency_pair': currency_pair,
                    'amount': amount,
                    'price': price,
                    'buyer_id': 'REAL_FX_BUYER',
                    'seller_id': 'REAL_FX_SELLER',
                    'notional_value': amount * price,
                    'data_source': 'alpha_vantage',
                    'is_real_data': True
                }
                
                self.logger.debug(f"Fetched {currency_pair} data: {price}")
                return trade
            else:
                self.logger.warning(f"Unexpected Alpha Vantage response format: {data}")
                return None
                
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Network error fetching Alpha Vantage data for {currency_pair}: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Error fetching Alpha Vantage data for {currency_pair}: {e}")
            return None
    
    def fetch_exchange_rate_host_data(self, currency_pair: str) -> Optional[Dict[str, Any]]:
        """
        Fetch FX data from Exchange Rate Host API.
        
        Args:
            currency_pair: Currency pair to fetch (e.g., 'EUR/USD')
            
        Returns:
            Dictionary containing FX data or None if failed
        """
        try:
            exchange_config = self.fx_sources.get('exchange_rate_host', {})
            api_key = exchange_config.get('api_key')
            base_url = exchange_config.get('base_url')
            
            if not api_key:
                self.logger.warning("Exchange Rate Host API key not configured")
                return None
            
            # Convert currency pair format (EUR/USD -> EUR to USD)
            from_currency, to_currency = currency_pair.split('/')
            
            url = f"{base_url}"
            params = {
                'from': from_currency,
                'to': to_currency,
                'amount': 1,
                'access_key': api_key
            }
            
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            
            if 'result' in data and data['result'] is not None:
                # Generate synthetic trade data based on real price
                price = float(data['result'])
                amount = 100000  # Standard lot size
                
                trade = {
                    'trade_id': str(uuid.uuid4()),
                    'timestamp': datetime.now().isoformat(),
                    'currency_pair': currency_pair,
                    'amount': amount,
                    'price': price,
                    'buyer_id': 'REAL_FX_BUYER',
                    'seller_id': 'REAL_FX_SELLER',
                    'notional_value': amount * price,
                    'data_source': 'exchange_rate_host',
                    'is_real_data': True
                }
                
                self.logger.debug(f"Fetched {currency_pair} data from Exchange Rate Host: {price}")
                return trade
            else:
                self.logger.warning(f"Unexpected Exchange Rate Host response format: {data}")
                return None
                
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Network error fetching Exchange Rate Host data for {currency_pair}: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Error fetching Exchange Rate Host data for {currency_pair}: {e}")
            return None
    
    def _generate_synthetic_fx_data(self, currency_pair: str) -> Dict[str, Any]:
        """
        Generate realistic synthetic FX data when real data is unavailable.
        
        Args:
            currency_pair: Currency pair (e.g., 'EUR/USD')
            
        Returns:
            Dictionary containing synthetic FX data
        """
        import random
        
        # Realistic price ranges for major currency pairs
        price_ranges = {
            'EUR/USD': [1.0500, 1.1500],
            'GBP/USD': [1.2000, 1.3500],
            'USD/JPY': [100.00, 150.00],
            'USD/CHF': [0.8500, 0.9500],
            'AUD/USD': [0.6500, 0.7500],
            'USD/CAD': [1.2500, 1.3500],
            'NZD/USD': [0.6000, 0.7000]
        }
        
        # Get price range for the currency pair
        if currency_pair in price_ranges:
            min_price, max_price = price_ranges[currency_pair]
        else:
            # Default range for unknown pairs
            min_price, max_price = 1.0000, 2.0000
        
        # Generate realistic price with small random variation
        base_price = (min_price + max_price) / 2
        variation = random.uniform(-0.01, 0.01)  # ±1% variation
        price = base_price + variation
        
        # Ensure price stays within realistic bounds
        price = max(min_price, min(max_price, price))
        
        amount = random.randint(10000, 1000000)  # Random trade size
        
        trade = {
            'trade_id': str(uuid.uuid4()),
            'timestamp': datetime.now().isoformat(),
            'currency_pair': currency_pair,
            'amount': amount,
            'price': round(price, 5),
            'buyer_id': 'SYNTHETIC_BUYER',
            'seller_id': 'SYNTHETIC_SELLER',
            'notional_value': amount * price,
            'data_source': 'synthetic_fallback',
            'is_real_data': False
        }
        
        self.logger.debug(f"Generated synthetic {currency_pair} data: {price}")
        return trade
    
    def setup_binance_websocket(self, currency_pair: str):
        """
        Setup Binance WebSocket connection for real-time crypto data.
        
        Args:
            currency_pair: Currency pair to monitor (e.g., 'BTC/USD')
        """
        try:
            binance_config = self.fx_sources.get('binance_websocket', {})
            url = binance_config.get('url')
            
            def on_message(ws, message):
                try:
                    data = json.loads(message)
                    
                    # Extract price from Binance WebSocket message
                    if 'p' in data:  # Binance format
                        price = float(data['p'])
                        amount = float(data.get('q', 100000))
                        
                        trade = {
                            'trade_id': str(uuid.uuid4()),
                            'timestamp': datetime.now().isoformat(),
                            'currency_pair': currency_pair,
                            'amount': amount,
                            'price': price,
                            'buyer_id': 'WEBSOCKET_BUYER',
                            'seller_id': 'WEBSOCKET_SELLER',
                            'notional_value': amount * price,
                            'data_source': 'binance_websocket',
                            'is_real_data': True
                        }
                        
                        self.send_trade(trade)
                        
                except Exception as e:
                    self.logger.error(f"Error processing WebSocket message: {e}")
            
            def on_error(ws, error):
                self.logger.error(f"WebSocket error: {error}")
            
            def on_close(ws, close_status_code, close_msg):
                self.logger.info("WebSocket connection closed")
            
            def on_open(ws):
                self.logger.info(f"WebSocket connected for {currency_pair}")
            
            # Create WebSocket connection
            ws = websocket.WebSocketApp(
                url,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close
            )
            
            # Run WebSocket in a separate thread
            ws_thread = threading.Thread(target=ws.run_forever, daemon=True)
            ws_thread.start()
            
            return ws
            
        except Exception as e:
            self.logger.error(f"Error setting up WebSocket for {currency_pair}: {e}")
            return None
    
    def send_trade(self, trade: Dict[str, Any]):
        """Send a trade to Kafka topic."""
        try:
            self.logger.info(f"Sending trade to topic '{self.topic}': {json.dumps(trade)}")
            future = self.producer.send(self.topic, value=trade)
            # Wait for the send to complete
            record_metadata = future.get(timeout=10)
            self.logger.info(f"Trade sent successfully to partition {record_metadata.partition}, offset {record_metadata.offset}")
            self.producer.flush()
            self.total_trades_sent += 1
        except Exception as e:
            self.logger.error(f"Failed to send trade {trade.get('trade_id', 'unknown')}: {e}")
    
    def run_producer(self, duration_minutes: int = 60, source: str = 'auto'):
        """
        Run the producer with specified data source.
        
        Args:
            duration_minutes: How long to run the producer
            source: Data source to use ('auto', 'alpha_vantage', 'binance_websocket', 'exchange_rate_host', 'synthetic')
        """
        if not self.real_time_enabled:
            self.logger.warning("Real-time data is disabled in config. Enable it to use real FX data.")
            return
        
        # Determine best source if 'auto'
        if source == 'auto':
            # Try Alpha Vantage first
            alpha_config = self.fx_sources.get('alpha_vantage', {})
            alpha_api_key = alpha_config.get('api_key', '')
            if alpha_api_key and alpha_api_key != 'YOUR_ALPHA_VANTAGE_API_KEY':
                # Test Alpha Vantage API
                test_trade = self.fetch_alpha_vantage_data("EUR/USD")
                if test_trade:
                    source = 'alpha_vantage'
                    self.logger.info("Alpha Vantage API working, using as primary source")
                else:
                    self.logger.warning("Alpha Vantage API failed, trying Exchange Rate Host")
            
            # If Alpha Vantage failed, try Exchange Rate Host
            if source == 'auto':
                exchange_config = self.fx_sources.get('exchange_rate_host', {})
                exchange_api_key = exchange_config.get('api_key', '')
                if exchange_api_key:
                    # Test Exchange Rate Host API
                    test_trade = self.fetch_exchange_rate_host_data("EUR/USD")
                    if test_trade:
                        source = 'exchange_rate_host'
                        self.logger.info("Exchange Rate Host API working, using as backup source")
                    else:
                        self.logger.warning("Exchange Rate Host API failed")
            
            # If both failed, try Binance WebSocket
            if source == 'auto' and self.fx_sources.get('binance_websocket', {}):
                source = 'binance_websocket'
                self.logger.info("Using Binance WebSocket as fallback")
            
            # Last resort: synthetic data
            if source == 'auto':
                self.logger.error("All real data sources failed. Using synthetic data.")
                source = 'synthetic'
        
        print(f"\nStarting Real-Time FX Data Producer")
        print(f"Data Source: {source}")
        print(f"Duration: {duration_minutes} minutes")
        print(f"Total trades sent: {self.total_trades_sent}")
        print("="*50)
        
        # Main producer loop
        if source == 'alpha_vantage' or source == 'auto':
            self.logger.info("Starting Alpha Vantage FX data producer")
            currency_pairs = self.fx_sources.get('alpha_vantage', {}).get('currency_pairs', [])
            update_interval = self.fx_sources.get('alpha_vantage', {}).get('update_interval', 60)
            start_time = datetime.now()
            end_time = start_time + timedelta(minutes=duration_minutes)
            self.running = True
            try:
                while datetime.now() < end_time and self.running:
                    for currency_pair in currency_pairs:
                        trade = self.fetch_alpha_vantage_data(currency_pair)
                        if trade:
                            self.send_trade(trade)
                            print(f"{currency_pair}: {trade['price']:.4f} (Alpha Vantage)")
                        else:
                            # Try Exchange Rate Host as fallback
                            trade = self.fetch_exchange_rate_host_data(currency_pair)
                            if trade:
                                self.send_trade(trade)
                                print(f"{currency_pair}: {trade['price']:.4f} (Exchange Rate Host)")
                            else:
                                print(f"Failed to fetch {currency_pair} from both Alpha Vantage and Exchange Rate Host")
                        time.sleep(1)
                    print(f"Waiting {update_interval} seconds before next update...")
                    time.sleep(update_interval)
                self.logger.info("Alpha Vantage/Exchange Rate Host producer finished")
            except KeyboardInterrupt:
                self.logger.info("Alpha Vantage/Exchange Rate Host producer interrupted")
            finally:
                self.shutdown()
                return
        elif source == 'binance_websocket':
            self.run_binance_websocket_producer(duration_minutes)
        elif source == 'exchange_rate_host':
            self.run_exchange_rate_host_producer(duration_minutes)
        elif source == 'synthetic':
            self.logger.warning("No real data source available. Using synthetic data.")
            self.run_synthetic_producer(duration_minutes)
        else:
            self.logger.error(f"Unknown data source: {source}")
            print(f"Available sources: alpha_vantage, binance_websocket, exchange_rate_host, synthetic")

    def run_synthetic_producer(self, duration_minutes: int = 60):
        """
        Run producer using synthetic FX data.
        """
        self.logger.info("Starting Synthetic FX data producer")
        currency_pairs = ["EUR/USD", "GBP/USD", "USD/JPY", "USD/CHF", "AUD/USD"]
        update_interval = 30
        start_time = datetime.now()
        end_time = start_time + timedelta(minutes=duration_minutes)
        self.running = True
        try:
            while datetime.now() < end_time and self.running:
                for currency_pair in currency_pairs:
                    if not self.running:
                        break
                    trade = self._generate_synthetic_fx_data(currency_pair)
                    if trade:
                        self.send_trade(trade)
                        print(f"{trade['currency_pair']}: {trade['price']:.4f} (Synthetic)")
                    time.sleep(1)
                print(f"Waiting {update_interval} seconds before next update...")
                time.sleep(update_interval)
        except KeyboardInterrupt:
            self.logger.info("Synthetic producer interrupted")
        finally:
            self.running = False

    def shutdown(self):
        """Shutdown the producer."""
        self.running = False
        if self.producer:
            self.producer.close()
        self.logger.info("Real FX producer shutdown complete")


def main():
    """Main function to run the real-time FX producer."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Real-time FX Data Producer')
    parser.add_argument('--duration', type=int, default=60,
                       help='Duration to run producer in minutes')
    parser.add_argument('--source', type=str, default='auto',
                       choices=['auto', 'alpha_vantage', 'binance_websocket', 'exchange_rate_host', 'synthetic'],
                       help='Data source to use (default: auto - best available)')
    parser.add_argument('--config', type=str, default='config/config.yaml',
                       help='Path to configuration file')
    
    args = parser.parse_args()
    
    producer = None
    try:
        producer = RealFXDataProducer(config_path=args.config)
        producer.run_producer(duration_minutes=args.duration, source=args.source)
    except KeyboardInterrupt:
        print("\nReal FX producer interrupted by user")
    except Exception as e:
        print(f"\nReal FX producer failed: {e}")
        sys.exit(1)
    finally:
        if producer:
            producer.shutdown()


if __name__ == "__main__":
    main() 