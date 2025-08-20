"""
Kafka Producer for FX Trade Data.

This module generates synthetic FX trade data and sends it to Kafka topics
for real-time anomaly detection processing.
"""

import json
import time
import random
import uuid
from datetime import datetime, timedelta
from typing import Dict, Any, List
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from kafka import KafkaProducer
from consumer.utils import load_config, setup_logging


class FXTradeProducer:
    """Producer class for generating and sending FX trade data to Kafka."""
    
    def __init__(self, config_path: str = "../config/config.yaml"):
        """
        Initialize the FX trade producer.
        
        Args:
            config_path: Path to configuration file
        """
        self.config = load_config(config_path)
        self.logger = setup_logging(self.config)
        
        # Kafka configuration
        kafka_config = self.config.get('kafka', {})
        self.bootstrap_servers = kafka_config.get('bootstrap_servers', 'localhost:9092')
        self.topic = kafka_config.get('producer_topic', 'fx-trades')
        
        # Data generation configuration
        data_config = self.config.get('data', {})
        self.currency_pairs = data_config.get('currency_pairs', ['EUR/USD', 'GBP/USD', 'USD/JPY'])
        self.price_ranges = data_config.get('price_ranges', {})
        self.amount_range = data_config.get('amount_range', [1000, 1000000])
        self.anomaly_rate = data_config.get('anomaly_rate', 0.05)
        self.trade_interval = data_config.get('trade_interval', 1.0)
        
        # Initialize Kafka producer
        self.producer = None
        self._initialize_producer()
        
        # Statistics
        self.total_trades_sent = 0
        self.anomalies_sent = 0
        
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
    
    def generate_trade_data(self) -> Dict[str, Any]:
        """
        Generate a single FX trade record.
        
        Returns:
            Dictionary containing trade data
        """
        # Select random currency pair
        currency_pair = random.choice(self.currency_pairs)
        
        # Get price range for this pair
        if currency_pair in self.price_ranges:
            min_price, max_price = self.price_ranges[currency_pair]
        else:
            min_price, max_price = 1.0, 2.0
        
        # Generate base trade data
        amount = random.uniform(self.amount_range[0], self.amount_range[1])
        price = random.uniform(min_price, max_price)
        
        # Determine if this should be an anomaly
        is_anomaly = random.random() < self.anomaly_rate
        
        if is_anomaly:
            # Inject different types of anomalies
            anomaly_type = random.choice(['price_spike', 'amount_spike', 'unusual_pair'])
            
            if anomaly_type == 'price_spike':
                # Extreme price deviation (2-5x normal price)
                price = price * random.uniform(2.0, 5.0)
                self.logger.debug(f"Generated price spike anomaly: {price:.4f}")
                
            elif anomaly_type == 'amount_spike':
                # Extreme amount (10-50x normal amount)
                amount = amount * random.uniform(10.0, 50.0)
                self.logger.debug(f"Generated amount spike anomaly: {amount:.2f}")
                
            else:  # unusual_pair
                # Use unusual currency pair
                currency_pair = 'XXX/YYY'
                price = random.uniform(0.1, 10.0)
                self.logger.debug(f"Generated unusual pair anomaly: {currency_pair}")
        
        # Generate trade record
        trade = {
            'trade_id': str(uuid.uuid4()),
            'timestamp': datetime.now().isoformat(),
            'currency_pair': currency_pair,
            'amount': round(amount, 2),
            'price': round(price, 4),
            'buyer_id': f"BUYER_{random.randint(1, 100):03d}",
            'seller_id': f"SELLER_{random.randint(1, 100):03d}",
            'notional_value': round(amount * price, 2),
            'is_anomaly': is_anomaly
        }
        
        if is_anomaly:
            self.anomalies_sent += 1
            
        return trade
    
    def send_trade(self, trade: Dict[str, Any]) -> None:
        """
        Send a single trade to Kafka.
        
        Args:
            trade: Trade data dictionary
        """
        try:
            # Use trade_id as key for partitioning
            key = trade['trade_id']
            
            # Send to Kafka
            future = self.producer.send(
                topic=self.topic,
                key=key,
                value=trade
            )
            
            # Wait for send to complete
            record_metadata = future.get(timeout=10)
            
            self.total_trades_sent += 1
            
            self.logger.debug(
                f"Trade sent successfully",
                trade_id=trade['trade_id'],
                topic=record_metadata.topic,
                partition=record_metadata.partition,
                offset=record_metadata.offset,
                is_anomaly=trade['is_anomaly']
            )
            
        except Exception as e:
            self.logger.error(f"Failed to send trade {trade.get('trade_id', 'unknown')}: {e}")
            raise
    
    def run_producer(self, duration_minutes: int = 60, max_trades: int = None) -> None:
        """
        Run the producer for a specified duration or number of trades.
        
        Args:
            duration_minutes: How long to run the producer (minutes)
            max_trades: Maximum number of trades to send (optional)
        """
        self.logger.info(f"Starting FX trade producer for {duration_minutes} minutes")
        
        start_time = datetime.now()
        end_time = start_time + timedelta(minutes=duration_minutes)
        
        try:
            while datetime.now() < end_time:
                # Check if we've reached max trades
                if max_trades and self.total_trades_sent >= max_trades:
                    self.logger.info(f"Reached maximum trades limit: {max_trades}")
                    break
                
                # Generate and send trade
                trade = self.generate_trade_data()
                self.send_trade(trade)
                
                # Log progress periodically
                if self.total_trades_sent % 100 == 0:
                    self.logger.info(
                        f"Producer progress",
                        total_trades=self.total_trades_sent,
                        anomalies=self.anomalies_sent,
                        anomaly_rate=f"{self.anomalies_sent/self.total_trades_sent:.2%}"
                    )
                
                # Wait before next trade
                time.sleep(self.trade_interval)
                
        except KeyboardInterrupt:
            self.logger.info("Producer stopped by user")
        except Exception as e:
            self.logger.error(f"Error in producer: {e}")
            raise
        finally:
            self._shutdown()
    
    def _shutdown(self) -> None:
        """Shutdown the producer gracefully."""
        if self.producer:
            self.producer.flush()
            self.producer.close()
            
        self.logger.info(
            f"Producer shutdown complete",
            total_trades_sent=self.total_trades_sent,
            anomalies_sent=self.anomalies_sent,
            final_anomaly_rate=f"{self.anomalies_sent/self.total_trades_sent:.2%}" if self.total_trades_sent > 0 else "0%"
        )
    
    def send_batch(self, num_trades: int) -> None:
        """
        Send a batch of trades quickly for testing.
        
        Args:
            num_trades: Number of trades to send
        """
        self.logger.info(f"Sending batch of {num_trades} trades")
        
        try:
            for i in range(num_trades):
                trade = self.generate_trade_data()
                self.send_trade(trade)
                
                # Small delay to avoid overwhelming Kafka
                time.sleep(0.1)
                
        except Exception as e:
            self.logger.error(f"Error sending batch: {e}")
            raise
        finally:
            self._shutdown()


def main():
    """Main function to run the producer."""
    import argparse
    
    parser = argparse.ArgumentParser(description='FX Trade Kafka Producer')
    parser.add_argument('--duration', type=int, default=60, 
                       help='Duration to run producer in minutes (default: 60)')
    parser.add_argument('--max-trades', type=int, default=None,
                       help='Maximum number of trades to send (default: no limit)')
    parser.add_argument('--batch', type=int, default=None,
                       help='Send a batch of trades and exit (default: continuous)')
    
    args = parser.parse_args()
    
    producer = FXTradeProducer()
    
    try:
        if args.batch:
            producer.send_batch(args.batch)
        else:
            producer.run_producer(duration_minutes=args.duration, max_trades=args.max_trades)
    except KeyboardInterrupt:
        print("\nProducer stopped by user")
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main() 