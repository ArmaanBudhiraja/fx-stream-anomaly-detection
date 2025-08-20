#!/usr/bin/env python3
"""
FX Anomaly Detection Consumer

This module implements a real-time anomaly detection system for FX trades
using Apache Spark Streaming and machine learning models.
"""

import os
import sys
import json
import argparse
import logging
from datetime import datetime
from typing import Dict, Any, Optional

# Add current directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import yaml
import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf, struct, to_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from kafka import KafkaProducer, KafkaConsumer
import joblib
from sklearn.ensemble import IsolationForest

from consumer.utils import setup_logging, create_spark_session, load_config
from consumer.database_storage import DatabaseStorage


class FXAnomalyDetector:
    """Real-time FX anomaly detection using Spark Streaming and ML models."""
    
    def __init__(self, config_path: str = "config/config.yaml"):
        """
        Initialize the anomaly detector.
        
        Args:
            config_path: Path to configuration file
        """
        self.config = load_config(config_path)
        self.logger = setup_logging(self.config['logging'])
        
        # Initialize Spark session
        self.spark = create_spark_session(self.config['spark'])
        
        # Load the trained model
        self.model = self._load_model()
        
        # Initialize Kafka producer for anomalies
        self.anomaly_producer = self._init_kafka_producer()
        
        # Initialize database storage
        self.db_storage = DatabaseStorage(config_path)
        
        # Define schema for FX trade data
        self.trade_schema = StructType([
            StructField("timestamp", TimestampType(), True),
            StructField("currency_pair", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("amount", DoubleType(), True),
            StructField("side", StringType(), True),
            StructField("trade_id", StringType(), True)
        ])
        
        self.logger.info("FX Anomaly Detector initialized successfully")
    
    def _load_model(self) -> Optional[IsolationForest]:
        """Load the trained anomaly detection model."""
        model_path = self.config['model']['path']
        
        try:
            if os.path.exists(model_path):
                model = joblib.load(model_path)
                self.logger.info(f"Model loaded from {model_path}")
                return model
            else:
                self.logger.warning(f"Model not found at {model_path}. Will train a new one.")
                return self._train_default_model()
        except Exception as e:
            self.logger.error(f"Error loading model: {e}")
            return self._train_default_model()
    
    def _train_default_model(self) -> IsolationForest:
        """Train a default model if no pre-trained model is available."""
        self.logger.info("Training default anomaly detection model...")
        
        # Create synthetic training data
        np.random.seed(self.config['model']['random_state'])
        n_samples = 1000
        
        # Generate normal FX trade data
        normal_data = np.random.randn(n_samples, 4)  # 4 features: price, amount, hour, minute
        normal_data[:, 0] = normal_data[:, 0] * 0.01 + 1.1  # Price around 1.1
        normal_data[:, 1] = np.abs(normal_data[:, 1] * 10000 + 50000)  # Amount around 50k
        normal_data[:, 2] = np.random.randint(0, 24, n_samples)  # Hour
        normal_data[:, 3] = np.random.randint(0, 60, n_samples)  # Minute
        
        # Train isolation forest
        model = IsolationForest(
            contamination=self.config['model']['contamination'],
            random_state=self.config['model']['random_state'],
            n_estimators=self.config['model']['n_estimators']
        )
        
        model.fit(normal_data)
        self.logger.info("Default model trained successfully")
        return model
    
    def _init_kafka_producer(self) -> KafkaProducer:
        """Initialize Kafka producer for sending anomaly alerts."""
        try:
            producer = KafkaProducer(
                bootstrap_servers=self.config['kafka']['bootstrap_servers'],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None
            )
            self.logger.info("Kafka producer initialized")
            return producer
        except Exception as e:
            self.logger.error(f"Error initializing Kafka producer: {e}")
            return None
    
    def _extract_features(self, trade_data: Dict[str, Any]) -> np.ndarray:
        """Extract features from trade data for anomaly detection."""
        try:
            # Extract timestamp components
            timestamp = pd.to_datetime(trade_data['timestamp'])
            hour = timestamp.hour
            day_of_week = timestamp.dayofweek
            month = timestamp.month

            # Extract price and amount
            price = float(trade_data['price'])
            amount = float(trade_data['amount'])
            notional_value = float(trade_data.get('notional_value', amount * price))

            # Encode currency pair (simple hash-based encoding)
            currency_pair = trade_data['currency_pair']
            currency_pair_encoded = hash(currency_pair) % 100  # Simple encoding

            # Create derived features
            amount_log = np.log1p(amount)
            price_log = np.log1p(price)
            notional_value_log = np.log1p(notional_value)

            # Create feature vector matching EXACTLY the training features order
            features = np.array([
                amount,                    # 1. amount
                price,                     # 2. price
                notional_value,            # 3. notional_value
                currency_pair_encoded,     # 4. currency_pair_encoded
                hour,                      # 5. hour
                day_of_week,               # 6. day_of_week
                month,                     # 7. month
                amount_log,                # 8. amount_log
                price_log,                 # 9. price_log
                notional_value_log         # 10. notional_value_log
            ]).reshape(1, -1)

            return features
        except Exception as e:
            self.logger.error(f"Error extracting features: {e}")
            return None
    
    def _detect_anomaly(self, features: np.ndarray) -> bool:
        """Detect if the trade is anomalous."""
        try:
            if self.model is None:
                return False
            
            # Predict anomaly (-1 for anomaly, 1 for normal)
            prediction = self.model.predict(features)
            return prediction[0] == -1
        except Exception as e:
            self.logger.error(f"Error in anomaly detection: {e}")
            return False
    
    def _send_anomaly_alert(self, trade_data: Dict[str, Any], features: np.ndarray):
        """Send anomaly alert to Kafka topic and store in database."""
        try:
            anomaly_data = {
                'timestamp': trade_data['timestamp'],
                'trade_id': trade_data['trade_id'],
                'currency_pair': trade_data['currency_pair'],
                'price': trade_data['price'],
                'amount': trade_data['amount'],
                'side': trade_data['side'],
                'anomaly_score': float(self.model.decision_function(features)[0]),
                'detected_at': datetime.now().isoformat(),
                'data_source': trade_data.get('data_source', 'unknown'),
                'is_real_data': trade_data.get('is_real_data', False)
            }
            
            # Send to Kafka if enabled
            if self.anomaly_producer:
                self.anomaly_producer.send(
                    self.config['kafka']['anomaly_topic'],
                    key=trade_data['trade_id'],
                    value=anomaly_data
                )
                self.anomaly_producer.flush()
            
            # Store in database if enabled
            if self.db_storage:
                self.db_storage.store_anomaly(anomaly_data)
            
            # Log anomaly
            self.logger.warning(f"ANOMALY DETECTED: {anomaly_data}")
            
            # Console output if enabled
            if self.config['output']['console_enabled']:
                print(f"🚨 ANOMALY: {trade_data['currency_pair']} @ {trade_data['price']} "
                      f"(Amount: {trade_data['amount']}, Score: {anomaly_data['anomaly_score']:.4f})")
                
        except Exception as e:
            self.logger.error(f"Error sending anomaly alert: {e}")
    
    def process_trade(self, trade_data: Dict[str, Any]):
        self.logger.info(f"Received trade: {json.dumps(trade_data)}")
        features = self._extract_features(trade_data)
        self.logger.info(f"Extracted features: {features.flatten().tolist() if features is not None else 'None'}")
        if features is not None:
            is_anomaly = self._detect_anomaly(features)
            self.logger.info(f"Anomaly detection result: {is_anomaly}")
            if is_anomaly:
                self.logger.info(f"Anomaly detected: {json.dumps(trade_data)}")
                self._store_anomaly(trade_data, features)
        else:
            self.logger.warning("Failed to extract features from trade data")

    def _store_anomaly(self, trade_data: Dict[str, Any], features: np.ndarray):
        self.logger.info(f"Storing anomaly in DB: {json.dumps(trade_data)}")
        # ... existing code ...
    
    def start_streaming(self):
        """Start the streaming anomaly detection process."""
        self.logger.info("Starting FX anomaly detection streaming...")
        
        try:
            # Create streaming DataFrame from Kafka
            df = self.spark.readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", self.config['kafka']['bootstrap_servers']) \
                .option("subscribe", self.config['kafka']['consumer_topic']) \
                .option("startingOffsets", self.config['kafka']['auto_offset_reset']) \
                .load()
            
            # Parse JSON data
            parsed_df = df.select(
                from_json(col("value").cast("string"), self.trade_schema).alias("trade")
            ).select("trade.*")
            
            # Define processing function
            def process_batch(batch_df, batch_id):
                if batch_df.count() > 0:
                    trades = batch_df.toPandas().to_dict('records')
                    for trade in trades:
                        self.process_trade(trade)
            
            # Start streaming query
            query = parsed_df.writeStream \
                .foreachBatch(process_batch) \
                .outputMode("append") \
                .option("checkpointLocation", self.config['spark']['checkpoint_location']) \
                .start()
            
            self.logger.info("Streaming query started successfully")
            
            # Wait for termination
            query.awaitTermination()
            
        except Exception as e:
            self.logger.error(f"Error in streaming: {e}")
            raise
    
    def start_kafka_consumer(self):
        """Start Kafka consumer for processing trades."""
        self.logger.info("Starting Kafka consumer for anomaly detection...")
        
        try:
            # Create consumer with explicit topic subscription
            consumer = KafkaConsumer(
                bootstrap_servers=self.config['kafka']['bootstrap_servers'],
                group_id=self.config['kafka']['group_id'],
                auto_offset_reset=self.config['kafka']['auto_offset_reset'],
                enable_auto_commit=self.config['kafka']['enable_auto_commit'],
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
            
            # Explicitly subscribe to the topic
            consumer.subscribe([self.config['kafka']['consumer_topic']])
            self.logger.info(f"Subscribed to topic: {self.config['kafka']['consumer_topic']}")
            
            # Wait for partition assignment
            self.logger.info("Waiting for partition assignment...")
            consumer.poll(timeout_ms=5000)
            partitions = consumer.assignment()
            self.logger.info(f"Assigned partitions: {partitions}")
            
            if not partitions:
                self.logger.error("No partitions assigned! Consumer will not receive messages.")
                return
            
            self.logger.info("Kafka consumer started successfully")
            
            # Process messages
            for message in consumer:
                try:
                    self.logger.info(f"Raw message received: {message}")
                    trade_data = message.value
                    self.logger.info(f"Processing trade: {trade_data}")
                    self.process_trade(trade_data)
                except Exception as e:
                    self.logger.error(f"Error processing message: {e}")
                    
        except Exception as e:
            self.logger.error(f"Error in Kafka consumer: {e}")
            raise
    
    def cleanup(self):
        """Clean up resources."""
        try:
            if self.anomaly_producer:
                self.anomaly_producer.close()
            
            if self.db_storage:
                self.db_storage.close()
            
            if self.spark:
                self.spark.stop()
                
            self.logger.info("Cleanup completed")
        except Exception as e:
            self.logger.error(f"Error during cleanup: {e}")


def main():
    """Main function to run the anomaly detector."""
    parser = argparse.ArgumentParser(description='FX Anomaly Detection Consumer')
    parser.add_argument('--config', type=str, default='config/config.yaml',
                       help='Path to configuration file')
    parser.add_argument('--mode', type=str, choices=['streaming', 'consumer'], 
                       default='consumer', help='Processing mode')
    
    args = parser.parse_args()
    
    detector = None
    try:
        detector = FXAnomalyDetector(config_path=args.config)
        
        if args.mode == 'streaming':
            detector.start_streaming()
        else:
            detector.start_kafka_consumer()
            
    except KeyboardInterrupt:
        print("\n🛑 Anomaly detector interrupted by user")
    except Exception as e:
        print(f"\n❌ Anomaly detector failed: {e}")
        sys.exit(1)
    finally:
        if detector:
            detector.cleanup()


if __name__ == "__main__":
    main() 