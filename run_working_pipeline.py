#!/usr/bin/env python3
"""
Working FX Anomaly Detection Pipeline
This version bypasses Kafka consumer issues and directly processes data
"""

import os
import sys
import time
import signal
import subprocess
import threading
import warnings
from datetime import datetime
from typing import Optional

# Suppress sklearn warnings
warnings.filterwarnings('ignore', category=UserWarning, module='sklearn')

# Add current directory to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from consumer.utils import load_config, setup_logging
from consumer.database_storage import DatabaseStorage
from consumer.fx_anomaly_detector import FXAnomalyDetector
from producer.real_time_fx_producer import RealFXDataProducer
from dashboard.anomaly_dashboard import AnomalyDashboard

class WorkingFXAnomalyPipeline:
    """Working FX anomaly detection pipeline that bypasses Kafka issues."""
    
    def __init__(self, config_path: str = "config/config.yaml"):
        """Initialize the pipeline."""
        self.config = load_config(config_path)
        self.logger = setup_logging(self.config)
        self.running = False
        self.processes = []
        
    def start_infrastructure(self):
        """Start Docker infrastructure."""
        self.logger.info("Starting infrastructure (Kafka + PostgreSQL)...")
        try:
            subprocess.run(["docker-compose", "-f", "docker/docker-compose.yml", "up", "-d"], 
                         check=True, capture_output=True)
            self.logger.info("✅ Infrastructure started successfully")
            return True
        except subprocess.CalledProcessError as e:
            self.logger.error(f"Failed to start infrastructure: {e}")
            return False
    
    def wait_for_services(self, timeout: int = 120) -> bool:
        """Wait for services to be ready."""
        self.logger.info("Waiting for services to be ready...")
        
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                # Test database connection
                db_storage = DatabaseStorage("config/config.yaml")
                db_storage._initialize_database()
                db_storage.close()
                
                self.logger.info("✅ Services are ready")
                return True
                
            except Exception as e:
                if time.time() - start_time > 30:
                    self.logger.info("Waiting for services to be fully ready...")
                time.sleep(5)
        
        self.logger.error("❌ Services failed to start within timeout")
        return False
    
    def start_direct_processing(self):
        """Start direct processing without Kafka consumer issues."""
        self.logger.info("Starting direct FX data processing...")
        
        # Initialize components
        detector = FXAnomalyDetector("config/config.yaml")
        db_storage = DatabaseStorage("config/config.yaml")
        db_storage._initialize_database()
        
        # Create producer for data generation
        producer = RealFXDataProducer("config/config.yaml")
        
        self.running = True
        anomaly_count = 0
        
        try:
            while self.running:
                # Generate real FX data
                currency_pairs = self.config['data']['fx_data_sources']['exchange_rate_host']['currency_pairs']
                
                for currency_pair in currency_pairs:
                    if not self.running:
                        break
                        
                    # Fetch real data
                    trade = producer.fetch_exchange_rate_host_data(currency_pair)
                    if not trade:
                        # Fallback to synthetic data
                        trade = producer._generate_synthetic_fx_data(currency_pair)
                    
                    if trade:
                        self.logger.info(f"Processing trade: {trade['currency_pair']} @ {trade['price']}")
                        
                        # Direct anomaly detection
                        features = detector._extract_features(trade)
                        if features is not None:
                            is_anomaly = detector._detect_anomaly(features)
                            
                            if is_anomaly:
                                anomaly_count += 1
                                self.logger.warning(f"🚨 ANOMALY DETECTED #{anomaly_count}: {trade['currency_pair']} @ {trade['price']}")
                                
                                # Store anomaly
                                anomaly_data = {
                                    'timestamp': trade['timestamp'],
                                    'trade_id': trade['trade_id'],
                                    'currency_pair': trade['currency_pair'],
                                    'price': trade['price'],
                                    'amount': trade['amount'],
                                    'side': trade.get('side', 'unknown'),
                                    'anomaly_score': float(detector.model.decision_function(features)[0]),
                                    'detected_at': datetime.now().isoformat(),
                                    'data_source': trade.get('data_source', 'unknown'),
                                    'is_real_data': trade.get('is_real_data', False)
                                }
                                
                                db_storage.store_anomaly(anomaly_data)
                                print(f"🚨 ANOMALY #{anomaly_count}: {trade['currency_pair']} @ {trade['price']} (Score: {anomaly_data['anomaly_score']:.4f})")
                            else:
                                print(f"✅ Normal: {trade['currency_pair']} @ {trade['price']}")
                
                # Wait before next cycle
                time.sleep(30)  # 30 second intervals
                
        except KeyboardInterrupt:
            self.logger.info("Direct processing interrupted")
        except Exception as e:
            self.logger.error(f"Error in direct processing: {e}")
        finally:
            db_storage.close()
    
    def start_dashboard(self):
        """Start the web dashboard."""
        self.logger.info("Starting web dashboard...")
        try:
            dashboard = AnomalyDashboard("config/config.yaml")
            dashboard.run_dashboard(host='localhost', port=5000, debug=False)
        except Exception as e:
            self.logger.error(f"Dashboard error: {e}")
    
    def run(self):
        """Run the complete pipeline."""
        self.logger.info("🚀 Starting Working FX Anomaly Detection Pipeline")
        self.logger.info("Mode: Direct processing (bypasses Kafka consumer issues)")
        self.logger.info("Dashboard: Enabled")
        
        # Check prerequisites
        self.logger.info("Checking prerequisites...")
        
        # Check Python dependencies
        try:
            import kafka
            import sklearn
            import pandas
            import numpy
            self.logger.info("✅ All Python dependencies are installed")
        except ImportError as e:
            self.logger.error(f"❌ Missing dependency: {e}")
            return
        
        # Check Docker
        try:
            result = subprocess.run(["docker", "--version"], capture_output=True, text=True)
            self.logger.info(f"✅ Docker is available: {result.stdout.strip()}")
        except FileNotFoundError:
            self.logger.error("❌ Docker not found")
            return
        
        # Start infrastructure
        if not self.start_infrastructure():
            return
        
        # Wait for services
        if not self.wait_for_services():
            return
        
        # Test database connection
        self.logger.info("Testing database connection...")
        try:
            db_storage = DatabaseStorage("config/config.yaml")
            db_storage._initialize_database()
            db_storage.close()
            self.logger.info("✅ Database connection successful")
        except Exception as e:
            self.logger.error(f"❌ Database connection failed: {e}")
            return
        
        # Check model
        if os.path.exists(self.config['model']['path']):
            self.logger.info("✅ Model already exists, skipping training")
        else:
            self.logger.info("Training model...")
            try:
                subprocess.run(["python", "model/train_model.py"], check=True)
                self.logger.info("✅ Model trained successfully")
            except subprocess.CalledProcessError:
                self.logger.error("❌ Model training failed")
                return
        
        # Start dashboard in background
        dashboard_thread = threading.Thread(target=self.start_dashboard, daemon=True)
        dashboard_thread.start()
        
        # Start direct processing
        self.logger.info("Starting direct FX data processing...")
        self.start_direct_processing()
        
        self.logger.info("Pipeline stopped")

def main():
    """Main function."""
    pipeline = WorkingFXAnomalyPipeline()
    
    def signal_handler(signum, frame):
        pipeline.running = False
        pipeline.logger.info("Received signal, shutting down...")
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        pipeline.run()
    except KeyboardInterrupt:
        pipeline.logger.info("Pipeline interrupted by user")
    except Exception as e:
        pipeline.logger.error(f"Pipeline failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 