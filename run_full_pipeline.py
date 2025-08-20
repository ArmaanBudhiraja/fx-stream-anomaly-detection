#!/usr/bin/env python3
"""
Full FX Anomaly Detection Pipeline

This script runs the complete pipeline including:
1. Infrastructure (Kafka + PostgreSQL)
2. Real-time FX data producer
3. Anomaly detection consumer
4. Web dashboard
"""

import os
import sys
import time
import signal
import subprocess
import threading
from datetime import datetime
from typing import Optional

# Add current directory to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from consumer.utils import load_config, setup_logging
from consumer.database_storage import DatabaseStorage


class FXAnomalyPipeline:
    """Complete FX anomaly detection pipeline."""
    
    def __init__(self, config_path: str = "config/config.yaml"):
        """
        Initialize the pipeline.
        
        Args:
            config_path: Path to configuration file
        """
        self.config = load_config(config_path)
        self.logger = setup_logging(self.config)
        
        # Process handles
        self.producer_process = None
        self.consumer_process = None
        self.dashboard_process = None
        
        # Control flags
        self.running = False
        self.shutdown_event = threading.Event()
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals."""
        self.logger.info(f"Received signal {signum}, shutting down...")
        self.shutdown()
    
    def check_prerequisites(self) -> bool:
        """Check if all prerequisites are met."""
        self.logger.info("Checking prerequisites...")
        
        # Check Python dependencies
        try:
            import pyspark
            import kafka
            import sklearn
            import pandas
            import numpy
            import requests
            import flask
            self.logger.info("✅ All Python dependencies are installed")
        except ImportError as e:
            self.logger.error(f"❌ Missing Python dependency: {e}")
            return False
        
        # Check Docker
        try:
            result = subprocess.run(['docker', '--version'], 
                                  capture_output=True, text=True, timeout=10)
            if result.returncode == 0:
                self.logger.info(f"✅ Docker is available: {result.stdout.strip()}")
            else:
                self.logger.error("❌ Docker is not available")
                return False
        except Exception as e:
            self.logger.error(f"❌ Error checking Docker: {e}")
            return False
        
        # Check Docker Compose
        try:
            result = subprocess.run(['docker-compose', '--version'], 
                                  capture_output=True, text=True, timeout=10)
            if result.returncode == 0:
                self.logger.info(f"✅ Docker Compose is available: {result.stdout.strip()}")
            else:
                self.logger.error("❌ Docker Compose is not available")
                return False
        except Exception as e:
            self.logger.error(f"❌ Error checking Docker Compose: {e}")
            return False
        
        return True
    
    def start_infrastructure(self) -> bool:
        """Start Kafka and PostgreSQL infrastructure."""
        self.logger.info("Starting infrastructure (Kafka + PostgreSQL)...")
        
        try:
            # Start Docker Compose services
            result = subprocess.run(
                ['docker-compose', '-f', 'docker/docker-compose.yml', 'up', '-d'],
                capture_output=True, text=True, timeout=60
            )
            
            if result.returncode == 0:
                self.logger.info("✅ Infrastructure started successfully")
                return True
            else:
                self.logger.error(f"❌ Failed to start infrastructure: {result.stderr}")
                return False
                
        except Exception as e:
            self.logger.error(f"❌ Error starting infrastructure: {e}")
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
    
    def test_database_connection(self) -> bool:
        """Test database connection."""
        self.logger.info("Testing database connection...")
        
        try:
            db_storage = DatabaseStorage("config/config.yaml")
            db_storage._initialize_database()
            db_storage.close()
            self.logger.info("✅ Database connection successful")
            return True
        except Exception as e:
            self.logger.error(f"❌ Database connection failed: {e}")
            return False
    
    def check_model(self) -> bool:
        """Check if model exists."""
        model_path = self.config.get('model', {}).get('path', './model/fx_anomaly_model.pkl')
        
        if os.path.exists(model_path):
            self.logger.info("✅ Model already exists, skipping training")
            return True
        else:
            self.logger.warning("⚠️ Model not found, training new model...")
            return self.train_model()
    
    def train_model(self) -> bool:
        """Train the anomaly detection model."""
        try:
            self.logger.info("Training anomaly detection model...")
            
            result = subprocess.run(
                [sys.executable, 'model/train_model.py'],
                capture_output=True, text=True, timeout=300
            )
            
            if result.returncode == 0:
                self.logger.info("✅ Model training completed")
                return True
            else:
                self.logger.error(f"❌ Model training failed: {result.stderr}")
                return False
                
        except Exception as e:
            self.logger.error(f"❌ Error training model: {e}")
            return False
    
    def start_producer(self) -> bool:
        """Start the real-time FX data producer."""
        self.logger.info("Starting real-time FX data producer...")
        
        try:
            # Use Alpha Vantage as primary source
            self.producer_process = subprocess.Popen([
                sys.executable, 'producer/real_time_fx_producer.py',
                '--source', 'alpha_vantage',
                '--duration', '60'
            ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            
            # Wait a moment to see if it starts successfully
            time.sleep(2)
            
            if self.producer_process.poll() is None:
                self.logger.info("✅ Producer started")
                return True
            else:
                stdout, stderr = self.producer_process.communicate()
                self.logger.error(f"❌ Producer failed to start: {stderr}")
                return False
                
        except Exception as e:
            self.logger.error(f"❌ Error starting producer: {e}")
            return False
    
    def start_consumer(self) -> bool:
        """Start the anomaly detection consumer."""
        self.logger.info("Starting anomaly detection consumer...")
        
        try:
            self.consumer_process = subprocess.Popen([
                sys.executable, 'consumer/fx_anomaly_detector.py'
            ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            
            # Wait a moment to see if it starts successfully
            time.sleep(2)
            
            if self.consumer_process.poll() is None:
                self.logger.info("✅ Consumer started")
                return True
            else:
                stdout, stderr = self.consumer_process.communicate()
                self.logger.error(f"❌ Consumer failed to start: {stderr}")
                return False
                
        except Exception as e:
            self.logger.error(f"❌ Error starting consumer: {e}")
            return False
    
    def start_dashboard(self) -> bool:
        """Start the web dashboard."""
        self.logger.info("Starting web dashboard...")
        
        try:
            self.dashboard_process = subprocess.Popen([
                sys.executable, 'dashboard/anomaly_dashboard.py',
                '--web',
                '--host', 'localhost',
                '--port', '5000'
            ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            
            # Wait a moment to see if it starts successfully
            time.sleep(3)
            
            if self.dashboard_process.poll() is None:
                self.logger.info("✅ Dashboard started at http://localhost:5000")
                return True
            else:
                stdout, stderr = self.dashboard_process.communicate()
                self.logger.error(f"❌ Dashboard failed to start: {stderr}")
                return False
                
        except Exception as e:
            self.logger.error(f"❌ Error starting dashboard: {e}")
            return False
    
    def monitor_processes(self):
        """Monitor running processes."""
        while self.running and not self.shutdown_event.is_set():
            # Check producer
            if self.producer_process and self.producer_process.poll() is not None:
                self.logger.warning("Process producer has stopped")
            
            # Check consumer
            if self.consumer_process and self.consumer_process.poll() is not None:
                self.logger.warning("Process consumer has stopped")
            
            # Check dashboard
            if self.dashboard_process and self.dashboard_process.poll() is not None:
                self.logger.warning("Process dashboard has stopped")
            
            time.sleep(5)
    
    def print_status(self):
        """Print pipeline status and URLs."""
        print("\n" + "="*80)
        print("🎉 Pipeline is running!")
        print("="*80)
        print("📋 Service URLs:")
        print("   Kafka: localhost:9092")
        print("   PostgreSQL: localhost:5432")
        print("   Kafka UI: http://localhost:8080")
        print("   pgAdmin: http://localhost:5050 (admin@fxanomaly.com / admin)")
        print("   Dashboard: http://localhost:5000")
        print("="*80)
        print("💾 Database Info:")
        print("   Database: fx_anomalies")
        print("   Username: postgres")
        print("   Password: postgres")
        print("="*80)
        print("🔍 Monitoring:")
        print("   - Check logs in ./logs/ directory")
        print("   - View anomalies in PostgreSQL database")
        print("   - Monitor Kafka topics via Kafka UI")
        print("   - Access dashboard for real-time visualization")
        print("="*80)
        print("⚠️  Real-time Data Notes:")
        print("   - Using Alpha Vantage API for real FX data")
        print("   - Update interval: 60 seconds (respects API limits)")
        print("   - Falls back to synthetic data if API fails")
        print("="*80)
        print("Monitoring pipeline...")
        print("Press Ctrl+C to stop the pipeline")
    
    def run(self):
        """Run the complete pipeline."""
        self.logger.info("🚀 Starting Full FX Anomaly Detection Pipeline")
        
        # Check mode
        real_time_enabled = self.config.get('data', {}).get('real_time_enabled', False)
        mode = "Real-time FX data" if real_time_enabled else "Synthetic data"
        self.logger.info(f"Mode: {mode}")
        
        # Check dashboard
        dashboard_enabled = True  # Always enable dashboard
        self.logger.info(f"Dashboard: {'Enabled' if dashboard_enabled else 'Disabled'}")
        
        # Check prerequisites
        if not self.check_prerequisites():
            self.logger.error("❌ Prerequisites check failed")
            return False
        
        # Start infrastructure
        if not self.start_infrastructure():
            self.logger.error("❌ Failed to start infrastructure")
            return False
        
        # Wait for services
        if not self.wait_for_services():
            self.logger.error("❌ Services failed to start")
            return False
        
        # Test database connection
        if not self.test_database_connection():
            self.logger.error("❌ Database connection failed")
            return False
        
        # Check/train model
        if not self.check_model():
            self.logger.error("❌ Model training failed")
            return False
        
        # Start producer
        if not self.start_producer():
            self.logger.error("❌ Failed to start producer")
            return False
        
        # Wait a moment for producer to start
        time.sleep(5)
        
        # Start consumer
        if not self.start_consumer():
            self.logger.error("❌ Failed to start consumer")
            return False
        
        # Start dashboard if enabled
        if dashboard_enabled:
            if not self.start_dashboard():
                self.logger.error("❌ Failed to start dashboard")
                return False
        
        # Print status
        self.print_status()
        
        # Start monitoring
        self.running = True
        monitor_thread = threading.Thread(target=self.monitor_processes)
        monitor_thread.daemon = True
        monitor_thread.start()
        
        # Keep running until shutdown
        try:
            while self.running and not self.shutdown_event.is_set():
                time.sleep(1)
        except KeyboardInterrupt:
            self.logger.info("Received interrupt signal")
        
        return True
    
    def shutdown(self):
        """Shutdown the pipeline."""
        self.logger.info("Stopping pipeline...")
        self.running = False
        self.shutdown_event.set()
        
        # Stop producer
        if self.producer_process:
            self.logger.info("Stopping producer...")
            self.producer_process.terminate()
            try:
                self.producer_process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.producer_process.kill()
        
        # Stop consumer
        if self.consumer_process:
            self.logger.info("Stopping consumer...")
            self.consumer_process.terminate()
            try:
                self.consumer_process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.consumer_process.kill()
        
        # Stop dashboard
        if self.dashboard_process:
            self.logger.info("Stopping dashboard...")
            self.dashboard_process.terminate()
            try:
                self.dashboard_process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.dashboard_process.kill()
        
        self.logger.info("Pipeline stopped")


def main():
    """Main function."""
    pipeline = FXAnomalyPipeline()
    
    try:
        success = pipeline.run()
        if not success:
            sys.exit(1)
    except KeyboardInterrupt:
        print("\n🛑 Pipeline interrupted by user")
    except Exception as e:
        print(f"\n❌ Pipeline failed: {e}")
        sys.exit(1)
    finally:
        pipeline.shutdown()


if __name__ == "__main__":
    main() 