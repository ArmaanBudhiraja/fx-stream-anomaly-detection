#!/usr/bin/env python3
"""
Setup script for FX Anomaly Detection System.

This script helps users set up the environment and install dependencies.
"""

import os
import sys
import subprocess
import platform
from pathlib import Path


def print_banner():
    """Print setup banner."""
    print("\n" + "="*80)
    print("🔧 FX ANOMALY DETECTION SYSTEM - SETUP")
    print("="*80)
    print("This script will help you set up the FX anomaly detection system.")
    print("="*80 + "\n")


def check_python_version():
    """Check if Python version is compatible."""
    print("🐍 Checking Python version...")
    
    version = sys.version_info
    if version.major < 3 or (version.major == 3 and version.minor < 10):
        print(f"❌ Python 3.10+ required. Current version: {version.major}.{version.minor}")
        return False
    
    print(f"✅ Python {version.major}.{version.minor}.{version.micro} is compatible")
    return True


def check_java():
    """Check if Java is installed (required for Spark)."""
    print("☕ Checking Java installation...")
    
    try:
        result = subprocess.run(
            ["java", "-version"],
            capture_output=True, text=True, timeout=10
        )
        if result.returncode == 0:
            print("✅ Java is installed")
            return True
        else:
            print("❌ Java is not properly installed")
            return False
    except FileNotFoundError:
        print("❌ Java is not installed")
        print("   Please install Java 8+ from: https://adoptium.net/")
        return False
    except Exception as e:
        print(f"❌ Error checking Java: {e}")
        return False


def check_docker():
    """Check if Docker is installed."""
    print("🐳 Checking Docker installation...")
    
    try:
        result = subprocess.run(
            ["docker", "--version"],
            capture_output=True, text=True, timeout=10
        )
        if result.returncode == 0:
            print("✅ Docker is installed")
            return True
        else:
            print("❌ Docker is not properly installed")
            return False
    except FileNotFoundError:
        print("❌ Docker is not installed")
        print("   Please install Docker from: https://docs.docker.com/get-docker/")
        return False
    except Exception as e:
        print(f"❌ Error checking Docker: {e}")
        return False


def install_python_dependencies():
    """Install Python dependencies."""
    print("📦 Installing Python dependencies...")
    
    try:
        # Upgrade pip first
        subprocess.run([sys.executable, "-m", "pip", "install", "--upgrade", "pip"], 
                      check=True, capture_output=True)
        
        # Install requirements
        result = subprocess.run(
            [sys.executable, "-m", "pip", "install", "-r", "requirements.txt"],
            capture_output=True, text=True, timeout=300  # 5 minutes timeout
        )
        
        if result.returncode == 0:
            print("✅ Python dependencies installed successfully")
            return True
        else:
            print(f"❌ Failed to install dependencies: {result.stderr}")
            return False
            
    except subprocess.TimeoutExpired:
        print("❌ Installation timed out")
        return False
    except Exception as e:
        print(f"❌ Error installing dependencies: {e}")
        return False


def create_directories():
    """Create necessary directories."""
    print("📁 Creating directories...")
    
    directories = [
        "logs",
        "checkpoints",
        "checkpoints/sql",
        "checkpoints/streaming",
        "checkpoints/kafka_output",
        "model/eda"
    ]
    
    for directory in directories:
        Path(directory).mkdir(parents=True, exist_ok=True)
        print(f"✅ Created directory: {directory}")


def start_kafka():
    """Start Kafka using Docker Compose."""
    print("🚀 Starting Kafka and Zookeeper...")
    
    try:
        # Change to docker directory
        docker_dir = Path("docker")
        if not docker_dir.exists():
            print("❌ Docker directory not found")
            return False
        
        # Start services
        result = subprocess.run(
            ["docker-compose", "up", "-d"],
            cwd=docker_dir,
            capture_output=True,
            text=True,
            timeout=60
        )
        
        if result.returncode == 0:
            print("✅ Kafka and Zookeeper started successfully")
            
            # Wait a bit for services to be ready
            print("⏳ Waiting for services to be ready...")
            import time
            time.sleep(10)
            
            return True
        else:
            print(f"❌ Failed to start Kafka: {result.stderr}")
            return False
            
    except subprocess.TimeoutExpired:
        print("❌ Starting Kafka timed out")
        return False
    except Exception as e:
        print(f"❌ Error starting Kafka: {e}")
        return False


def verify_kafka():
    """Verify that Kafka is running properly."""
    print("🔍 Verifying Kafka is running...")
    
    try:
        # Check if Kafka container is running
        result = subprocess.run(
            ["docker", "ps", "--filter", "name=kafka", "--format", "{{.Status}}"],
            capture_output=True, text=True, timeout=10
        )
        
        if "Up" in result.stdout:
            print("✅ Kafka is running")
            return True
        else:
            print("❌ Kafka is not running properly")
            return False
            
    except Exception as e:
        print(f"❌ Error verifying Kafka: {e}")
        return False


def print_next_steps():
    """Print next steps for the user."""
    print("\n" + "="*80)
    print("🎉 SETUP COMPLETED SUCCESSFULLY!")
    print("="*80)
    print("\nNext steps:")
    print("1. Train the model:")
    print("   cd model && python train_model.py")
    print("\n2. Run the demo:")
    print("   python run_demo.py")
    print("\n3. Or run components individually:")
    print("   # Start producer")
    print("   cd producer && python kafka_producer.py --batch 100")
    print("   # Start consumer")
    print("   cd consumer && python fx_anomaly_detector.py")
    print("\n4. Monitor Kafka:")
    print("   Open http://localhost:8080 in your browser")
    print("\n5. View logs:")
    print("   tail -f logs/fx_anomaly_detector.log")
    print("\nFor more information, see README.md")
    print("="*80 + "\n")


def main():
    """Main setup function."""
    print_banner()
    
    # Check prerequisites
    if not check_python_version():
        sys.exit(1)
    
    if not check_java():
        print("\n⚠️  Java is required for Apache Spark. Please install it and run setup again.")
        sys.exit(1)
    
    if not check_docker():
        print("\n⚠️  Docker is required for Kafka. Please install it and run setup again.")
        sys.exit(1)
    
    # Install dependencies
    if not install_python_dependencies():
        print("\n❌ Failed to install Python dependencies.")
        sys.exit(1)
    
    # Create directories
    create_directories()
    
    # Start Kafka
    if not start_kafka():
        print("\n❌ Failed to start Kafka.")
        sys.exit(1)
    
    # Verify Kafka
    if not verify_kafka():
        print("\n❌ Kafka verification failed.")
        sys.exit(1)
    
    # Print next steps
    print_next_steps()


if __name__ == "__main__":
    main()