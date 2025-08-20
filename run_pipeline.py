#!/usr/bin/env python3
"""
FX Anomaly Detection Pipeline Runner

This script helps you choose the right pipeline to run.
"""

import sys
import subprocess
import os

def print_banner():
    """Print a nice banner."""
    print("=" * 60)
    print("🚀 FX ANOMALY DETECTION SYSTEM")
    print("=" * 60)

def print_options():
    """Print the available options."""
    print("\n📋 Available Pipeline Options:")
    print()
    print("1️⃣  WORKING PIPELINE (RECOMMENDED)")
    print("   Command: python run_working_pipeline.py")
    print("   ✅ Simplified and reliable")
    print("   ✅ Bypasses Kafka consumer issues")
    print("   ✅ Direct data processing")
    print("   ✅ Better for development and testing")
    print()
    print("2️⃣  FULL PIPELINE (ADVANCED)")
    print("   Command: python run_full_pipeline.py")
    print("   ⚠️  Complete Kafka streaming")
    print("   ⚠️  More complex setup")
    print("   ⚠️  May have connectivity issues")
    print("   ⚠️  Production-ready architecture")
    print()

def get_user_choice():
    """Get user's choice."""
    while True:
        choice = input("🤔 Which pipeline would you like to run? (1 or 2): ").strip()
        if choice in ['1', '2']:
            return choice
        print("❌ Please enter 1 or 2")

def run_pipeline(choice):
    """Run the selected pipeline."""
    if choice == '1':
        print("\n🚀 Starting WORKING PIPELINE...")
        print("This is the recommended option for most users.")
        print("It will process FX data directly and detect anomalies.")
        print("Dashboard will be available at: http://localhost:5000")
        print("\n" + "="*50)
        
        try:
            subprocess.run([sys.executable, "run_working_pipeline.py"], check=True)
        except KeyboardInterrupt:
            print("\n⏹️  Pipeline stopped by user")
        except subprocess.CalledProcessError as e:
            print(f"\n❌ Pipeline failed with error: {e}")
            return False
    
    elif choice == '2':
        print("\n🚀 Starting FULL PIPELINE...")
        print("This uses complete Kafka streaming architecture.")
        print("Make sure all Kafka services are properly configured.")
        print("Dashboard will be available at: http://localhost:5000")
        print("\n" + "="*50)
        
        try:
            subprocess.run([sys.executable, "run_full_pipeline.py"], check=True)
        except KeyboardInterrupt:
            print("\n⏹️  Pipeline stopped by user")
        except subprocess.CalledProcessError as e:
            print(f"\n❌ Pipeline failed with error: {e}")
            return False
    
    return True

def main():
    """Main function."""
    print_banner()
    
    # Check for help flag
    if len(sys.argv) > 1 and sys.argv[1] in ['--help', '-h', 'help']:
        print_options()
        print("\n💡 Usage:")
        print("   python run_pipeline.py          # Interactive menu")
        print("   python run_pipeline.py 1        # Run working pipeline")
        print("   python run_pipeline.py 2        # Run full pipeline")
        print("   python run_pipeline.py --help   # Show this help")
        return
    
    print_options()
    
    # Check if user provided command line argument
    if len(sys.argv) > 1:
        choice = sys.argv[1]
        if choice not in ['1', '2']:
            print("❌ Invalid choice. Use 1 for working pipeline or 2 for full pipeline.")
            print("💡 Run 'python run_pipeline.py --help' for usage information.")
            sys.exit(1)
    else:
        choice = get_user_choice()
    
    success = run_pipeline(choice)
    
    if not success:
        print("\n💡 If you're having issues, try the working pipeline (option 1)")
        print("   It's more reliable and bypasses common Kafka issues.")
    
    print("\n👋 Thanks for using FX Anomaly Detection System!")

if __name__ == "__main__":
    main() 