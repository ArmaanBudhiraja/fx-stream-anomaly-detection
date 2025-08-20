#!/usr/bin/env python3
"""
Database Storage for FX Anomalies

This module provides database storage functionality for FX anomalies and trades,
supporting multiple database backends (SQLite, PostgreSQL, MongoDB).
"""

import json
import sqlite3
import os
from datetime import datetime
from typing import Dict, Any, List, Optional
import sys

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from consumer.utils import load_config, setup_logging

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    POSTGRESQL_AVAILABLE = True
except ImportError:
    POSTGRESQL_AVAILABLE = False


class DatabaseStorage:
    """Database storage class for FX anomalies and trades."""
    
    def __init__(self, config_path: str = "config/config.yaml"):
        """
        Initialize database storage.
        
        Args:
            config_path: Path to configuration file
        """
        self.config = load_config(config_path)
        self.logger = setup_logging(self.config)
        
        # Database configuration
        output_config = self.config.get('output', {})
        self.database_enabled = output_config.get('database_enabled', False)
        self.database_type = output_config.get('database_type', 'sqlite')
        
        if not self.database_enabled:
            self.logger.warning("Database storage is disabled in config")
            return
        
        # Initialize database connection
        self.connection = None
        self._initialize_database()
    
    def _initialize_database(self):
        """Initialize database connection based on configuration."""
        try:
            if self.database_type == 'sqlite':
                self._initialize_sqlite()
            elif self.database_type == 'postgresql':
                if not POSTGRESQL_AVAILABLE:
                    raise ImportError("psycopg2 is required for PostgreSQL support. Install with: pip install psycopg2-binary")
                self._initialize_postgresql()
            elif self.database_type == 'mongodb':
                self._initialize_mongodb()
            else:
                raise ValueError(f"Unsupported database type: {self.database_type}")
                
            self.logger.info(f"Database initialized: {self.database_type}")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize database: {e}")
            raise
    
    def _initialize_sqlite(self):
        """Initialize SQLite database."""
        db_config = self.config.get('database', {}).get('sqlite', {})
        db_path = db_config.get('database_path', './data/fx_anomalies.db')
        
        # Create data directory if it doesn't exist
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        
        self.connection = sqlite3.connect(db_path, check_same_thread=False)
        self.connection.row_factory = sqlite3.Row
        
        # Create tables
        self._create_sqlite_tables()
    
    def _initialize_postgresql(self):
        """Initialize PostgreSQL database."""
        db_config = self.config.get('database', {}).get('postgresql', {})
        
        # Connection parameters
        host = db_config.get('host', 'localhost')
        port = db_config.get('port', 5432)
        database = db_config.get('database', 'fx_anomalies')
        username = db_config.get('username', 'postgres')
        password = db_config.get('password', 'postgres')
        
        # Create connection
        self.connection = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=username,
            password=password
        )
        self.connection.autocommit = True
        
        # Create tables
        self._create_postgresql_tables()
    
    def _create_postgresql_tables(self):
        """Create PostgreSQL tables for anomalies and trades."""
        cursor = self.connection.cursor()
        
        # Anomalies table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS anomalies (
                id SERIAL PRIMARY KEY,
                trade_id VARCHAR(255) UNIQUE NOT NULL,
                timestamp TIMESTAMP NOT NULL,
                currency_pair VARCHAR(20) NOT NULL,
                price DECIMAL(10, 5) NOT NULL,
                amount DECIMAL(15, 2) NOT NULL,
                notional_value DECIMAL(15, 2) NOT NULL,
                anomaly_score DECIMAL(10, 6) NOT NULL,
                detected_at TIMESTAMP NOT NULL,
                data_source VARCHAR(50),
                is_real_data BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Trades table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS trades (
                id SERIAL PRIMARY KEY,
                trade_id VARCHAR(255) UNIQUE NOT NULL,
                timestamp TIMESTAMP NOT NULL,
                currency_pair VARCHAR(20) NOT NULL,
                price DECIMAL(10, 5) NOT NULL,
                amount DECIMAL(15, 2) NOT NULL,
                notional_value DECIMAL(15, 2) NOT NULL,
                buyer_id VARCHAR(100),
                seller_id VARCHAR(100),
                data_source VARCHAR(50),
                is_real_data BOOLEAN DEFAULT FALSE,
                is_anomaly BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Create indexes for better performance
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_anomalies_timestamp ON anomalies(timestamp)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_anomalies_currency_pair ON anomalies(currency_pair)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_trades_timestamp ON trades(timestamp)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_trades_currency_pair ON trades(currency_pair)')
        
        self.logger.info("PostgreSQL tables created successfully")
    
    def _create_sqlite_tables(self):
        """Create SQLite tables for anomalies and trades."""
        cursor = self.connection.cursor()
        
        # Anomalies table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS anomalies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                trade_id TEXT UNIQUE NOT NULL,
                timestamp TEXT NOT NULL,
                currency_pair TEXT NOT NULL,
                price REAL NOT NULL,
                amount REAL NOT NULL,
                notional_value REAL NOT NULL,
                anomaly_score REAL NOT NULL,
                detected_at TEXT NOT NULL,
                data_source TEXT,
                is_real_data BOOLEAN DEFAULT FALSE,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Trades table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                trade_id TEXT UNIQUE NOT NULL,
                timestamp TEXT NOT NULL,
                currency_pair TEXT NOT NULL,
                price REAL NOT NULL,
                amount REAL NOT NULL,
                notional_value REAL NOT NULL,
                buyer_id TEXT,
                seller_id TEXT,
                data_source TEXT,
                is_real_data BOOLEAN DEFAULT FALSE,
                is_anomaly BOOLEAN DEFAULT FALSE,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Create indexes for better performance
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_anomalies_timestamp ON anomalies(timestamp)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_anomalies_currency_pair ON anomalies(currency_pair)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_trades_timestamp ON trades(timestamp)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_trades_currency_pair ON trades(currency_pair)')
        
        self.connection.commit()
        self.logger.info("SQLite tables created successfully")
    
    def store_anomaly(self, anomaly_data: Dict[str, Any]) -> bool:
        """
        Store an anomaly in the database.
        
        Args:
            anomaly_data: Dictionary containing anomaly data
            
        Returns:
            True if stored successfully, False otherwise
        """
        if not self.database_enabled:
            return False
        
        try:
            if self.database_type == 'sqlite':
                return self._store_anomaly_sqlite(anomaly_data)
            elif self.database_type == 'postgresql':
                return self._store_anomaly_postgresql(anomaly_data)
            else:
                self.logger.error(f"Unsupported database type: {self.database_type}")
                return False
                
        except Exception as e:
            self.logger.error(f"Error storing anomaly: {e}")
            return False
    
    def _store_anomaly_postgresql(self, anomaly_data: Dict[str, Any]) -> bool:
        """Store anomaly in PostgreSQL database."""
        cursor = self.connection.cursor()
        
        cursor.execute('''
            INSERT INTO anomalies 
            (trade_id, timestamp, currency_pair, price, amount, notional_value, 
             anomaly_score, detected_at, data_source, is_real_data)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (trade_id) DO UPDATE SET
                timestamp = EXCLUDED.timestamp,
                price = EXCLUDED.price,
                amount = EXCLUDED.amount,
                notional_value = EXCLUDED.notional_value,
                anomaly_score = EXCLUDED.anomaly_score,
                detected_at = EXCLUDED.detected_at,
                data_source = EXCLUDED.data_source,
                is_real_data = EXCLUDED.is_real_data
        ''', (
            anomaly_data['trade_id'],
            anomaly_data['timestamp'],
            anomaly_data['currency_pair'],
            anomaly_data['price'],
            anomaly_data['amount'],
            anomaly_data['amount'] * anomaly_data['price'],
            anomaly_data['anomaly_score'],
            anomaly_data['detected_at'],
            anomaly_data.get('data_source', 'unknown'),
            anomaly_data.get('is_real_data', False)
        ))
        
        return True
    
    def _store_anomaly_sqlite(self, anomaly_data: Dict[str, Any]) -> bool:
        """Store anomaly in SQLite database."""
        cursor = self.connection.cursor()
        
        cursor.execute('''
            INSERT OR REPLACE INTO anomalies 
            (trade_id, timestamp, currency_pair, price, amount, notional_value, 
             anomaly_score, detected_at, data_source, is_real_data)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            anomaly_data['trade_id'],
            anomaly_data['timestamp'],
            anomaly_data['currency_pair'],
            anomaly_data['price'],
            anomaly_data['amount'],
            anomaly_data['amount'] * anomaly_data['price'],
            anomaly_data['anomaly_score'],
            anomaly_data['detected_at'],
            anomaly_data.get('data_source', 'unknown'),
            anomaly_data.get('is_real_data', False)
        ))
        
        self.connection.commit()
        return True
    
    def get_anomalies(self, limit: int = 100, currency_pair: str = None) -> List[Dict[str, Any]]:
        """
        Retrieve anomalies from database.
        
        Args:
            limit: Maximum number of anomalies to retrieve
            currency_pair: Filter by currency pair (optional)
            
        Returns:
            List of anomaly dictionaries
        """
        if not self.database_enabled:
            return []
        
        try:
            if self.database_type == 'sqlite':
                return self._get_anomalies_sqlite(limit, currency_pair)
            elif self.database_type == 'postgresql':
                return self._get_anomalies_postgresql(limit, currency_pair)
            else:
                return []
                
        except Exception as e:
            self.logger.error(f"Error retrieving anomalies: {e}")
            return []
    
    def _get_anomalies_postgresql(self, limit: int, currency_pair: str) -> List[Dict[str, Any]]:
        """Retrieve anomalies from PostgreSQL database."""
        cursor = self.connection.cursor(cursor_factory=RealDictCursor)
        
        query = '''
            SELECT * FROM anomalies 
            WHERE 1=1
        '''
        params = []
        
        if currency_pair:
            query += ' AND currency_pair = %s'
            params.append(currency_pair)
        
        query += ' ORDER BY detected_at DESC LIMIT %s'
        params.append(limit)
        
        cursor.execute(query, params)
        results = cursor.fetchall()
        
        return [dict(row) for row in results]
    
    def _get_anomalies_sqlite(self, limit: int, currency_pair: str) -> List[Dict[str, Any]]:
        """Retrieve anomalies from SQLite database."""
        cursor = self.connection.cursor()
        
        query = '''
            SELECT * FROM anomalies 
            WHERE 1=1
        '''
        params = []
        
        if currency_pair:
            query += ' AND currency_pair = ?'
            params.append(currency_pair)
        
        query += ' ORDER BY detected_at DESC LIMIT ?'
        params.append(limit)
        
        cursor.execute(query, params)
        columns = [description[0] for description in cursor.description]
        results = cursor.fetchall()
        
        return [dict(zip(columns, row)) for row in results]
    
    def get_anomaly_stats(self) -> Dict[str, Any]:
        """
        Get anomaly statistics from database.
        
        Returns:
            Dictionary containing anomaly statistics
        """
        if not self.database_enabled:
            return {}
        
        try:
            if self.database_type == 'sqlite':
                return self._get_anomaly_stats_sqlite()
            elif self.database_type == 'postgresql':
                return self._get_anomaly_stats_postgresql()
            else:
                return {}
                
        except Exception as e:
            self.logger.error(f"Error retrieving anomaly stats: {e}")
            return {}
    
    def _get_anomaly_stats_postgresql(self) -> Dict[str, Any]:
        """Get anomaly statistics from PostgreSQL database."""
        cursor = self.connection.cursor()
        
        # Total anomalies
        cursor.execute('SELECT COUNT(*) FROM anomalies')
        total_anomalies = cursor.fetchone()[0]
        
        # Anomalies by currency pair
        cursor.execute('''
            SELECT currency_pair, COUNT(*) as count 
            FROM anomalies 
            GROUP BY currency_pair 
            ORDER BY count DESC
        ''')
        anomalies_by_pair = dict(cursor.fetchall())
        
        # Recent anomalies (last 24 hours)
        cursor.execute('''
            SELECT COUNT(*) FROM anomalies 
            WHERE detected_at >= NOW() - INTERVAL '24 hours'
        ''')
        recent_anomalies = cursor.fetchone()[0]
        
        # Average anomaly score
        cursor.execute('SELECT AVG(anomaly_score) FROM anomalies')
        avg_score = cursor.fetchone()[0] or 0
        
        return {
            'total_anomalies': total_anomalies,
            'recent_anomalies': recent_anomalies,
            'anomalies_by_pair': anomalies_by_pair,
            'average_score': float(avg_score)
        }
    
    def _get_anomaly_stats_sqlite(self) -> Dict[str, Any]:
        """Get anomaly statistics from SQLite database."""
        cursor = self.connection.cursor()
        
        # Total anomalies
        cursor.execute('SELECT COUNT(*) FROM anomalies')
        total_anomalies = cursor.fetchone()[0]
        
        # Anomalies by currency pair
        cursor.execute('''
            SELECT currency_pair, COUNT(*) as count 
            FROM anomalies 
            GROUP BY currency_pair 
            ORDER BY count DESC
        ''')
        anomalies_by_pair = dict(cursor.fetchall())
        
        # Recent anomalies (last 24 hours)
        cursor.execute('''
            SELECT COUNT(*) FROM anomalies 
            WHERE detected_at >= datetime('now', '-24 hours')
        ''')
        recent_anomalies = cursor.fetchone()[0]
        
        # Average anomaly score
        cursor.execute('SELECT AVG(anomaly_score) FROM anomalies')
        avg_score = cursor.fetchone()[0] or 0
        
        return {
            'total_anomalies': total_anomalies,
            'recent_anomalies': recent_anomalies,
            'anomalies_by_pair': anomalies_by_pair,
            'average_score': float(avg_score)
        }
    
    def close(self):
        """Close database connection."""
        if self.connection:
            self.connection.close()
            self.logger.info("Database connection closed")


def main():
    """Test database storage functionality."""
    storage = DatabaseStorage()
    
    # Test anomaly data
    test_anomaly = {
        'trade_id': 'test_001',
        'timestamp': datetime.now().isoformat(),
        'currency_pair': 'EUR/USD',
        'price': 1.0850,
        'amount': 100000,
        'anomaly_score': 0.95,
        'detected_at': datetime.now().isoformat(),
        'data_source': 'test',
        'is_real_data': False
    }
    
    # Store anomaly
    success = storage.store_anomaly(test_anomaly)
    print(f"Stored anomaly: {success}")
    
    # Get anomalies
    anomalies = storage.get_anomalies(limit=10)
    print(f"Retrieved {len(anomalies)} anomalies")
    
    # Get stats
    stats = storage.get_anomaly_stats()
    print(f"Anomaly stats: {stats}")
    
    storage.close()


if __name__ == "__main__":
    main() 