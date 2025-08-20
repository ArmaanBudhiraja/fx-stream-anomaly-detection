"""
Unit tests for utility functions in the FX Anomaly Detection System.
"""

import pytest
import json
import tempfile
import os
import yaml
from datetime import datetime
from unittest.mock import patch, MagicMock

import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from consumer.utils import (
    load_config, setup_logging, get_fx_trade_schema, parse_json_message,
    enrich_trade_data, validate_trade_data, prepare_features_for_model,
    format_anomaly_output, create_spark_session
)
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType


class TestConfigLoading:
    """Test configuration loading functionality."""
    
    def test_load_config_success(self):
        """Test successful configuration loading."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump({'test': 'value'}, f)
            config_path = f.name
        
        try:
            config = load_config(config_path)
            assert config['test'] == 'value'
        finally:
            os.unlink(config_path)
    
    def test_load_config_file_not_found(self):
        """Test configuration loading with non-existent file."""
        with pytest.raises(FileNotFoundError):
            load_config('non_existent_file.yaml')
    
    def test_load_config_invalid_yaml(self):
        """Test configuration loading with invalid YAML."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write('invalid: yaml: content: [')
            config_path = f.name
        
        try:
            with pytest.raises(yaml.YAMLError):
                load_config(config_path)
        finally:
            os.unlink(config_path)


class TestLogging:
    """Test logging setup functionality."""
    
    def test_setup_logging(self):
        """Test logging setup."""
        config = {'logging': {'level': 'INFO', 'file': './test_log.log'}}
        
        # Create logs directory
        os.makedirs('./logs', exist_ok=True)
        
        logger = setup_logging(config)
        assert logger is not None
        
        # Clean up
        if os.path.exists('./test_log.log'):
            os.unlink('./test_log.log')


class TestSchema:
    """Test schema definition functionality."""
    
    def test_get_fx_trade_schema(self):
        """Test FX trade schema creation."""
        schema = get_fx_trade_schema()
        
        assert isinstance(schema, StructType)
        assert len(schema.fields) == 9  # Expected number of fields
        
        # Check specific fields
        field_names = [field.name for field in schema.fields]
        expected_fields = [
            'trade_id', 'timestamp', 'currency_pair', 'amount', 'price',
            'buyer_id', 'seller_id', 'notional_value', 'processing_timestamp'
        ]
        
        for field in expected_fields:
            assert field in field_names


class TestMessageParsing:
    """Test JSON message parsing functionality."""
    
    def test_parse_json_message_success(self):
        """Test successful JSON message parsing."""
        message = '{"trade_id": "123", "amount": 1000.0}'
        result = parse_json_message(message)
        
        assert result is not None
        assert result['trade_id'] == '123'
        assert result['amount'] == 1000.0
        assert 'processing_timestamp' in result
    
    def test_parse_json_message_invalid_json(self):
        """Test JSON message parsing with invalid JSON."""
        message = 'invalid json'
        result = parse_json_message(message)
        
        assert result is None
    
    def test_parse_json_message_empty(self):
        """Test JSON message parsing with empty message."""
        result = parse_json_message('')
        
        assert result is None


class TestDataProcessing:
    """Test data processing functionality."""
    
    @pytest.fixture
    def spark_session(self):
        """Create a Spark session for testing."""
        spark = SparkSession.builder \
            .appName("TestFXAnomaly") \
            .master("local[1]") \
            .getOrCreate()
        yield spark
        spark.stop()
    
    def test_enrich_trade_data(self, spark_session):
        """Test trade data enrichment."""
        # Create test data
        data = [
            ('TRADE_001', datetime.now(), 'EUR/USD', 1000.0, 1.1000, 'BUYER_001', 'SELLER_001')
        ]
        df = spark_session.createDataFrame(data, ['trade_id', 'timestamp', 'currency_pair', 'amount', 'price', 'buyer_id', 'seller_id'])
        
        # Enrich data
        enriched_df = enrich_trade_data(df)
        
        # Check that notional_value was added
        assert 'notional_value' in enriched_df.columns
        
        # Check calculation
        row = enriched_df.first()
        expected_notional = 1000.0 * 1.1000
        assert abs(row['notional_value'] - expected_notional) < 0.01
    
    def test_validate_trade_data(self, spark_session):
        """Test trade data validation."""
        # Create test data with some invalid records
        data = [
            ('TRADE_001', datetime.now(), 'EUR/USD', 1000.0, 1.1000, 'BUYER_001', 'SELLER_001'),  # Valid
            ('TRADE_002', datetime.now(), None, 1000.0, 1.1000, 'BUYER_001', 'SELLER_001'),      # Invalid (null currency)
            ('TRADE_003', datetime.now(), 'EUR/USD', -100.0, 1.1000, 'BUYER_001', 'SELLER_001'), # Invalid (negative amount)
            ('TRADE_004', datetime.now(), 'EUR/USD', 1000.0, 0.0, 'BUYER_001', 'SELLER_001'),    # Invalid (zero price)
        ]
        df = spark_session.createDataFrame(data, ['trade_id', 'timestamp', 'currency_pair', 'amount', 'price', 'buyer_id', 'seller_id'])
        
        # Validate data
        valid_df = validate_trade_data(df)
        
        # Should only have 1 valid record
        assert valid_df.count() == 1
        assert valid_df.first()['trade_id'] == 'TRADE_001'
    
    def test_prepare_features_for_model(self, spark_session):
        """Test feature preparation for model."""
        # Create test data
        data = [
            ('TRADE_001', datetime.now(), 'EUR/USD', 1000.0, 1.1000, 'BUYER_001', 'SELLER_001', 1100.0)
        ]
        df = spark_session.createDataFrame(data, ['trade_id', 'timestamp', 'currency_pair', 'amount', 'price', 'buyer_id', 'seller_id', 'notional_value'])
        
        # Prepare features
        feature_df = prepare_features_for_model(df)
        
        # Check that required features are present
        expected_features = ['amount', 'price', 'notional_value', 'currency_pair_encoded', 'hour']
        for feature in expected_features:
            assert feature in feature_df.columns
        
        # Check that trade_id is preserved
        assert 'trade_id' in feature_df.columns


class TestAnomalyOutput:
    """Test anomaly output formatting."""
    
    @pytest.fixture
    def spark_session(self):
        """Create a Spark session for testing."""
        spark = SparkSession.builder \
            .appName("TestFXAnomaly") \
            .master("local[1]") \
            .getOrCreate()
        yield spark
        spark.stop()
    
    def test_format_anomaly_output(self, spark_session):
        """Test anomaly output formatting."""
        # Create test anomaly data
        data = [
            ('TRADE_001', datetime.now(), 'EUR/USD', 1000.0, 1.1000, 1100.0, -0.5)
        ]
        df = spark_session.createDataFrame(data, ['trade_id', 'timestamp', 'currency_pair', 'amount', 'price', 'notional_value', 'anomaly_score'])
        
        config = {'output': {'console_enabled': True}}
        
        # Format output
        output = format_anomaly_output(df, config)
        
        # Should be valid JSON
        parsed = json.loads(output)
        assert 'anomalies' in parsed
        assert 'count' in parsed
        assert parsed['count'] == 1
        
        # Check anomaly data
        anomaly = parsed['anomalies'][0]
        assert anomaly['trade_id'] == 'TRADE_001'
        assert anomaly['currency_pair'] == 'EUR/USD'
        assert anomaly['amount'] == 1000.0


class TestSparkSession:
    """Test Spark session creation."""
    
    def test_create_spark_session(self):
        """Test Spark session creation."""
        config = {
            'spark': {
                'app_name': 'TestApp',
                'master': 'local[1]',
                'spark_sql_streaming_checkpoint_location': './test_checkpoints'
            }
        }
        
        spark = create_spark_session(config)
        
        assert isinstance(spark, SparkSession)
        assert spark.conf.get('spark.app.name') == 'TestApp'
        
        spark.stop()


if __name__ == '__main__':
    pytest.main([__file__]) 