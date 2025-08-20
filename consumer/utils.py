"""
Utility functions for FX Anomaly Detection System.
"""

import json
import logging
import os
import yaml
from datetime import datetime
from typing import Dict, Any, Optional, List
import structlog
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, TimestampType
from pyspark.sql.functions import col, udf, from_json, to_timestamp, lit
from pyspark.sql.types import StringType


def load_config(config_path: str = "config/config.yaml") -> Dict[str, Any]:
    """
    Load configuration from YAML file.
    
    Args:
        config_path: Path to the configuration file
        
    Returns:
        Configuration dictionary
        
    Raises:
        FileNotFoundError: If config file doesn't exist
        yaml.YAMLError: If config file is malformed
    """
    try:
        with open(config_path, 'r') as file:
            config = yaml.safe_load(file)
        return config
    except FileNotFoundError:
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    except yaml.YAMLError as e:
        raise yaml.YAMLError(f"Error parsing configuration file: {e}")


def setup_logging(config: Dict[str, Any]) -> structlog.BoundLogger:
    """
    Setup structured logging for the application.
    
    Args:
        config: Configuration dictionary containing logging settings
        
    Returns:
        Configured logger instance
    """
    log_config = config.get('logging', {})
    
    # Create logs directory if it doesn't exist
    log_file = log_config.get('file', './logs/fx_anomaly_detector.log')
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    
    # Configure structlog
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            structlog.processors.JSONRenderer()
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
    
    # Configure standard logging
    logging.basicConfig(
        level=getattr(logging, log_config.get('level', 'INFO')),
        format=log_config.get('format', '%(asctime)s - %(name)s - %(levelname)s - %(message)s'),
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    
    return structlog.get_logger()


def get_fx_trade_schema() -> StructType:
    """
    Define the schema for FX trade data.
    
    Returns:
        Spark DataFrame schema for FX trades
    """
    return StructType([
        StructField("trade_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("currency_pair", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("price", DoubleType(), True),
        StructField("buyer_id", StringType(), True),
        StructField("seller_id", StringType(), True),
        StructField("notional_value", DoubleType(), True),
        StructField("processing_timestamp", TimestampType(), True)
    ])


def parse_json_message(message: str) -> Optional[Dict[str, Any]]:
    """
    Parse JSON message from Kafka.
    
    Args:
        message: JSON string message
        
    Returns:
        Parsed dictionary or None if parsing fails
    """
    try:
        data = json.loads(message)
        # Add processing timestamp
        data['processing_timestamp'] = datetime.now()
        return data
    except json.JSONDecodeError as e:
        logging.error(f"Failed to parse JSON message: {e}")
        return None
    except Exception as e:
        logging.error(f"Unexpected error parsing message: {e}")
        return None


def enrich_trade_data(df: DataFrame) -> DataFrame:
    """
    Enrich trade data with derived fields.
    
    Args:
        df: Input DataFrame with trade data
        
    Returns:
        Enriched DataFrame
    """
    # Calculate notional value (amount * price)
    df = df.withColumn("notional_value", col("amount") * col("price"))
    
    # Add processing timestamp if not present
    if "processing_timestamp" not in df.columns:
        df = df.withColumn("processing_timestamp", lit(datetime.now()))
    
    return df


def validate_trade_data(df: DataFrame) -> DataFrame:
    """
    Validate and clean trade data.
    
    Args:
        df: Input DataFrame with trade data
        
    Returns:
        Cleaned DataFrame with valid records only
    """
    # Remove records with null values in critical fields
    df = df.filter(
        col("trade_id").isNotNull() &
        col("currency_pair").isNotNull() &
        col("amount").isNotNull() &
        col("price").isNotNull() &
        (col("amount") > 0) &
        (col("price") > 0)
    )
    
    # Remove duplicate trade_ids
    df = df.dropDuplicates(["trade_id"])
    
    return df


def prepare_features_for_model(df: DataFrame) -> DataFrame:
    """
    Prepare features for anomaly detection model.
    
    Args:
        df: Input DataFrame with trade data
        
    Returns:
        DataFrame with features ready for model prediction
    """
    # Create numerical features for currency pairs
    currency_pair_udf = udf(lambda x: hash(x) % 1000, StringType())
    df = df.withColumn("currency_pair_encoded", currency_pair_udf(col("currency_pair")))
    
    # Extract hour from timestamp for time-based features
    df = df.withColumn("hour", col("timestamp").cast("timestamp").hour())
    
    # Create feature columns for model
    feature_columns = [
        "amount",
        "price", 
        "notional_value",
        "currency_pair_encoded",
        "hour"
    ]
    
    return df.select(feature_columns + ["trade_id", "timestamp", "currency_pair"])


def format_anomaly_output(anomaly_df: DataFrame, config: Dict[str, Any]) -> str:
    """
    Format anomaly data for output.
    
    Args:
        anomaly_df: DataFrame containing anomaly records
        config: Configuration dictionary
        
    Returns:
        Formatted JSON string for output
    """
    try:
        # Convert to pandas for easier JSON formatting
        pandas_df = anomaly_df.toPandas()
        
        # Format each anomaly record
        anomalies = []
        for _, row in pandas_df.iterrows():
            anomaly_record = {
                "trade_id": row["trade_id"],
                "timestamp": row["timestamp"].isoformat() if row["timestamp"] else None,
                "currency_pair": row["currency_pair"],
                "amount": float(row["amount"]),
                "price": float(row["price"]),
                "notional_value": float(row["notional_value"]),
                "anomaly_score": float(row.get("anomaly_score", -1)),
                "detection_timestamp": datetime.now().isoformat()
            }
            anomalies.append(anomaly_record)
        
        return json.dumps({
            "anomalies": anomalies,
            "count": len(anomalies),
            "batch_timestamp": datetime.now().isoformat()
        }, indent=2)
        
    except Exception as e:
        logging.error(f"Error formatting anomaly output: {e}")
        return json.dumps({"error": str(e), "timestamp": datetime.now().isoformat()})


def create_spark_session(config: Dict[str, Any]) -> SparkSession:
    """
    Create and configure Spark session for streaming.
    
    Args:
        config: Configuration dictionary
        
    Returns:
        Configured SparkSession
    """
    spark_config = config.get('spark', {})
    
    spark = SparkSession.builder \
        .appName(spark_config.get('app_name', 'FX-Anomaly-Detector')) \
        .master(spark_config.get('master', 'local[*]')) \
        .config("spark.sql.streaming.checkpointLocation", 
                spark_config.get('spark_sql_streaming_checkpoint_location', './checkpoints/sql')) \
        .config("spark.streaming.backpressure.enabled", 
                spark_config.get('spark_streaming_backpressure_enabled', True)) \
        .config("spark.streaming.receiver.writeAheadLog.enabled", 
                spark_config.get('spark_streaming_receiver_write_ack_enabled', True)) \
        .getOrCreate()
    
    # Set log level
    spark.sparkContext.setLogLevel("WARN")
    
    return spark 