"""
FX Anomaly Detection Model Training Script.

This script generates synthetic FX trade data, performs exploratory data analysis,
trains an Isolation Forest model for anomaly detection, and saves the model for
production use.
"""

import os
import sys
import json
import yaml
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
from typing import Dict, Any, List, Tuple
import joblib
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, confusion_matrix
import warnings
warnings.filterwarnings('ignore')

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from consumer.utils import load_config, setup_logging


class FXAnomalyModelTrainer:
    """Trainer class for FX anomaly detection model."""
    
    def __init__(self, config_path: str = "config/config.yaml"):
        """
        Initialize the model trainer.
        
        Args:
            config_path: Path to configuration file
        """
        self.config = load_config(config_path)
        self.logger = setup_logging(self.config)
        self.model_dir = os.path.dirname(os.path.abspath(__file__))
        
        # Create model directory if it doesn't exist
        os.makedirs(self.model_dir, exist_ok=True)
        
        # Model artifacts
        self.model = None
        self.scaler = None
        self.feature_columns = None
        
    def generate_synthetic_data(self, num_records: int = 10000) -> pd.DataFrame:
        """
        Generate synthetic FX trade data with anomalies.
        
        Args:
            num_records: Number of records to generate
            
        Returns:
            DataFrame with synthetic trade data
        """
        self.logger.info(f"Generating {num_records} synthetic FX trade records")
        
        data_config = self.config.get('data', {})
        currency_pairs = data_config.get('currency_pairs', ['EUR/USD', 'GBP/USD', 'USD/JPY'])
        price_ranges = data_config.get('price_ranges', {})
        amount_range = data_config.get('amount_range', [1000, 1000000])
        anomaly_rate = data_config.get('anomaly_rate', 0.05)
        
        # Generate base data
        np.random.seed(42)
        
        # Generate timestamps
        start_time = datetime.now() - timedelta(days=30)
        timestamps = [start_time + timedelta(seconds=i*60) for i in range(num_records)]
        
        # Generate trade data
        trades = []
        for i in range(num_records):
            # Select random currency pair
            currency_pair = np.random.choice(currency_pairs)
            
            # Get price range for this pair
            if currency_pair in price_ranges:
                min_price, max_price = price_ranges[currency_pair]
            else:
                min_price, max_price = 1.0, 2.0
            
            # Generate normal trade data
            amount = np.random.uniform(amount_range[0], amount_range[1])
            price = np.random.uniform(min_price, max_price)
            
            # Determine if this should be an anomaly
            is_anomaly = np.random.random() < anomaly_rate
            
            if is_anomaly:
                # Inject different types of anomalies
                anomaly_type = np.random.choice(['price_spike', 'amount_spike', 'unusual_pair'])
                
                if anomaly_type == 'price_spike':
                    # Extreme price deviation
                    price = price * np.random.uniform(2.0, 5.0)
                elif anomaly_type == 'amount_spike':
                    # Extreme amount
                    amount = amount * np.random.uniform(10.0, 50.0)
                else:  # unusual_pair
                    # Use unusual currency pair
                    currency_pair = 'XXX/YYY'
                    price = np.random.uniform(0.1, 10.0)
            
            trade = {
                'trade_id': f"TRADE_{i:06d}",
                'timestamp': timestamps[i],
                'currency_pair': currency_pair,
                'amount': amount,
                'price': price,
                'buyer_id': f"BUYER_{np.random.randint(1, 100):03d}",
                'seller_id': f"SELLER_{np.random.randint(1, 100):03d}",
                'notional_value': amount * price,
                'is_anomaly': is_anomaly
            }
            trades.append(trade)
        
        df = pd.DataFrame(trades)
        self.logger.info(f"Generated {len(df)} trades with {df['is_anomaly'].sum()} anomalies")
        
        return df
    
    def perform_eda(self, df: pd.DataFrame) -> None:
        """
        Perform exploratory data analysis on the generated data.
        
        Args:
            df: DataFrame with trade data
        """
        self.logger.info("Performing exploratory data analysis")
        
        # Create EDA directory
        eda_dir = os.path.join(self.model_dir, 'eda')
        os.makedirs(eda_dir, exist_ok=True)
        
        # Basic statistics
        print("\n=== Basic Statistics ===")
        print(df.describe())
        
        # Anomaly distribution
        print(f"\n=== Anomaly Distribution ===")
        print(f"Total records: {len(df)}")
        print(f"Anomalies: {df['is_anomaly'].sum()}")
        print(f"Anomaly rate: {df['is_anomaly'].mean():.2%}")
        
        # Currency pair distribution
        print(f"\n=== Currency Pair Distribution ===")
        print(df['currency_pair'].value_counts())
        
        # Create visualizations
        plt.figure(figsize=(15, 10))
        
        # 1. Amount distribution
        plt.subplot(2, 3, 1)
        plt.hist(df['amount'], bins=50, alpha=0.7)
        plt.title('Trade Amount Distribution')
        plt.xlabel('Amount')
        plt.ylabel('Frequency')
        
        # 2. Price distribution
        plt.subplot(2, 3, 2)
        plt.hist(df['price'], bins=50, alpha=0.7)
        plt.title('Price Distribution')
        plt.xlabel('Price')
        plt.ylabel('Frequency')
        
        # 3. Notional value distribution
        plt.subplot(2, 3, 3)
        plt.hist(df['notional_value'], bins=50, alpha=0.7)
        plt.title('Notional Value Distribution')
        plt.xlabel('Notional Value')
        plt.ylabel('Frequency')
        
        # 4. Currency pair distribution
        plt.subplot(2, 3, 4)
        df['currency_pair'].value_counts().plot(kind='bar')
        plt.title('Currency Pair Distribution')
        plt.xlabel('Currency Pair')
        plt.ylabel('Count')
        plt.xticks(rotation=45)
        
        # 5. Anomaly vs Normal - Amount
        plt.subplot(2, 3, 5)
        normal_amounts = df[~df['is_anomaly']]['amount']
        anomaly_amounts = df[df['is_anomaly']]['amount']
        plt.hist(normal_amounts, bins=30, alpha=0.7, label='Normal', color='blue')
        plt.hist(anomaly_amounts, bins=30, alpha=0.7, label='Anomaly', color='red')
        plt.title('Amount Distribution: Normal vs Anomaly')
        plt.xlabel('Amount')
        plt.ylabel('Frequency')
        plt.legend()
        
        # 6. Anomaly vs Normal - Price
        plt.subplot(2, 3, 6)
        normal_prices = df[~df['is_anomaly']]['price']
        anomaly_prices = df[df['is_anomaly']]['price']
        plt.hist(normal_prices, bins=30, alpha=0.7, label='Normal', color='blue')
        plt.hist(anomaly_prices, bins=30, alpha=0.7, label='Anomaly', color='red')
        plt.title('Price Distribution: Normal vs Anomaly')
        plt.xlabel('Price')
        plt.ylabel('Frequency')
        plt.legend()
        
        plt.tight_layout()
        plt.savefig(os.path.join(eda_dir, 'eda_plots.png'), dpi=300, bbox_inches='tight')
        plt.close()
        
        # Save EDA summary
        eda_summary = {
            'total_records': len(df),
            'anomaly_count': int(df['is_anomaly'].sum()),
            'anomaly_rate': float(df['is_anomaly'].mean()),
            'currency_pairs': df['currency_pair'].value_counts().to_dict(),
            'amount_stats': df['amount'].describe().to_dict(),
            'price_stats': df['price'].describe().to_dict(),
            'notional_value_stats': df['notional_value'].describe().to_dict()
        }
        
        with open(os.path.join(eda_dir, 'eda_summary.json'), 'w') as f:
            json.dump(eda_summary, f, indent=2, default=str)
        
        self.logger.info(f"EDA completed. Results saved to {eda_dir}")
    
    def prepare_features(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
        """
        Prepare features for model training.
        
        Args:
            df: DataFrame with trade data
            
        Returns:
            Tuple of (feature DataFrame, feature column names)
        """
        self.logger.info("Preparing features for model training")
        
        # Create feature DataFrame
        feature_df = df.copy()
        
        # Encode currency pairs
        feature_df['currency_pair_encoded'] = feature_df['currency_pair'].astype('category').cat.codes
        
        # Extract time-based features
        feature_df['hour'] = feature_df['timestamp'].dt.hour
        feature_df['day_of_week'] = feature_df['timestamp'].dt.dayofweek
        feature_df['month'] = feature_df['timestamp'].dt.month
        
        # Create derived features
        feature_df['amount_log'] = np.log1p(feature_df['amount'])
        feature_df['price_log'] = np.log1p(feature_df['price'])
        feature_df['notional_value_log'] = np.log1p(feature_df['notional_value'])
        
        # Define feature columns
        feature_columns = [
            'amount', 'price', 'notional_value',
            'currency_pair_encoded', 'hour', 'day_of_week', 'month',
            'amount_log', 'price_log', 'notional_value_log'
        ]
        
        self.feature_columns = feature_columns
        
        return feature_df[feature_columns], feature_columns
    
    def train_model(self, X: pd.DataFrame) -> None:
        """
        Train the Isolation Forest model.
        
        Args:
            X: Feature DataFrame
        """
        self.logger.info("Training Isolation Forest model")
        
        model_config = self.config.get('model', {})
        
        # Initialize and train model
        self.model = IsolationForest(
            contamination=model_config.get('contamination', 0.05),
            random_state=model_config.get('random_state', 42),
            n_estimators=model_config.get('n_estimators', 100),
            max_samples=model_config.get('max_samples', 'auto')
        )
        
        # Fit the model
        self.model.fit(X)
        
        self.logger.info("Model training completed")
    
    def evaluate_model(self, X: pd.DataFrame, y: pd.Series) -> Dict[str, Any]:
        """
        Evaluate the trained model.
        
        Args:
            X: Feature DataFrame
            y: True anomaly labels
            
        Returns:
            Dictionary with evaluation metrics
        """
        self.logger.info("Evaluating model performance")
        
        # Get predictions
        predictions = self.model.predict(X)
        scores = self.model.decision_function(X)
        
        # Convert predictions to binary (1 for anomaly, 0 for normal)
        # Isolation Forest returns -1 for anomalies, 1 for normal
        y_pred = (predictions == -1).astype(int)
        y_true = y.astype(int)
        
        # Calculate metrics
        from sklearn.metrics import precision_score, recall_score, f1_score, accuracy_score
        
        metrics = {
            'accuracy': accuracy_score(y_true, y_pred),
            'precision': precision_score(y_true, y_pred, zero_division=0),
            'recall': recall_score(y_true, y_pred, zero_division=0),
            'f1_score': f1_score(y_true, y_pred, zero_division=0),
            'anomaly_detection_rate': y_pred.mean(),
            'true_anomaly_rate': y_true.mean()
        }
        
        # Print results
        print("\n=== Model Evaluation Results ===")
        for metric, value in metrics.items():
            print(f"{metric}: {value:.4f}")
        
        # Create confusion matrix
        cm = confusion_matrix(y_true, y_pred)
        print(f"\nConfusion Matrix:")
        print(cm)
        
        self.logger.info(f"Model evaluation completed. F1 Score: {metrics['f1_score']:.4f}")
        
        return metrics
    
    def save_model(self) -> None:
        """Save the trained model and artifacts."""
        self.logger.info("Saving model and artifacts")
        
        model_path = self.config.get('model', {}).get('path', './model/fx_anomaly_model.pkl')
        
        # Save model
        joblib.dump(self.model, model_path)
        
        # Save scaler (if used)
        if self.scaler:
            scaler_path = model_path.replace('.pkl', '_scaler.pkl')
            joblib.dump(self.scaler, scaler_path)
        
        # Save feature information
        model_info = {
            'feature_columns': self.feature_columns,
            'model_type': 'IsolationForest',
            'training_timestamp': datetime.now().isoformat(),
            'config': self.config.get('model', {})
        }
        
        info_path = model_path.replace('.pkl', '_info.json')
        with open(info_path, 'w') as f:
            json.dump(model_info, f, indent=2)
        
        self.logger.info(f"Model saved to {model_path}")
    
    def run_training_pipeline(self, num_records: int = 10000) -> None:
        """
        Run the complete training pipeline.
        
        Args:
            num_records: Number of synthetic records to generate
        """
        self.logger.info("Starting FX anomaly detection model training pipeline")
        
        try:
            # 1. Generate synthetic data
            df = self.generate_synthetic_data(num_records)
            
            # 2. Perform EDA
            self.perform_eda(df)
            
            # 3. Prepare features
            feature_df, feature_columns = self.prepare_features(df)
            
            # 4. Split data for evaluation
            X_train, X_test, y_train, y_test = train_test_split(
                feature_df, df['is_anomaly'], 
                test_size=0.2, random_state=42, stratify=df['is_anomaly']
            )
            
            # 5. Train model
            self.train_model(X_train)
            
            # 6. Evaluate model
            metrics = self.evaluate_model(X_test, y_test)
            
            # 7. Save model
            self.save_model()
            
            self.logger.info("Training pipeline completed successfully")
            
        except Exception as e:
            self.logger.error(f"Error in training pipeline: {e}")
            raise


def main():
    """Main function to run the training pipeline."""
    trainer = FXAnomalyModelTrainer(config_path="config/config.yaml")
    trainer.run_training_pipeline(num_records=10000)


if __name__ == "__main__":
    main() 