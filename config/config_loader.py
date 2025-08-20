"""
Secure Configuration Loader for FX Anomaly Detection System

This module handles loading configuration from YAML files while securely
managing API keys and sensitive data through environment variables.
"""

import os
import yaml
import re
from typing import Dict, Any, Optional
from pathlib import Path


class SecureConfigLoader:
    """Secure configuration loader that handles environment variables."""
    
    def __init__(self, config_path: str = "config/config.yaml"):
        self.config_path = config_path
        self._config = None
    
    def load_config(self) -> Dict[str, Any]:
        """Load configuration from YAML file with environment variable substitution."""
        if self._config is not None:
            return self._config
        
        if not os.path.exists(self.config_path):
            raise FileNotFoundError(f"Configuration file not found: {self.config_path}")
        
        with open(self.config_path, 'r') as file:
            config_content = file.read()
        
        # Replace environment variable placeholders
        config_content = self._substitute_env_vars(config_content)
        
        # Load YAML
        self._config = yaml.safe_load(config_content)
        
        # Validate required environment variables
        self._validate_required_env_vars()
        
        return self._config
    
    def _substitute_env_vars(self, content: str) -> str:
        """Replace ${VAR_NAME} placeholders with actual environment variable values."""
        def replace_env_var(match):
            var_name = match.group(1)
            env_value = os.getenv(var_name)
            if env_value is None:
                raise ValueError(f"Required environment variable '{var_name}' is not set")
            return env_value
        
        # Replace ${VAR_NAME} patterns with environment variable values
        return re.sub(r'\$\{([^}]+)\}', replace_env_var, content)
    
    def _validate_required_env_vars(self):
        """Validate that all required environment variables are set."""
        required_vars = [
            'ALPHA_VANTAGE_API_KEY',
            'EXCHANGE_RATE_HOST_API_KEY'
        ]
        
        missing_vars = []
        for var in required_vars:
            if not os.getenv(var):
                missing_vars.append(var)
        
        if missing_vars:
            raise ValueError(
                f"Missing required environment variables: {', '.join(missing_vars)}\n"
                f"Please set these in your .env file or environment."
            )
    
    def get_api_keys(self) -> Dict[str, str]:
        """Get API keys from environment variables."""
        return {
            'alpha_vantage': os.getenv('ALPHA_VANTAGE_API_KEY'),
            'exchange_rate_host': os.getenv('EXCHANGE_RATE_HOST_API_KEY')
        }
    
    def get_database_config(self) -> Dict[str, Any]:
        """Get database configuration with environment variable substitution."""
        config = self.load_config()
        db_config = config.get('database', {})
        
        # Substitute environment variables in database config
        if 'postgresql' in db_config:
            postgres_config = db_config['postgresql']
            if isinstance(postgres_config.get('password'), str) and postgres_config['password'].startswith('${'):
                # Extract variable name from ${VAR_NAME} format
                var_name = postgres_config['password'][2:-1]  # Remove ${ and }
                postgres_config['password'] = os.getenv(var_name, 'postgres')
        
        return db_config


def load_config(config_path: str = "config/config.yaml") -> Dict[str, Any]:
    """Convenience function to load configuration."""
    loader = SecureConfigLoader(config_path)
    return loader.load_config()


def get_api_key(service: str) -> Optional[str]:
    """Get API key for a specific service."""
    env_var_map = {
        'alpha_vantage': 'ALPHA_VANTAGE_API_KEY',
        'exchange_rate_host': 'EXCHANGE_RATE_HOST_API_KEY'
    }
    
    env_var = env_var_map.get(service.lower())
    if not env_var:
        raise ValueError(f"Unknown service: {service}")
    
    return os.getenv(env_var)


def validate_api_keys() -> bool:
    """Validate that all required API keys are present."""
    try:
        loader = SecureConfigLoader()
        loader._validate_required_env_vars()
        return True
    except ValueError:
        return False


if __name__ == "__main__":
    # Test configuration loading
    try:
        config = load_config()
        print("✅ Configuration loaded successfully")
        print(f"Kafka servers: {config['kafka']['bootstrap_servers']}")
        print(f"Database type: {config['output']['database_type']}")
        
        # Check API keys (without exposing them)
        api_keys = get_api_key('alpha_vantage')
        if api_keys:
            print("✅ Alpha Vantage API key is set")
        else:
            print("❌ Alpha Vantage API key is missing")
            
    except Exception as e:
        print(f"❌ Configuration error: {e}") 