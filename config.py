"""
Centralized configuration for S3 Event Processor
"""
from pathlib import Path
from typing import Optional
from pydantic_settings import BaseSettings, SettingsConfigDict

# Project root (where config.py and .env live) so .env is loaded regardless of CWD
_PROJECT_ROOT = Path(__file__).resolve().parent


class Config(BaseSettings):
    """Application configuration loaded from .env"""
    
    # AWS Configuration
    aws_region: str
    aws_access_key_id: Optional[str] = None  # Optional if using IAM role
    aws_secret_access_key: Optional[str] = None  # Optional if using IAM role
    aws_endpoint_url: Optional[str] = None  # Optional, e.g. http://localhost:4566 for LocalStack
    sqs_queue_url: str
    
    # PostgreSQL Configuration
    db_host: str
    db_port: int
    db_name: str
    db_user: str
    db_password: str
    db_pool_min_size: int
    db_pool_max_size: int
    
    # Processing Configuration
    sqs_batch_size: int
    sqs_wait_time_seconds: int
    sqs_visibility_timeout: int
    
    # Logging Configuration
    log_level: str
    log_format: str
    
    model_config = SettingsConfigDict(
        env_file=str(_PROJECT_ROOT / ".env"),
        env_file_encoding="utf-8",
        case_sensitive=False
    )
    
    def get_db_dsn(self) -> str:
        """Returns PostgreSQL DSN for asyncpg"""
        return f"postgresql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"


# Global configuration instance
config: Optional[Config] = None


def load_config() -> Config:
    """Loads configuration from environment variables"""
    global config
    if config is None:
        config = Config()
    return config


def get_config() -> Config:
    """Returns loaded configuration"""
    if config is None:
        raise RuntimeError("Configuration not loaded. Call load_config() first.")
    return config
