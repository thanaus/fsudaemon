"""
Centralized configuration for S3 Event Processor
"""
from pathlib import Path
from typing import Optional
from pydantic import SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

# Project root (where config.py and .env live) so .env is loaded regardless of CWD
_PROJECT_ROOT = Path(__file__).resolve().parent


class Config(BaseSettings):
    """Application configuration loaded from .env"""

    # AWS Configuration
    aws_region: str
    aws_access_key_id: Optional[str] = None           # Optional if using IAM role
    aws_secret_access_key: Optional[SecretStr] = None  # Optional if using IAM role
    aws_endpoint_url: Optional[str] = None             # Optional, e.g. http://localhost:4566 for LocalStack
    sqs_queue_url: str

    # PostgreSQL Configuration
    db_host: str
    db_port: int
    db_name: str
    db_user: str
    db_password: SecretStr
    db_pool_min_size: int
    db_pool_max_size: int

    # Processing Configuration
    sqs_batch_size: int
    sqs_wait_time_seconds: int
    sqs_visibility_timeout: int

    # Logging Configuration
    log_level: str

    # OpenTelemetry OTLP (optional)
    otlp_endpoint: Optional[str] = None  # e.g. http://otel-collector:4317

    model_config = SettingsConfigDict(
        env_file=str(_PROJECT_ROOT / ".env"),
        env_file_encoding="utf-8",
        case_sensitive=False
    )

    @field_validator("sqs_batch_size")
    @classmethod
    def validate_sqs_batch_size(cls, v: int) -> int:
        if not 1 <= v <= 10:
            raise ValueError("sqs_batch_size must be between 1 and 10 (AWS SQS limit)")
        return v

    @field_validator("sqs_wait_time_seconds")
    @classmethod
    def validate_sqs_wait_time(cls, v: int) -> int:
        if not 0 <= v <= 20:
            raise ValueError("sqs_wait_time_seconds must be between 0 and 20")
        return v

    @field_validator("sqs_visibility_timeout")
    @classmethod
    def validate_sqs_visibility_timeout(cls, v: int) -> int:
        if not 0 <= v <= 43200:
            raise ValueError("sqs_visibility_timeout must be between 0 and 43200")
        return v

    @field_validator("log_level")
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        valid = ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")
        if v.upper() not in valid:
            raise ValueError(f"Invalid log_level: {v!r}. Must be one of {valid}")
        return v.upper()

    @field_validator("otlp_endpoint")
    @classmethod
    def validate_otlp_endpoint(cls, v: Optional[str]) -> Optional[str]:
        if v is not None and not (v.startswith("http://") or v.startswith("https://")):
            raise ValueError(f"otlp_endpoint must start with http:// or https://: {v!r}")
        return v

    def get_db_dsn(self) -> str:
        """Returns PostgreSQL DSN for asyncpg"""
        return (
            f"postgresql://{self.db_user}:{self.db_password.get_secret_value()}"
            f"@{self.db_host}:{self.db_port}/{self.db_name}"
        )


def load_config() -> Config:
    """Loads and returns a Config instance from environment variables"""
    return Config()
