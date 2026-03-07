"""
Centralized configuration for S3 Event Processor
"""
from functools import lru_cache
from pathlib import Path
from urllib.parse import quote_plus
from pydantic import SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

# Project root (where config.py and .env live) so .env is loaded regardless of CWD
_PROJECT_ROOT = Path(__file__).resolve().parent


class Config(BaseSettings):
    """Application configuration loaded from .env"""

    # AWS Configuration
    aws_region: str
    aws_access_key_id: SecretStr | None = None
    aws_secret_access_key: SecretStr | None = None
    aws_endpoint_url: str | None = None
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
    otlp_endpoint: str | None = None

    model_config = SettingsConfigDict(
        env_file=str(_PROJECT_ROOT / ".env"),
        env_file_encoding="utf-8",
        case_sensitive=False,
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

    @field_validator("otlp_endpoint", "aws_endpoint_url")
    @classmethod
    def validate_http_endpoint(cls, v: str | None) -> str | None:
        if v is not None and not (v.startswith("http://") or v.startswith("https://")):
            raise ValueError(f"Endpoint must start with http:// or https://: {v!r}")
        return v

    @model_validator(mode="after")
    def validate_pool_sizes(self) -> "Config":
        if self.db_pool_min_size > self.db_pool_max_size:
            raise ValueError(
                f"db_pool_min_size ({self.db_pool_min_size}) "
                f"must be <= db_pool_max_size ({self.db_pool_max_size})"
            )
        return self

    def get_db_dsn(self) -> str:
        """Returns PostgreSQL DSN for asyncpg."""
        user = quote_plus(self.db_user)
        password = quote_plus(self.db_password.get_secret_value())
        return f"postgresql://{user}:{password}@{self.db_host}:{self.db_port}/{self.db_name}"


@lru_cache(maxsize=1)
def load_config() -> Config:
    """Loads and returns a Config instance from environment variables.
    Result is cached — subsequent calls return the same instance."""
    return Config()
