from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """API configuration settings."""

    model_config = SettingsConfigDict(
        case_sensitive=False,
        extra="ignore",
    )

    # Metadata
    app_title: str = "Nuclear Outages API"
    app_version: str = "1.0.0"
    debug: bool = False
    docs_url: str = "/docs"
    redoc_url: str = "/redoc"
    openapi_url: str = "/openapi.json"
    cors_origins: list[str] = Field(default_factory=lambda: ["*"])

    # Database (PostgreSQL for parquet storage)
    # Set DATABASE_URL to use PostgreSQL backend, otherwise falls back to filesystem
    database_url: str | None = None

    # File paths
    storage_dir: str = "storage"
    plants_file: str = "storage/plants.parquet"
    facility_outages_file: str = "storage/facility_outages.parquet"
    us_outages_file: str = "storage/us_outages.parquet"
    metadata_file: str = "storage/metadata.json"

    # Pagination
    default_limit: int = 100
    max_limit: int = 1000

    valid_datasets: list[str] = ["facility", "us", "plants"]


settings = Settings()
