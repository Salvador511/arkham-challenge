from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Configuración de la API"""
    # Metadata
    app_title: str = "Nuclear Outages API"
    app_version: str = "1.0.0"
    debug: bool = False

    # Rutas de archivos
    storage_dir: str = "storage"
    plants_file: str = "storage/plants.parquet"
    facility_outages_file: str = "storage/facility_outages.parquet"
    us_outages_file: str = "storage/us_outages.parquet"
    metadata_file: str = "storage/metadata.json"


    # Paginación
    default_limit: int = 100
    max_limit: int = 1000

    valid_datasets: list = ["facility", "us"]

    class Config:
        case_sensitive = False


# Instancia global que se usa en toda la app
settings = Settings()
