from typing import Dict, Any
from pydantic import BaseSettings

class Settings(BaseSettings):
    # Storage configs
    prefix_protocol: str = 's3a'
    # TODO: add ENV
    raw: str = 'datalake-raw'
    staged: str = 'datalake-staged'
    curated: str = 'datalake-curated'
    spark_control: str = 'spark-control'

    checkpoint_paths: str = f"{prefix_protocol}://{spark_control}/checkpoints"
    checkpoint_raw: str = f"{checkpoint_paths}/raw"
    checkpoint_staged: str = f"{checkpoint_paths}/staged"
    checkpoint_curated: str = f"{checkpoint_paths}/curated"

    schemaRegistry: Dict[str, Dict[str, Any]] = {
        'local': {
            'url': 'http://localhost:8081'
        },
        'example': {
            'url': 'http://schema-registry:8081'
        }
    }

    # Kafka configurations
    kafka_settings: Dict[str, Dict[str, Any]] = {
        'local': {
            'bootstrap_servers': 'localhost:9092',
            'tls': False
        
        },
        'example': {
            'bootstrap_servers': 'broker:29092',
            'tls': False
        }
    }

    # Delta Lake confis
    max_num_files_allowed: int = 10000

settings = Settings()
