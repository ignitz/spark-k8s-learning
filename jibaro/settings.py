from typing import Dict, Any
from pydantic import BaseSettings

class Settings(BaseSettings):
    prefix_protocol: str = 's3a'
    raw: str = 'datalake-raw'
    staged: str = 'datalake-staged'
    curated: str = 'datalake-curated'
    spark_control: str = 'spark-control'

    kafka_settings: Dict[str, Any] = {
        'bootstrap_servers': 'broker:29092',
    }
