from typing import Optional
from jibaro.settings import settings


def mount_path(
    layer: Optional[str] = None, project_name: Optional[str] = None, database: Optional[str] = None, table_name: Optional[str] = None,
) -> str:
    if (
        layer is None or
        project_name is None or
        database is None or
        table_name is None
    ):
        return None
    bucket = {
        "raw": settings.raw,
        "staged": settings.staged,
        "curated": settings.curated
    }[layer]
    prefix = f"{settings.prefix_protocol}://{bucket}"

    path: str = f"{prefix}/{project_name}/{database}/{table_name}"
    return path


def mount_checkpoint_path(
    layer: Optional[str] = None, project_name: Optional[str] = None, database: Optional[str] = None, table_name: Optional[str] = None,
) -> str:
    if (
        layer is None or
        project_name is None or
        database is None or
        table_name is None
    ):
        return None
    bucket = {
        "raw": settings.raw,
        "staged": settings.staged,
        "curated": settings.curated
    }[layer]
    prefix = f"{settings.prefix_protocol}://{settings.spark_control}"

    path: str = f"{prefix}/{bucket}/{project_name}/{database}/{table_name}"

    return path
