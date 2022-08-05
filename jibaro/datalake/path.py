
from jibaro.settings import settings


def mount_path(
    layer: str, project_name: str, database: str, table_name: str,
) -> str:
    bucket = {
        "raw": settings.raw,
        "staged": settings.staged,
        "curated": settings.curated
    }[layer]
    prefix = f"{settings.prefix_protocol}://{bucket}"

    path: str = f"{prefix}/{project_name}/{database}/{table_name}"
    return path
