from dataclasses import dataclass
from functools import lru_cache
import os


@dataclass
class Settings:
    mysql_host: str
    mysql_port: int
    mysql_user: str
    mysql_password: str
    mysql_db: str
    mysql_pool_name: str
    mysql_pool_size: int
    enable_query_logging: bool


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Считывает настройки окружения один раз за запуск."""
    return Settings(
        mysql_host=os.getenv("MYSQL_HOST", "localhost"),
        mysql_port=int(os.getenv("MYSQL_PORT", "3306")),
        mysql_user=os.getenv("MYSQL_USER", "root"),
        mysql_password=os.getenv("MYSQL_PASSWORD", "DataAnalyst2025!"),
        mysql_db=os.getenv("MYSQL_DB", "tink1"),
        mysql_pool_name=os.getenv("MYSQL_POOL_NAME", "tourism_pool"),
        mysql_pool_size=int(os.getenv("MYSQL_POOL_SIZE", "6")),
        enable_query_logging=os.getenv("ENABLE_QUERY_LOGGING", "0") == "1",
    )

