"""Configuration primitives for the FastAPI backend."""

from functools import lru_cache
from typing import Annotated

from pydantic import AliasChoices, Field, field_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict
from sqlalchemy.engine import URL


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    app_name: str = Field(default="OSINT Platform API", alias="APP_NAME")
    app_version: str = Field(default="0.1.0", alias="APP_VERSION")
    api_prefix: str = Field(default="/api", alias="API_PREFIX")
    debug: bool = Field(default=False, validation_alias=AliasChoices("DEBUG", "DJANGO_DEBUG"))
    cors_allowed_origins: Annotated[list[str], NoDecode] = Field(
        default_factory=list,
        validation_alias=AliasChoices("CORS_ALLOWED_ORIGINS"),
    )
    cors_allow_credentials: bool = Field(default=True, alias="CORS_ALLOW_CREDENTIALS")

    postgres_host: str = Field(default="postgres", alias="POSTGRES_HOST")
    postgres_port: int = Field(default=5432, alias="POSTGRES_PORT")
    postgres_db: str = Field(default="osint_db", alias="POSTGRES_DB")
    postgres_user: str = Field(default="postgres", alias="POSTGRES_USER")
    postgres_password: str = Field(default="postgres", alias="POSTGRES_PASSWORD")
    postgres_pool_size: int = Field(default=10, alias="POSTGRES_POOL_SIZE")
    postgres_max_overflow: int = Field(default=20, alias="POSTGRES_MAX_OVERFLOW")
    postgres_pool_timeout: int = Field(default=30, alias="POSTGRES_POOL_TIMEOUT")
    postgres_pool_recycle: int = Field(default=1800, alias="POSTGRES_POOL_RECYCLE")

    neo4j_uri: str = Field(default="bolt://neo4j:7687", alias="NEO4J_URI")
    neo4j_user: str = Field(default="neo4j", alias="NEO4J_USER")
    neo4j_password: str = Field(default="neo4j", alias="NEO4J_PASSWORD")
    neo4j_database: str = Field(default="neo4j", alias="NEO4J_DATABASE")
    neo4j_max_connection_lifetime: int = Field(default=3600, alias="NEO4J_MAX_CONNECTION_LIFETIME")
    neo4j_max_connection_pool_size: int = Field(default=100, alias="NEO4J_MAX_CONNECTION_POOL_SIZE")
    neo4j_connection_timeout: int = Field(default=30, alias="NEO4J_CONNECTION_TIMEOUT")

    neo4j_max_network_depth: int = Field(default=3, alias="NEO4J_MAX_NETWORK_DEPTH")
    neo4j_max_network_edges: int = Field(default=200, alias="NEO4J_MAX_NETWORK_EDGES")

    @field_validator("cors_allowed_origins", mode="before")
    @classmethod
    def parse_csv_list(cls, value: object) -> list[str]:
        if value is None or value == "":
            return []
        if isinstance(value, str):
            return [item.strip() for item in value.split(",") if item.strip()]
        if isinstance(value, (list, tuple, set)):
            return [str(item).strip() for item in value if str(item).strip()]
        raise TypeError("Unsupported CORS origin format")

    @property
    def postgres_dsn(self) -> str:
        return URL.create(
            drivername="postgresql+asyncpg",
            username=self.postgres_user,
            password=self.postgres_password,
            host=self.postgres_host,
            port=self.postgres_port,
            database=self.postgres_db,
        ).render_as_string(hide_password=False)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
