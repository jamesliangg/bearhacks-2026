from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # In local dev, infra/.env is the canonical env file. Keep .env as a fallback.
    model_config = SettingsConfigDict(env_file=("infra/.env", ".env"), extra="ignore")

    USE_SNOWFLAKE: bool = True
    LOCAL_SQLITE_PATH: str = "./data/via_delays.sqlite"

    # Optional: fetch Snowflake PAT from Auth0 (tenant-level secret managed outside this repo).
    AUTH0_DOMAIN: str | None = None
    AUTH0_CLIENT_ID: str | None = None
    AUTH0_CLIENT_SECRET: str | None = None
    AUTH0_AUDIENCE: str | None = None
    AUTH0_SNOWFLAKE_TOKEN_URL: str | None = None

    SNOWFLAKE_ACCOUNT: str | None = None
    SNOWFLAKE_USER: str | None = None
    SNOWFLAKE_PASSWORD: str | None = None
    SNOWFLAKE_TOKEN: str | None = None
    SNOWFLAKE_ROLE: str | None = None
    SNOWFLAKE_WAREHOUSE: str | None = None
    SNOWFLAKE_DATABASE: str = "VIA_DELAYS"
    SNOWFLAKE_SCHEMA_RAW: str = "RAW"
    SNOWFLAKE_SCHEMA_STAGING: str = "STAGING"
    SNOWFLAKE_SCHEMA_MART: str = "MART"
    SNOWFLAKE_MODEL_STAGE: str = "MODEL_STAGE"

    MODEL_DIR: str = "./models"
    ACTIVE_MODEL_FILE: str = "active.joblib"
    ACTIVE_MODEL_ID: str | None = None

    SCRAPE_USER_AGENT: str = "via-delay-oracle/0.1"
    TRANSITDOCS_BASE: str = "https://asm.transitdocs.com"
    VIA_LIVE_BASE: str = "https://tsimobile.viarail.ca"


settings = Settings()
