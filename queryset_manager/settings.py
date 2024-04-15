from typing import Optional
import environs

env = environs.Env()
env.read_env()

DB_HOST                    = env.str("QUERYSET_MANAGER_DB_HOST", "127.0.0.1")
DB_PORT                    = env.int("QUERYSET_MANAGER_DB_PORT", 5432)
DB_NAME                    = env.str("QUERYSET_MANAGER_DB_NAME", "fallback3")
DB_USER                    = env.str("QUERYSET_MANAGER_DB_USER", "views_data")
DB_SCHEMA: Optional[str]   = env.str("QUERYSET_MANAGER_DB_SCHEMA", None)
DB_PASSWORD: Optional[str] = env.str("QUERYSET_MANAGER_DB_PASSWORD", None)
DB_SSL                     = env.str("QUERYSET_MANAGER_DB_SSL", "allow")

LOG_LEVEL                  = env.str("LOG_LEVEL", "WARNING")

DATA_SERVICE_URL           = env.str("DATA_SERVICE_URL", "http://data-service")
