"""
Required settings:
* Env:
    - KEY_VAULT_URL
* Secrets:
    - DB_USER
    - DB_PASSWORD
* Config:
    - DB_HOST
    - QUERYSET_DB_NAME
    - JOB_MANAGER_URL
    - LOG_LEVEL
"""
from datetime import date
from functools import lru_cache
import environs
from fitin import views_config

env = environs.Env()
env.read_env()

try:
    env("TESTING")
except environs.EnvError:
    config = views_config(env.str("KEY_VAULT_URL"))
else:
    config = lambda x: ""

config = lru_cache(maxsize=None)(config)

BASE_DATE = date(year=1979,month=12,day=1)
