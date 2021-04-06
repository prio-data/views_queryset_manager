"""
Required settings:
* Env:
    - KEY_VAULT_URL
* Secrets:
    - DB_USER
    - DB_PASSWORD
* Config:
    - DB_HOST
    - JOB_MANAGER_URL
    - QUERYSET_DB_SCHEMA
    - LOG_LEVEL
"""
import environs
from datetime import date
from fitin import views_config

env = environs.Env()
env.read_env()

try:
    env("TESTING")
except environs.EnvError:
    config = views_config(env.str("KEY_VAULT_URL"))
else:
    config = lambda x: ""

BASE_DATE = date(year=1979,month=12,day=1)
