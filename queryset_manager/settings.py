"""
Required settings:
    - KEY_VAULT_URL
    - DB_HOST
    - DB_PORT
    - DB_NAME
    - DB_USER
    - DB_SCHEMA
    - LOG_LEVEL
"""
import environs


env = environs.Env()
env.read_env()
config = env.str

JOB_MANAGER_URL = env.str("JOB_MANAGER_URL")
