
import os
from datetime import date

import requests
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.appconfiguration import AzureAppConfigurationClient
from environs import Env
env = Env()
env.read_env()

BASE_DATE = date(year=1979,month=12,day=1)

PROD = env.bool("PRODUCTION",True)

# SECRETS ================================================

if PROD:
    secret_client = SecretClient(env.str("KEY_VAULT_URL"),DefaultAzureCredential())
    get_secret = lambda k: secret_client.get_secret(k).value
    config_client = AzureAppConfigurationClient.from_connection_string(
                get_secret("appconfig-connection-string")
            )
    get_config = lambda k: config_client.get_configuration_setting(k).value
else:
    REST_ENV_URL = env.str("REST_ENV_URL")
    get_secret = lambda k: requests.get(os.path.join(REST_ENV_URL,k)).content.decode()
    get_config = get_secret

DB_USER=get_secret("db-user")
DB_PASSWORD=get_secret("db-password")

# CONFIGURATION ==========================================

SOURCE_URL = get_config("job-manager-url")
DB_NAME = get_config("base-db-name")
DB_SCHEMA = get_config("queryset-schema")
DB_HOST = get_config("db-host")
LOG_LEVEL = get_config("log-level")
