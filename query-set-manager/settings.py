
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.appconfiguration import AzureAppConfigurationClient
from environs import Env
env = Env()
env.read_env()

# SECRETS ================================================

if env.str("PRODUCTION",True):
    secret_client = SecretClient(env.str("KEY_VAULT_URL"),DefaultAzureCredential())
    get_secret = lambda k: secret_client.get_secret(k).value

DB_USER=get_secret("db-user")
DB_PASSWORD=get_secret("db-password")

# CONFIGURATION ==========================================

config_client = AzureAppConfigurationClient.from_connection_string(
            get_secret("appconfig-connection-string")
        )
get_config = lambda k: config_client.get_configuration_setting(k).value

ROUTER_URL = get_config("data-router-url")
DB_NAME = get_config("base-db-name")
DB_SCHEMA = get_config("queryset-schema")
DB_HOST = get_config("db-host")
