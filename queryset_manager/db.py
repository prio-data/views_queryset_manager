
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import settings

def get_con():
    connection_parameters = [
            f"host={settings.DB_HOST}",
            f"port={settings.DB_PORT}",
            f"user={settings.DB_USER}",
            f"dbname={settings.DB_NAME}",
            f"sslmode={settings.DB_SSL}",
            ]

    if settings.DB_PASSWORD:
        connection_parameters.append(f"password={settings.DB_PASSWORD}")

    return psycopg2.connect(" ".join(connection_parameters))

engine = create_engine("postgresql+psycopg2://", creator = get_con)

Session = sessionmaker(engine)
