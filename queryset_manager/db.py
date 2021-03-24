
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from . import settings

"""
CONNECTION_STRING=("postgresql+psycopg2://"
        f"{settings.DB_USER}:{settings.DB_PASSWORD}"
        f"@{settings.DB_HOST}/{settings.DB_NAME}?sslmode=require"
    )
"""

CONNECTION_STRING = "sqlite:///db.sqlite"

engine = create_engine(
        CONNECTION_STRING
        )

Session = sessionmaker(engine)
