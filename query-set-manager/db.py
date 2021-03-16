
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models import Base
import settings

CONNECTION_STRING=("postgresql+psycopg2://"
        f"{settings.DB_USER}:{settings.DB_PASSWORD}"
        f"@{settings.DB_HOST}/{settings.DB_NAME}?sslmode=require"
    )

engine = create_engine(
        CONNECTION_STRING
        )

Session = sessionmaker(engine)
Base.metadata.create_all(engine)
