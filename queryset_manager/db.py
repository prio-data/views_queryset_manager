
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from postgres_azure_certificate_auth import sec_con
from .settings import config 

sec = {
        "sslcert": config("SSL_CERT"),
        "sslkey": config("SSL_KEY"),
        "sslrootcert": config("SSL_ROOT_CERT"),
    }
con = {
        "host": config("DB_HOST"),
        "user": config("DB_USER"),
    }

def get_con():
    return sec_con(sec,con,dbname=config("QUERYSET_DB_NAME"))

engine = create_engine("postgresql://",creator=get_con)
Session = sessionmaker(engine)
