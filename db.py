
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models import Base

engine = create_engine("sqlite:///db.sqlite")
Session = sessionmaker(engine)
Base.metadata.create_all(engine)
