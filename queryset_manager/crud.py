
from sqlalchemy.orm import Session
from . import models

def get_queryset(session:Session,name:str):
    return session.query(models.Queryset).get(name)
