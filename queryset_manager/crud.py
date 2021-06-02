
from sqlalchemy.orm import Session
from toolz.functoolz import curry
import views_schema
from . import models
from sqlalchemy.exc import IntegrityError

class Exists(Exception):
    pass

def get_queryset(session:Session,name:str):
    return session.query(models.Queryset).get(name)

def create_queryset(session:Session, posted: views_schema.Queryset):
    queryset = models.Queryset.from_pydantic(session, posted)
    session.add(queryset)
    session.commit()
    return queryset

def get_or_create(kind, id_name, session, identifier):
    o = session.query(kind).get(identifier)
    if o is None:
        o = kind(**{id_name: identifier})
        session.add(o)
    return o

get_or_create_theme = curry(get_or_create, models.Theme, "name")
