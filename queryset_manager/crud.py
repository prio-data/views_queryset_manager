
from typing import TypeVar
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from toolz.functoolz import curry
import views_schema

from . import models

class Exists(Exception):
    pass

class DoesNotExist(Exception):
    pass

def get_queryset(session:Session,name:str) -> models.Queryset:
    return session.query(models.Queryset).get(name)

def create_queryset(session:Session, posted: views_schema.Queryset) -> models.Queryset:
    queryset = models.Queryset.from_pydantic(session, posted)
    session.add(queryset)

    try:
        session.commit()
    except IntegrityError:
        raise Exists

    return queryset

def delete_queryset(session: Session, name: str) -> None:
    qs = session.query(models.Queryset).get(name)
    if qs is not None:
        session.delete(qs)
        session.commit()
    else:
        raise DoesNotExist(f"Queryset {name} does not exist")

ModelType = TypeVar("ModelType")
def get_or_create(kind: ModelType, id_name: str, session: Session, identifier:str)-> ModelType:
    o = session.query(kind).get(identifier)
    if o is None:
        o = kind(**{id_name: identifier})
        session.add(o)
    return o

get_or_create_theme = curry(get_or_create, models.Theme, "name")
