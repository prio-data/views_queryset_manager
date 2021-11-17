
from typing import TypeVar
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from sqlalchemy.orm import Session
from toolz.functoolz import curry
from pymonad.either import Left, Right, Either
from pymonad.maybe import Maybe, Just, Nothing
from pymonad.promise import Promise
import views_schema
from . import models

T = TypeVar("T")

class Exists(Exception):
    pass

class DoesNotExist(Exception):
    pass

def get_queryset(session:Session,name:str) -> Promise:
    qs = session.query(models.Queryset).get(name)
    return Promise(lambda reject, resolve: resolve(qs) if qs is not None else reject(DoesNotExist))

def create_queryset(session:Session, posted: views_schema.Queryset) -> Either[Exception, models.Queryset]:
    try:
        queryset = models.Queryset.from_pydantic(session, posted)
    except ValueError as ve:
        return Left(ve)

    session.add(queryset)

    try:
        session.commit()
    except IntegrityError:
        return Left(Exists)
    except SQLAlchemyError as sqlerr:
        return Left(sqlerr)

    return Right(queryset)

def delete_queryset(session: Session, name: str) -> Maybe[str]:
    qs = session.query(models.Queryset).get(name)
    if qs is not None:
        session.delete(qs)
        session.commit()
        return Just(name)
    else:
        return Nothing

def get_or_create(kind: T, id_name: str, session: Session, identifier:str)-> T:
    o = session.query(kind).get(identifier)
    if o is None:
        o = kind(**{id_name: identifier})
        session.add(o)
    return o

get_or_create_theme = curry(get_or_create, models.Theme, "name")
