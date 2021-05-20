
from sqlalchemy.orm import Session
from toolz.functoolz import curry
from . import models,schema

def get_queryset(session:Session,name:str):
    return session.query(models.Queryset).get(name)

def create_queryset(session:Session, posted: schema.Queryset):
    operation_roots = []
    for chain in posted.operations:
        root = models.link_ops([models.Operation.from_pydantic(op) for op in chain])
        operation_roots.append(root)

    themes = []
    if posted.themes:
        for theme_name in posted.themes:
            theme = session.query(models.Theme).get(theme_name)
            if theme is None:
                theme = models.Theme(name=theme_name)
            themes.append(theme)

    queryset = models.Queryset(
            name = posted.name,
            loa = posted.loa,
            op_roots = operation_roots,
            themes = themes,
        )

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
