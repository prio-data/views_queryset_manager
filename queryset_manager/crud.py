
from sqlalchemy.orm import Session
from . import models,schema

def get_queryset(session:Session,name:str):
    return session.query(models.Queryset).get(name)

def create_queryset(session:Session, posted: schema.Queryset):
    operation_roots = []
    for chain in posted.operations:
        root = models.link_ops([models.Operation.from_pydantic(op) for op in chain])
        operation_roots.append(root)

    if posted.theme_name:
        theme = session.query(models.Theme).get(posted.theme_name)
        if theme is None:
            theme = models.Theme(name=posted.theme_name)
    else:
        theme = None

    queryset = models.Queryset(
            name = posted.name,
            loa = posted.loa,
            op_roots = operation_roots,
            theme = theme,
        )

    session.add(queryset)
    session.commit()

    return queryset
