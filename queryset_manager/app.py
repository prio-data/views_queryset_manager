import os
import logging
from typing import Optional
import io
from datetime import date

from fastapi import Response,Depends
import fastapi
from requests import HTTPError

from . import crud,models,schema,db,remotes,settings

logger = logging.getLogger("azure.core.pipeline.policies.http_logging_policy")
logger.setLevel(logging.WARNING)

try:
    logging.basicConfig(level=getattr(logging,settings.config("LOG_LEVEL")))
except AttributeError:
    pass

models.Base.metadata.create_all(db.engine)
app = fastapi.FastAPI()

def hyperlink(r:fastapi.Request,*rest):
    url = r.url
    base = f"{url.scheme}://{url.hostname}:{url.port}"
    return os.path.join(base,*rest)

def get_session():
    sess = db.Session()
    try:
        yield sess
    finally:
        sess.close()

@app.get("/data/{queryset_name}/")
def queryset_data(queryset_name:str,
        start_date:Optional[date]=None, end_date:Optional[date]=None,
        session = Depends(get_session)):
    """
    Retrieve data corresponding to a queryset
    """
    queryset = crud.get_queryset(session,queryset_name)

    if queryset is None:
        return Response(status_code=404)

    try:
        data = remotes.fetch_data_for_queryset(queryset,start_date,end_date)
    except remotes.OperationPending:
        return Response(status_code=202)
    except HTTPError as httpe:
        return Response(
                f"Proxied {httpe.response.content}",
                status_code=httpe.response.status_code
            )

    bytes_buffer = io.BytesIO()
    data.to_parquet(bytes_buffer,compression="gzip")
    return Response(bytes_buffer.getvalue(),media_type="application/octet-stream")

@app.get("/queryset/{queryset}/")
def queryset_detail(queryset:str, session = Depends(get_session)):
    """
    Get details about a queryset
    """

    queryset = session.query(models.Queryset).get(queryset)
    if queryset is None:
        return fastapi.Response(status_code=404)
    return {
        "level_of_analysis": queryset.loa.value,
        "theme": queryset.theme.name if queryset.theme else None,
        "op_paths": [op.get_path() for op in queryset.op_roots],
    }

@app.get("/queryset/")
def queryset_list(r:fastapi.Request, session = Depends(get_session)):
    """
    Lists all current querysets
    """
    querysets = session.query(models.Queryset).all()
    links = [hyperlink(r,qs.path()) for qs in querysets]
    return Response(str(links))

@app.post("/queryset/")
def queryset_create(
        r:fastapi.Request,
        posted: schema.QuerysetPost,
        session = Depends(get_session)):
    """
    Creates a new queryset
    """
    queryset = crud.create_queryset(session,posted)
    return Response(hyperlink(r,queryset.path()),status_code=201)

@app.put("/queryset/{queryset}/")
def queryset_replace(
        r:fastapi.Request,
        queryset:str, new: schema.QuerysetPut,
        session = Depends(get_session)):
    """
    Replaces the queryset with the posted queryset
    """

    existing_qs = session.query(models.Queryset).get(queryset)
    if existing_qs is None:
        pass
    else:
        session.delete(existing_qs)
        session.commit()

    return queryset_create(r, posted=schema.QuerysetPost.from_put(new,name=queryset))

@app.delete("/queryset/{queryset}/")
def queryset_delete(queryset:str, session = Depends(get_session)):
    """
    Deletes the target queryset (does not delete any data)
    """
    existing = session.query(models.Queryset).get(queryset)
    if existing is not None:
        session.delete(existing)
        session.commit()
        return fastapi.Response(status_code=204)
    return fastapi.Response(status_code=404)

@app.get("/theme/{theme}/")
def list_theme(r:fastapi.Request,theme:str, session = Depends(get_session)):
    """
    Returns a list of the querysets with associated with the requested theme.
    """
    theme = session.query(models.Theme).get(theme)
    if theme is None:
        return fastapi.Response("No such theme",status_code=404)
    querysets = session.query(models.Queryset).filter(models.Queryset.theme == theme).all()
    return [hyperlink(r,qs.path()) for qs in querysets]

@app.patch("/theme/{theme}/{queryset}/")
def theme_associate_queryset(theme:str,queryset:str, session = Depends(get_session)):
    """
    Associates the queryset with the theme, replacing its current association.
    """
    qs = session.query(models.Queryset).get(queryset)

    if qs.theme is not None:
        if len(qs.theme.querysets) == 1:
            session.delete(qs.theme)

    if qs is not None:
        stored_theme = session.query(models.Theme).get(theme)
        if stored_theme is None:
            stored_theme = models.Theme(name=theme)
        qs.theme = stored_theme
        session.add(stored_theme)
        session.commit()
        return Response(status_code=204)

    return Response(f"Queryset {queryset} not found",status_code=404)

@app.get("/theme/")
def theme_list(r:fastapi.Request, session = Depends(get_session)):
    themes = session.query(models.Theme).all()
    return [hyperlink(r,th.path()) for th in themes]
