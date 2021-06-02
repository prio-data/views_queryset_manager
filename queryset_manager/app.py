import os
import logging
from typing import Optional
import io
from datetime import date

from fastapi import Response, Depends
from fastapi.responses import JSONResponse
import fastapi
from requests import HTTPError

from . import crud,models,schema,db,remotes,settings

logger = logging.getLogger("azure.core.pipeline.policies.http_logging_policy")
logger.setLevel(logging.WARNING)

try:
    logging.basicConfig(level=getattr(logging,settings.config("LOG_LEVEL")))
except AttributeError:
    pass

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

remotes_api = remotes.Api(source_url = settings.config("SOURCE_URL"))

@app.get("/")
def handshake():
    """
    Returns information about the app, including which version of viewser it expects.
    """
    return JSONResponse({
            "viewser_version": "2.0.0"
        })

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
        data = remotes_api.fetch_data_for_queryset(queryset,start_date,end_date)
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
    return JSONResponse({
                "name": queryset.name,
                "description": queryset.description if queryset.description is not None else "",
                "loa": queryset.loa.value,
                "themes": [theme.name for theme in queryset.themes],
                "operations": [[op.dict() for op in root.get_chain()] for root in queryset.op_roots],
            })

@app.get("/queryset/")
def queryset_list(session = Depends(get_session)):
    """
    Lists all current querysets
    """
    querysets = session.query(models.Queryset).all()

    return JSONResponse({
                "querysets":[queryset.name for queryset in querysets]
            })

@app.post("/queryset/")
def queryset_create(posted: schema.QuerysetPost, session = Depends(get_session)):
    """
    Creates a new queryset
    """
    try:
        queryset = crud.create_queryset(session,posted)
    except crud.Exists:
        return Response(f"Queryset \"{posted.name}\" already exists", status_code=409)
    return JSONResponse({
               "name":queryset.name
           })

@app.put("/queryset/{queryset}/")
def queryset_replace(
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

    return queryset_create(
            posted = schema.QuerysetPost.from_put(new,name=queryset),
            session = session
        )

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

@app.patch("/theme/{theme_name}/{queryset_name}/")
def theme_associate_queryset(theme_name:str, queryset_name:str, session = Depends(get_session)):
    """
    Associates the queryset with the theme, replacing its current association.
    """
    theme = crud.get_or_create_theme(session, theme_name)
    queryset = session.query(models.Queryset).get(queryset_name)
    if queryset is None:
        return Response(f"No queryset named {queryset_name}", status_code=404)
    queryset.themes.append(theme)
    session.commit()
    return Response(f"{queryset_name} associated with {theme_name}")

@app.get("/theme/")
def theme_list(session = Depends(get_session)):
    themes = session.query(models.Theme).all()
    return JSONResponse({
            "querysets": [theme.name for theme in themes]
        })

@app.get("/theme/{theme}/")
def theme_detail(theme:str, session = Depends(get_session)):
    """
    Returns a list of the querysets with associated with the requested theme.
    """
    theme = session.query(models.Theme).get(theme)
    if theme is None:
        return fastapi.Response("No such theme",status_code=404)
    return JSONResponse({
            "name": theme.name,
            "description": theme.description if theme.description is not None else "",
            "querysets": [qs.name for qs in theme.querysets]
        })
