import os
import logging
from typing import Optional
from datetime import date

from fastapi import Response, Depends
from fastapi.responses import JSONResponse
import fastapi
import views_schema as schema
import aiohttp

from . import crud, models, db, remotes, settings, data_retriever

logger = logging.getLogger(__name__)

try:
    logging.basicConfig(level=getattr(logging,settings.LOG_LEVEL))
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

remotes_api = remotes.Api(
        source_url = os.path.join(settings.JOB_MANAGER_URL,"job")
        )

@app.get("/")
def handshake():
    """
    Returns information about the app, including which version of viewser it expects.
    """
    return JSONResponse({
            "viewser_version": "3.0.0"
        })

@app.get("/data/{queryset_name}")
async def queryset_data(
        queryset_name:str,
        start_date:Optional[date]=None, end_date:Optional[date]=None,
        session = Depends(get_session)):
    """
    Retrieve data corresponding to a queryset
    """
    queryset = crud.get_queryset(session,queryset_name)

    if queryset is None:
        return Response(status_code=404)

    async with aiohttp.ClientSession() as http_session:
        retriever = data_retriever.DataRetriever(
                settings.JOB_MANAGER_URL+"/job",
                http_session)
        status_code, content = await retriever.queryset_data_response(queryset)
        return Response(content, status_code = status_code)

@app.get("/querysets/{queryset}")
def queryset_detail(queryset:str, session = Depends(get_session)):
    """
    Get details about a queryset
    """

    queryset = session.query(models.Queryset).get(queryset)
    if queryset is None:
        return fastapi.Response(status_code=404)
    return queryset.dict()

@app.get("/querysets")
def queryset_list(session = Depends(get_session)):
    """
    Lists all current querysets
    """
    querysets = session.query(models.Queryset).all()

    return JSONResponse({
                "querysets":[queryset.name for queryset in querysets]
            })

@app.post("/querysets")
def queryset_create(
        posted: schema.Queryset,
        overwrite: bool = False,
        session = Depends(get_session)):
    """
    Creates a new queryset
    """
    if overwrite:
        try:
            crud.delete_queryset(session, posted.name)
        except crud.DoesNotExist:
            pass

    try:
        queryset = crud.create_queryset(session,posted)
    except crud.Exists:
        return Response(
                f"Queryset \"{posted.name}\" already exists, overwrite False",
                status_code=409)
    return JSONResponse({
               "name":queryset.name
           })

class QuerysetPut(schema.Queryset):
    name: Optional[str] = None

@app.put("/querysets/{queryset}")
def queryset_replace(
        queryset:str, new: QuerysetPut,
        session = Depends(get_session)):
    """
    Replaces the queryset with the posted queryset
    """
    new.name = queryset
    return queryset_create(new, overwrite = True, session = session)

@app.delete("/querysets/{queryset}")
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

@app.patch("/themes/{theme_name}/{queryset_name}")
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

@app.get("/themes")
def theme_list(session = Depends(get_session)):
    themes = session.query(models.Theme).all()
    return JSONResponse({
            "querysets": [theme.name for theme in themes]
        })

@app.get("/themes/{theme}")
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
