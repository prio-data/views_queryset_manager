import os
from typing import List,Optional
import io
from contextlib import closing
from fastapi import Response
import fastapi
import pydantic
import requests
import models
import db
from actions import retrieve_data,link_ops,parse_command,deparse_op

class Queryset(pydantic.BaseModel):
    name:Optional[str]=None
    loa:models.RemoteLOAs 
    ops: List[str]
    theme_name: Optional[str]=None

class Theme(pydantic.BaseModel):
    name:str

app = fastapi.FastAPI()

def hyperlink(r:fastapi.Request,*rest):
    url = r.url
    base = f"{url.scheme}://{url.hostname}:{url.port}"
    return os.path.join(base,*rest)

@app.get("/data/{queryset}/{year}/")
def queryset_data(queryset:str,year:int):
    """
    Retrieve data corresponding to a queryset
    """
    with closing(db.Session()) as sess:
        queryset = sess.query(models.Queryset).get(queryset)
        if queryset is None:
            return Response(status_code=404)
        try:
            data = retrieve_data(queryset,year)
        except requests.HTTPError as e:
            return Response(e.response.content,status_code=e.response.status_code)
        ff = io.BytesIO()
        data.to_parquet(ff,compression="gzip")
        return Response(ff.getvalue(),media_type="application/octet-stream")

@app.get("/queryset/{queryset}/")
def queryset_detail(_:fastapi.Request,queryset:str):
    """
    Get details about a queryset
    """
    with closing(db.Session()) as sess:
        queryset = sess.query(models.Queryset).get(queryset)
        if queryset is None:
            return fastapi.Response(status_code=404)
        return {
            "level_of_analysis": queryset.loa.value,
            "theme": queryset.theme.name if queryset.theme else None,
            "op_commands": [deparse_op(op) for op in queryset.op_roots],
            "op_paths": [op.get_path() for op in queryset.op_roots],
        }

@app.get("/queryset/")
def queryset_list(r:fastapi.Request):
    """
    Lists all current querysets
    """
    with closing(db.Session()) as sess:
        querysets = sess.query(models.Queryset).all()
        links = [hyperlink(r,qs.path()) for qs in querysets]
        return Response(str(links))

@app.post("/queryset/")
def queryset_create(r:fastapi.Request,queryset:Queryset):
    """
    Creates a new queryset
    """

    try:
        assert queryset.name is not None
    except AssertionError:
        return fastapi.Response("Name can not be none when POSTing",status_code=422)

    with closing(db.Session()) as sess:
        operations = []
        for op in queryset.ops:
            chain_root = link_ops(parse_command(op))
            operations.append(chain_root)
        
        if queryset.theme_name:
            theme = sess.query(Theme).get(queryset.theme_name)
            if theme is None:
                theme = Theme(name=queryset.theme_name)
        else:
            theme = None

        stored_queryset = models.Queryset(
                name = queryset.name,
                loa = queryset.loa,
                op_roots = operations,
                theme = theme,
            )
        try:
            sess.add(stored_queryset)
            sess.commit()
        except Exception as e:
            return Response(str(e),status_code=400)

        return Response(hyperlink(r,stored_queryset.path()),status_code=201)

@app.put("/queryset/{queryset}/")
def queryset_replace(r:fastapi.Request,queryset:str,new:Queryset):
    """
    Replaces the queryset with the posted queryset
    """

    try:
        assert new.name is None
    except AssertionError:
        return fastapi.Response("Name must be none when PUTting",status_code=422)

    with closing(db.Session()) as sess:
        existing_qs = sess.query(models.Queryset).get(queryset)
        if existing_qs is None:
            pass
        else:
            sess.delete(existing_qs)
            sess.commit()

    new.name = queryset

    return queryset_create(r,queryset=new)

@app.delete("/queryset/{queryset}/")
def queryset_delete(queryset:str):
    """
    Deletes the target queryset (does not delete any data)
    """
    with closing(db.Session()) as sess:
        existing = sess.query(models.Queryset).get(queryset)
        if existing is not None:
            sess.delete(existing)
            sess.commit()
            return fastapi.Response(status_code=204)
        else:
            return fastapi.Response(status_code=404)

@app.get("/theme/{theme}/")
def list_theme(r:fastapi.Request,theme:str):
    """
    Returns a list of the querysets with associated with the requested theme.
    """
    with closing(db.Session()) as sess:
        theme = sess.query(models.Theme).get(theme)
        if theme is None:
            return fastapi.Response("No such theme",status_code=404)
        querysets = sess.query(models.Queryset).filter(models.Queryset.theme == theme).all()
    return [hyperlink(r,qs.path()) for qs in querysets]

@app.patch("/theme/{theme}/{queryset}/")
def theme_associate_queryset(theme:str,queryset:str):
    """
    Associates the queryset with the theme, replacing its current association.
    """
    with closing(db.Session()) as sess:
        qs = sess.query(models.Queryset).get(queryset)
        if qs.theme is not None:
            if len(qs.theme.querysets) == 1:
                sess.delete(qs.theme)
        if qs is not None:
            stored_theme = sess.query(models.Theme).get(theme)
            if stored_theme is None:
                stored_theme = models.Theme(name=theme)
            qs.theme = stored_theme
            sess.add(stored_theme)
            sess.commit()
            return Response(status_code=204)
        else:
            return Response(f"Queryset {queryset} not found",status_code=404)

@app.get("/theme/")
def theme_list(r:fastapi.Request):
    with closing(db.Session()) as sess:
        themes = sess.query(models.Theme).all()
        return [hyperlink(r,th.path()) for th in themes]
