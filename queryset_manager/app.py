import os
import logging
from typing import List,Optional
import io
from contextlib import closing
from datetime import date

from fastapi import Response
import fastapi
import pydantic
from requests import HTTPError

from . import crud,models,schema,db,remotes,settings

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

@app.get("/data/{queryset_name}/")
def queryset_data(queryset_name:str,
        start_date:Optional[date]=None,end_date:Optional[date]=None):
    """
    Retrieve data corresponding to a queryset
    """
    with closing(db.Session()) as session:
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
def queryset_create(r:fastapi.Request,queryset:schema.QuerysetPost):
    """
    Creates a new queryset
    """

    with closing(db.Session()) as sess:
        operation_roots = []
        for chain in queryset.operations:
            root = models.link_ops([models.Operation.from_pydantic(op) for op in chain.steps])
            operation_roots.append(root)
        
        if queryset.theme_name:
            theme = sess.query(models.Theme).get(queryset.theme_name)
            if theme is None:
                theme = models.Theme(name=queryset.theme_name)
        else:
            theme = None

        stored_queryset = models.Queryset(
                name = queryset.name,
                loa = queryset.loa,
                op_roots = operation_roots,
                theme = theme,
            )
        try:
            sess.add(stored_queryset)
            sess.commit()
        except Exception as e:
            return Response(str(e),status_code=400)

        return Response(hyperlink(r,stored_queryset.path()),status_code=201)

@app.put("/queryset/{queryset}/")
def queryset_replace(r:fastapi.Request,queryset:str,new: schema.QuerysetPut):
    """
    Replaces the queryset with the posted queryset
    """

    with closing(db.Session()) as sess:
        existing_qs = sess.query(models.Queryset).get(queryset)
        if existing_qs is None:
            pass
        else:
            sess.delete(existing_qs)
            sess.commit()

    return queryset_create(r,queryset=schema.QuerysetPost.from_put(new,name=queryset))

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
