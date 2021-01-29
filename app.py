import fastapi

app = fastapi.FastAPI()

@app.get("/data/{queryset}")
def queryset_data(queryset:str):
    """
    Retrieve data corresponding to a queryset
    """
    pass

@app.get("/queryset/{queryset}/")
def queryset_detail(queryset:str):
    """
    Get details about a queryset
    """
    pass

@app.get("/queryset/")
def queryset_list(queryset:str):
    """
    Lists all current querysets
    """
    pass

@app.post("/queryset/")
def queryset_create(queryset:QuerySet):
    """
    Creates a new queryset
    """
    pass

@app.put("/queryset/{queryset}")
def queryset_replace(queryset:str,new:QuerySet):
    """
    Replaces the queryset with the posted queryset
    """
    pass

@app.delete("/queryset/{queryset}")
def queryset_delete(queryset:str):
    """
    Deletes the target queryset (does not delete any data)
    """
    pass

@app.get("/theme/{theme}")
def list_theme():
    """
    Returns a list of the querysets with associated with the requested theme.
    """
    pass

@app.patch("/theme/{theme}/{queryset}")
def theme_associate_queryset(theme:str,queryset:str):
    """
    Associates the queryset with the theme, replacing its current association.
    """
    pass

@app.post("/theme/")
def theme_create(theme:Theme):
    """
    Creates a new Theme
    """
    pass
