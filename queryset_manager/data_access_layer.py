
import os
from typing import Optional, List
import datetime
from sqlalchemy import orm
import pandas as pd
from toolz.functoolz import curry
from pymonad.either import Either, Left, Right
from pymonad.promise import Promise
from . import crud, retrieval, models, compatibility, relational_cache

class DataAccessLayer():
    def __init__(self, session: orm.Session, cache: relational_cache.RelationalCache, url: str):
        self.session = session
        self.cache = cache
        self.remote_url = os.path.join(url, "job")

    async def retrieve_data(self, queryset: models.Queryset)-> Promise:
        return (Promise.apply(retrieval.fetch_set).to_arguments(self.remote_url, queryset)
            .then(curry(compatibility.with_index_names, queryset.loa)))

    async def fetch(self,
            name: str,
            start_date: Optional[datetime.date],
            end_date: Optional[datetime.date]) -> Either[List[Exception], pd.DataFrame]:

        data = self.cache.fetch(name, start_date, end_date)

        if data is None:
            data = (crud.get_queryset(self.session, name)
                .then(self.retrieve_data)
                .then(curry(self.cache.store, name))
                .then(Right)
                .catch(lambda exc: Left(exc) if isinstance(exc, list) else Left([exc])))

        return data
