"""
data_retriever
==============

Exposes the DataRetriever class, which is used to fetch data pertainin to
querysets.
"""
import io
from operator import add
from typing import List, Tuple
import logging
import asyncio
from pymonad.either import Right, Left, Either
from toolz.functoolz import reduce, curry
import aiohttp
from views_schema import viewser as schema
import pandas as pd
from . import models, merge, response_result

identity = lambda x:x

logger = logging.getLogger(__name__)

sequence = curry(reduce,lambda a,b: a.then(lambda x: b.then(lambda y: [x] + [y])))

class DataRetriever():
    """
    DataRetriever
    =============


    """

    def __init__(self, url: str, session: aiohttp.ClientSession):
        self._url = url
        self._session = session

    async def queryset_data_response(self, queryset: models.Queryset) -> Tuple[int, bytes]:
        """
        queryset_data
        =============

        parameters:
            queryset (queryset_manager.models.Queryset)
        returns:
            Tuple[int, bytes]: Can be passed on as a response
        """
        response = await self.fetch_dataframe(queryset)
        return response.either(self._error_response, self._data_response)

    async def fetch_dataframe(self, queryset: models.Queryset)-> Either[List[schema.Dump], pd.DataFrame]:
        """
        _fetch_set
        ==========

        parameters:
            queryset (queryset_manager.models.Queryset)
        returns:
            Either[views_schema.viewser.Dump, pandas.DataFrame]
        """
        results = await asyncio.gather(*map(self._http, self._urls_from_queryset(queryset)))

        data = sequence([r.data for r in results]).maybe(
                Left(["Failed to retrieve data"]),
                lambda dataframes: merge.merge(dataframes).maybe(Left(["Failed to merge dataframes"]), Right))

        errors = sequence([r.error_dump for r in results])

        return errors.maybe(
                data,
                Left)

    def _url_from_path(self, path: str):
        """
        _url
        ====
        _
        """
        return self._url + "/" + path

    def _urls_from_queryset(self, queryset) -> List[str]:
        """
        _urls_from_queryset
        ===================

        parameters:
            queryset (queryset_manager.models.Queryset)
        returns:
            List[str]
        """
        return [self._url_from_path(path) for path in queryset.paths()]

    def _error_response(self, errors: List[schema.Dump]):
        error_dump = reduce(add, errors)
        message = error_dump.json()
        message = "\n".join([str(e) for e in errors])
        # TODO what do i do here?
        if {type(e) for e in errors} == {retrieval.Pending}:
            status_code = 202
        else:
            status_code = max([e.status_code for e in errors])
        return status_code, message

    def _data_response(self, data: pd.DataFrame):
        #data = compatibility.with_index_names(data, queryset.level_of_analysis.name)
        bytes_buffer = io.BytesIO()
        data.to_parquet(bytes_buffer,compression="gzip")
        return 200, bytes_buffer.getvalue()

    async def _http(self, url: str) -> response_result.ResponseResult:
        """
        _http
        =====

        parameters:
            url (str)

        returns:
            response_result.ResponseResult

        """
        async with self._session.get(url) as response:
            return await response_result.ResponseResult.from_aiohttp_response(response)
