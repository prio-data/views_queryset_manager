"""
data_retriever
==============

Exposes the DataRetriever class, which is used to fetch data pertaining to
querysets.
"""
from collections import defaultdict
import datetime
import io
from typing import List, Tuple, TypeVar
import logging
import asyncio
from pymonad.either import Right, Left, Either
from pymonad.maybe import Just, Nothing, Maybe
from toolz.functoolz import reduce
import aiohttp
from views_schema import viewser as schema
import pandas as pd
import models
import merge
import response_result

logger = logging.getLogger(__name__)

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

    async def fetch_dataframe(self, queryset: models.Queryset)-> Either[List[response_result.ResponseResult], pd.DataFrame]:
        """
        _fetch_set
        ==========

        parameters:
            queryset (queryset_manager.models.Queryset)
        returns:
            Either[List[response_result.ResponseResult], pandas.DataFrame]

        Tries to fetch a dataframe corresponding to a queryset. Returns either
        a dataframe, or a list of responses, some of which are errors (Non 2xx
        responses).
        """
        results = await asyncio.gather(*map(self._http, self._urls_from_queryset(queryset)))
        data = sequence([r.data for r in results])
        dataframe = data.maybe(
                Left([response_result.ResponseResult(500,"Failed to deserialize")]),
                lambda dataframes: merge.merge(dataframes).maybe(Left([response_result.ResponseResult(500, "Failed to merge")]), Right))

        errors = [r for r in results if r.pending or not r.ok]
        errors = Just(errors) if errors else Nothing

        return errors.maybe(
                dataframe,
                Left)

    def _url_from_path(self, path: str) -> str:
        """
        _url_from_path

        ====

        parameters:
            path (str)

        returns:
            str: url

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

        Returns queryset operation paths as urls
        """
        return [self._url_from_path(path) for path in queryset.paths()]

    def _error_response(self, errors: List[response_result.ResponseResult]) -> Tuple[int, bytes]:
        """
        _error_response
        ===============

        parameters:
            errors (List[queryset_manager.response_result.ResponseResult])
        returns:
            Tuple[int, bytes]

        Compile error responses into a views_schema.viewser.Dump object that is
        returned as JSON bytes, along with the max HTTP code (indicating the
        most serious error).
        """
        status_code = max([e.status_code for e in errors])

        messages = [schema.Message(content = e.content) for e in errors]
        dump = schema.Dump(
                title     = self.HTTP_STATUS_CODE_DESCRIPTIONS[status_code],
                timestamp = datetime.datetime.now(),
                username  = "queryset-manager",
                messages  = messages
                )

        return status_code, dump.json().encode()

    def _data_response(self, data: pd.DataFrame) -> Tuple[int, bytes]:
        """
        _data_response
        ==============

        parameters:
            data (pandas.DataFrame)
        returns:
            Tuple[int, bytes]

        Returns pandas dataframe as parquet bytes, along with a 200 code.
        """
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

    HTTP_STATUS_CODE_DESCRIPTIONS = defaultdict(lambda: "Something went wrong", {
            500: "Internal server error",
            404: "Not found",
            400: "Bad request"
        })

T = TypeVar("T")
def sequence(l: List[Maybe[T]]) -> Maybe[List[T]]:
    """
    sequence
    ========
    parameters:
        l (List[Maybe[T]])
    returns:
        Maybe[List[T]]

    Reduce a list of Maybe values into a single Maybe containing the list, or
    Nothing, akin to Haskells "sequence" function.
    """
    return reduce(lambda a,b: a.then(lambda x: b.then(lambda y: x + [y])), l, Just([]))
