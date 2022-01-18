
import json
from typing import List
import datetime
import logging
import asyncio
from io import BytesIO
from pymonad.either import Right, Left, Either
from pymonad.maybe import Maybe, Just, Nothing
from toolz.functoolz import curry, reduce
import aiohttp
#import fast_views
from pydantic import ValidationError
from pyarrow.lib import ArrowInvalid
import pandas as pd
from views_schema import viewser as schema
from . import models, errors

logger = logging.getLogger(__name__)

async def queryset_responses(base_url: str, queryset: models.Queryset) -> List[models.Response]:
    """
    queryset_responses
    ==================
    """
    def make_urls(base_url: str, queryset):
        return [base_url + "/" + path for path in queryset.paths()]

    async def get(session: aiohttp.ClientSession, url: str) -> models.Response:
        async with session.get(url) as response:
            content = await response.content.read()
            return models.Response(content = content, status_code = response.status)

    async with aiohttp.ClientSession() as session:
        results = await asyncio.gather(*map(curry(get, session), make_urls(base_url, queryset)))

    return results

def data_response(responses: List[models.Response]) -> models.Response:
    """
    data_response
    =============
    """
    return (deserialize_data(responses)
        .then(merge_data)
        .maybe(
            models.Response(content = "Failed to deserialize data from upstream response", status_code = 500),
            dataframe_as_response))

def deserialize_data(responses: List[models.Response]) -> Maybe[List[pd.DataFrame]]:
    """
    deserialize_data
    ================
    """
    try:
        return Just(map(lambda rsp: pd.read_parquet(BytesIO(rsp.content)), responses))
    except (OSError, ArrowInvalid):
        logger.critical("Failed to deserialize data from upstream.")
        return Nothing

def merge_data(dataframes: List[pd.DataFrame]) -> Maybe[pd.DataFrame]:
    """
    merge_data
    ==========
    """
    try:
        return Just(reduce(lambda a,b: a.merge(b, left_index = True, right_index = True, how = "inner"), dataframes))
    except Exception as e:
        logger.critical(f"Failed to join data due to exception: {str(e)}.")
        return Nothing

def dataframe_as_response(dataframe: pd.DataFrame) -> models.Response:
    """
    dataframe_as_response
    =====================
    """
    buf = BytesIO()
    dataframe.to_parquet(buf)
    return models.Response(content = buf.getvalue(), status_code = 200)

def error_response(responses: List[models.Response]) -> models.Response:
    """
    error_response
    ==============
    """
    messages = []
    for response in responses:
        try:
            propagated = schema.Dump(**json.loads(response.content))
            messages += propagated.messages
        except ValidationError:
            try:
                messages += errors.DEFAULT_ERROR_MESSAGES[response.status_code]
            except KeyError:
                messages += schema.Message(
                        content = response.content.decode())

    error_dump = schema.Dump(
            title = "Queryset manager had an issue.",
            timestamp = datetime.datetime.now(),
            messages = messages)

    return models.Response(
            status_code = max([rsp.status_code for rsp in responses]),
            content = error_dump.json())

def check_for_errors(responses: List[models.Response]) -> Either[List[models.Response], List[models.Response]]:
    """
    check_for_errors
    ================
    """
    error_responses = [rsp for rsp in responses if rsp.status_code != 200]
    return Left(error_responses) if error_responses else Right(responses)
