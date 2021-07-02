
import logging
from typing import List, Optional, Tuple
import asyncio
from io import BytesIO

import aiohttp
import fast_views
import pandas as pd
import numpy as np

from . import models

logger = logging.getLogger(__name__)

class HTTPNotOk(Exception):
    def __init__(self, url, status_code, content):
        self.status_code = status_code
        self.content = content
        super().__init__(f"{url} returned {status_code} ({content})")

class DeserializationError(Exception):
    pass

async def fetch_set(
        source_url: str,
        queryset: models.Queryset)-> Tuple[Optional[pd.DataFrame], Optional[Exception]]:

    urls = map(lambda p: source_url + "/" + p, queryset.paths())
    responses = await fetch_set_components(urls)

    deserializations = [pq_bytes_to_pandas(rsp) for rsp in responses]
    if (errors := [err for _,err in deserializations if err is not None]):
        return (None, DeserializationError("\n".join([str(error) for error in errors])))

    datasets = [df for df,_ in deserializations if np.dtype("O") not in set(df.dtypes)]
    return (fast_views.inner_join(datasets), None)

async def fetch(session, url):
    logger.info(f"Fetching {url}")
    async with session.get(url) as response:
        return await response.content.read()

async def fetch_set_components(urls)->List[bytes]:
    async with aiohttp.ClientSession() as session:
        return await asyncio.gather(*[fetch(session, url) for url in urls])

def pq_bytes_to_pandas(bytes)-> Tuple[Optional[pd.DataFrame],Optional[Exception]]:
    try:
        data = pd.read_parquet(BytesIO(bytes))
    except OSError:
        return (None, DeserializationError(f"Could not deserialize as dataframe: {bytes}"))
    return (data, None)
