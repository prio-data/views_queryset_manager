
import logging
from typing import List
import asyncio
from io import BytesIO

import aiohttp
import fast_views
import pandas as pd
import numpy as np

from . import models

logger = logging.getLogger(__name__)

async def fetch_set(source_url: str, queryset: models.Queryset):
    urls = map(lambda p: source_url + "/" + p, queryset.paths())
    responses = await fetch_set_components(urls)
    datasets = [pq_bytes_to_pandas(rsp) for rsp in responses]
    datasets = [df for df in datasets if np.dtype("O") not in set(df.dtypes)]
    return fast_views.inner_join(datasets)

async def fetch(session, url):
    logger.info(f"Fetching {url}")
    async with session.get(url) as response:
        print(url)
        return await response.content.read()

async def fetch_set_components(urls)->List[bytes]:
    async with aiohttp.ClientSession() as session:
        return await asyncio.gather(*[fetch(session, url) for url in urls])

def pq_bytes_to_pandas(bytes):
    return pd.read_parquet(BytesIO(bytes))
