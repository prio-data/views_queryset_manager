
import logging
import asyncio
from io import BytesIO
from pymonad.either import Right, Left, Either
from toolz.functoolz import compose, curry, reduce
import aiohttp
import fast_views
import pandas as pd
from . import models

identity = lambda x:x

logger = logging.getLogger(__name__)

class HTTPNotOk(Exception):
    def __init__(self, url, status_code, content):
        self.status_code = status_code
        self.content = content
        super().__init__(f"{url} returned {status_code} ({content})")

class DeserializationError(Exception):
    def __init__(self, bytes):
        self.status_code = 500
        super().__init__(f"Could not deserialize as parquet: {bytes}")

class Pending(Exception):
    def __init__(self,url):
        super().__init__(f"{url} is pending")

def distinguish_string(by, existing, new):
    if new in existing:
        return distinguish_string(by, existing, by(new))
    else:
        return new

def concat_distinct(by, existing, new):
    return existing + [distinguish_string(by, existing, new)]

distinct_names = lambda names: reduce(
        curry(concat_distinct, lambda s: "_"+s),
        names,
        [])

async def get(session: aiohttp.ClientSession, url: str)->Either:
    async with session.get(url) as response:
        content = await response.content.read()
        if (status := response.status) == 200:
            return Right(content)
        elif status == 202:
            return Left(Pending(url))
        else:
            return Left(HTTPNotOk(url,status, content))

def make_urls(base_url: str, queryset):
    return [base_url + "/" + path for path in queryset.paths()]

async def deserialize(request_result) -> pd.DataFrame:
    request_result = await request_result
    pd_from_bytes = lambda b: pd.read_parquet(BytesIO(b))
    try:
        return request_result.either(Left, lambda bytes: Right(pd_from_bytes(bytes)))
    except OSError:
        return Left(DeserializationError(request_result.either(str,str)))

async def fetch_set(base_url: str, queryset: models.Queryset)-> Either:
    async with aiohttp.ClientSession() as session:
        get_data = compose(
                deserialize,
                curry(get,session),
            )

        results = await asyncio.gather(
                *map(get_data, make_urls(base_url, queryset))
            )

    errors = reduce(lambda a,b: a + b.either(lambda x: [x], lambda x: list()), results, [])
    if errors:
        return Left(errors)

    results = [res.value for res in results]

    for df,name in zip(results, distinct_names([res.columns[0] for res in results])):
        df.columns = [name]

    return Right(fast_views.inner_join(results))
