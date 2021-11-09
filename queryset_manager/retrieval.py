
from typing import TypeVar, List, Any

import logging
import asyncio
from io import BytesIO
from pymonad.either import Right, Left, Either
from pymonad.promise import Promise
from toolz.functoolz import compose, curry, reduce
import aiohttp
import fast_views
import pandas as pd
from . import models

identity = lambda x:x

logger = logging.getLogger(__name__)

T = TypeVar("T")

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
        self.status_code = 202
        super().__init__(f"{url} is pending")

def combine_either(left: bool, eithers):
    extractors = (lambda x: list(), lambda x: [x])
    extractors = extractors if not left else extractors[::-1]
    return reduce(lambda a,b: a + b.either(*extractors), eithers, [])

lefts, rights = (curry(combine_either, b) for b in (True, False))

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

def list_with_distinct_names(dfs: List[pd.DataFrame])-> List[pd.DataFrame]:

    seen = []
    for df in dfs:
        seen_before = len(seen)
        seen = distinct_names(seen + list(df.columns))
        df.columns = seen[seen_before:]

    return dfs


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

def pd_from_bytes(data: bytes)-> Either[Exception, pd.DataFrame]:
    try:
        return Right(pd.read_parquet(BytesIO(data)))
    except OSError:
        return Left(DeserializationError(str(data)))

async def fetch_set(base_url: str, queryset: models.Queryset)-> Promise:
    def get_data(session, url):
        return (Promise.apply(get).to_arguments(session,url)
            .then(lambda m: m.then(pd_from_bytes)))

    async with aiohttp.ClientSession() as session:
        results = await asyncio.gather(
                *map(curry(get_data, session), make_urls(base_url, queryset))
            )

    errors = lefts(results)

    if errors:
        return Promise(lambda resolve,reject: reject(errors))

    results: List[pd.DataFrame] = rights(results)
    results = list_with_distinct_names(results)
    df = fast_views.inner_join(results)

    return Promise(lambda resolve,reject: resolve(df))
