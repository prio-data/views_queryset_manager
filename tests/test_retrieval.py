#from collections import namedtuple
import asyncio
import string
from io import BytesIO
from unittest import TestCase
from unittest.mock import patch
from pymonad.either import Left, Right
import pandas as pd
import numpy as np
from queryset_manager import retrieval

identity = lambda x:x

class MockQueryset():
    def __init__(self,paths):
        self._paths = paths
    def paths(self):
        return self._paths

async def bad_request_500(_, __):
    return Left(retrieval.HTTPNotOk("http://foobar", 500, "something went wrong"))

async def bad_request_gibberish(_, __):
    return Right(b"abcdefg")

async def test_success(_, __):
    data = pd.DataFrame(np.ones((9,9)))
    data.index = pd.MultiIndex.from_product(((1,2,3),(1,2,3)))
    data.columns = list(string.ascii_letters[:9])

    buf = BytesIO()
    data.to_parquet(buf)
    return Right(buf.getvalue())

class TestRetrieval(TestCase):
    @patch("queryset_manager.retrieval.get", bad_request_500)
    def test_500_request(self):
        res = asyncio.run(retrieval.fetch_set("http://",MockQueryset(["foo"])))
        error = res.either(identity,identity)[0]
        self.assertTrue(res.is_left())
        self.assertIs(type(error),retrieval.HTTPNotOk)

    @patch("queryset_manager.retrieval.get", bad_request_gibberish)
    def test_gibberish(self):
        res = asyncio.run(retrieval.fetch_set("http://",MockQueryset(["foo"])))
        error = res.either(identity,identity)[0]
        self.assertTrue(res.is_left())
        self.assertIs(type(error),retrieval.DeserializationError)

    @patch("queryset_manager.retrieval.get", test_success)
    def test_success(self):
        res = asyncio.run(retrieval.fetch_set("http://",MockQueryset(["foo"])))
        self.assertIs(type(res.value), pd.DataFrame)
        print(res.value)
