
import unittest
import json
import io
import asyncio
from unittest.mock import AsyncMock
import pandas as pd
from pandas.testing import assert_frame_equal
import numpy as np
from pymonad.maybe import Just, Nothing
import views_schema as schema
from views_schema.viewser import Dump
from alchemy_mock.mocking import UnifiedAlchemyMagicMock
from queryset_manager import data_retriever, models, response_result

class TestDataRetriever(unittest.TestCase):
    def setUp(self):
        self.retriever = data_retriever.DataRetriever("http://0.0.0.0", None)
        self.sess = UnifiedAlchemyMagicMock()
        self.mock_queryset = models.Queryset.from_pydantic(
                    self.sess,
                    schema.Queryset(
                    name = "_",
                    loa= "_",
                    themes = [],
                    description = "",
                    operations = [
                        [schema.DatabaseOperation(name = "table.column", arguments = ["values"])],
                        ]))

    def test_sequence(self):
        x = [Just(1), Just(2), Just(3), Just(4)]
        self.assertTrue(data_retriever.sequence(x).is_just())
        self.assertEqual(data_retriever.sequence(x).value, [1,2,3,4])
        self.assertTrue(data_retriever.sequence([Just(1), Just(2), Nothing]).is_nothing())

    def test_success(self):
        dataframe = pd.DataFrame(
                np.zeros(9),
                index = pd.MultiIndex.from_product((range(3), range(3)), names = ["time","unit"]),
                columns = ["a"])
        buf = io.BytesIO()
        dataframe.to_parquet(buf)

        self.retriever._http = AsyncMock()
        self.retriever._http.return_value = response_result.ResponseResult(200, buf.getvalue())

        coro = self.retriever.queryset_data_response(self.mock_queryset)

        _,res = asyncio.run(coro)
        assert_frame_equal(dataframe, pd.read_parquet(io.BytesIO(res)))


    def test_deserialization_error(self):
        self.retriever._http = AsyncMock()
        self.retriever._http.return_value = response_result.ResponseResult(200, "fgsfds")
        coro = self.retriever.queryset_data_response(self.mock_queryset)

        status_code,res = asyncio.run(coro)
        res = Dump(**json.loads(res.decode()))
        self.assertEqual(status_code, 500)
        self.assertIn("eserializ", res.messages[0].content)
