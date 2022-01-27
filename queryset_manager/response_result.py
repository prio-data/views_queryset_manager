import io
import aiohttp
import pandas as pd
from pymonad.maybe import Just, Nothing, Maybe
from views_schema import viewser as schema

class ResponseResult():
    def __init__(self, status_code, content):
        self.content = content
        self.status_code = status_code

    @classmethod
    async def from_aiohttp_response(cls, response: aiohttp.ClientResponse) -> "ResponseResult":
        content = await response.content.read()
        return cls(status_code = response.status, content = content)

    @property
    def pending(self):
        return self.status_code == 202

    @property
    def error_dump(self) -> Maybe[schema.Dump]:
        if str(self.status_code)[0] == "2":
            return Nothing
        else:
            return Just(schema.Dump())

    @property
    def data(self) -> Maybe[pd.DataFrame]:
        """
        data
        ====

        Maybe a pandas dataframe, if deserializable.
        """
        if self.status_code:
            return self._pd_from_bytes(self.content)
        else:
            return Nothing

    def _pd_from_bytes(self, data: bytes) -> Maybe[pd.DataFrame]:
        try:
            return Just(pd.read_parquet(io.BytesIO(data)))
        except Exception:
            return Nothing
