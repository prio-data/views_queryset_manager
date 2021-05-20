
import enum
from typing import List,Optional
from pydantic import BaseModel

class RemoteBases(enum.Enum):
    trf="trf"
    base="base"

class RemoteLOAs(enum.Enum):
    priogrid_month="priogrid_month"
    country_month = "country_month"

class Operation(BaseModel):
    base: str
    path: str
    args: List[str]

class Queryset(BaseModel):
    loa: RemoteLOAs
    operations: List[List[Operation]]
    theme_name: Optional[str]=None

class QuerysetPut(Queryset):
    pass

class QuerysetPost(Queryset):
    name:Optional[str]=None

    @classmethod
    def from_put(cls,put:QuerysetPut,name:str):
        return cls(
            name = name,
            loa = put.loa.value,
            operations = put.operations,
            theme_name = put.theme_name
        )

class Theme(BaseModel):
    name:str
