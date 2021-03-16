
import sys
from io import BytesIO
from contextlib import closing
import os
from typing import List

import pandas as pd
import requests
from sqlalchemy.exc import InvalidRequestError

from models import Queryset,Operation,Theme
from db import Session
from env import env
import settings
import logging

def retrieve_data(queryset:Queryset,year:int)->pd.DataFrame:
    with closing(Session()) as sess:
        dataset = None
        for rootnode,path in zip(queryset.op_roots,queryset.paths(year=year)):
            full_path = os.path.join(settings.ROUTER_URL,path)
            logging.error(full_path)
            r = requests.get(full_path)

            if r.status_code == 200:
                data = pd.read_parquet(BytesIO(r.content))
                if dataset is not None:
                    dataset = dataset.join(data)
                else:
                    dataset = data
            else:
                raise requests.HTTPError(response=r)

        return dataset

def link_ops(ops:List[Operation])->Operation:
    ops.reverse()
    prev = ops.pop()
    first = prev
    ops.reverse()
    for op in ops:
        op.previous_op = [prev]
        prev = op
    return first
