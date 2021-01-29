
from io import BytesIO
from contextlib import closing
import os
from typing import List

import pandas as pd
import requests

from models import Queryset,Op,Theme
from db import Session
from env import env

def retrieve_data(queryset:Queryset)->pd.DataFrame:
    """
    Not tested...
    """
    with closing(Session()) as sess:
        dataset = None
        for rootnode,path in zip(queryset.op_roots,queryset.paths()):
            full_path = os.path.join(env("ROUTER_URL"),path)
            r = requests.get(full_path)

            if r.status_code == 200:
                data = pd.read_parquet(BytesIO(r.content))
                if dataset is not None:
                    dataset = dataset.merge(data,on_index=True)
                else:
                    dataset = data
            else:
                sess.delete(rootnode)
        sess.commit()
        return dataset

def parse_command(command:str)->Op:
    """
    Parses a command and returns an Op chain
    ex. db country.name | splag 1 1 | templag -1
        Op("trf","templag","-1")
        Op("trf","splag","1_1")
        Op("base","country.name")
    """
    commands = command.split("|")
    ops = []
    for cmd in [c.strip() for c in commands]:
        base_path,path,*args = cmd.split(" ")
        op = Op(
            base_path = base_path,
            path = path,
            )
        if args:
            op.args = "_".join(args)
        ops.append(op)
    ops.reverse()
    return ops

def link_ops(ops:List[Op])->Op:
    ops.reverse()
    prev = ops.pop()
    first = prev
    ops.reverse()
    for op in ops:
        op.previous_op = [prev]
        prev = op
    return first

