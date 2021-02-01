
import sys
from io import BytesIO
from contextlib import closing
import os
from typing import List

import pandas as pd
import requests
from sqlalchemy.exc import InvalidRequestError

from models import Queryset,Op,Theme
from db import Session
from env import env

def retrieve_data(queryset:Queryset,year:int)->pd.DataFrame:
    with closing(Session()) as sess:
        dataset = None
        for rootnode,path in zip(queryset.op_roots,queryset.paths(year=year)):
            full_path = os.path.join(env("ROUTER_URL"),path)
            print(full_path)
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

def deparse_op(op:Op)->str:
    try:
        base = op.base_path.value
    except AttributeError:
        base = str(op.base_path)
    cmd = f"{base} {op.path}"
    if op.args is not None:
        cmd += op.args

    if op.next_op is not None:
        cmd = deparse_op(op.next_op) + " | " + cmd

    return cmd

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

if __name__ == "__main__":
    commands = [
        "base year.year",
        "base month.month",
        "base priogrid_month.ged_best_ns",
        "base priogrid_month.ged_best_ns | trf tlag 1",
    ]
    roots = [link_ops(parse_command(cmd)) for cmd in commands]
    print(roots)

    qs = Queryset(
            name = "testqs",
            loa = "priogrid_month",
            op_roots = roots
            )
    d = retrieve_data(qs,1990)
    d.to_csv("/tmp/d.csv")

