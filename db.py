from typing import List
import os
import enum

from sqlalchemy import Column,String,Table,Enum,Integer,ForeignKey
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship,sessionmaker
from sqlalchemy import create_engine

import pandas as pd

from env import env

Base = declarative_base()

class RemoteBases(enum.Enum):
    transform="trf"
    base="base"

class RemoteLOAs(enum.Enum):
    priogrid_month="pgm"
    country_month = "cm"

class Theme(Base):
    __tablename__ = "theme"
    name = Column(String,primary_key=True)

class Queryset(Base):
    __tablename__ = "queryset"
    name = Column(String,primary_key=True)
    loa = Column(Enum(RemoteLOAs),nullable=False)

    theme_id = Column(String,ForeignKey("theme.name"))
    theme = relationship("Theme",backref="querysets")

    op_roots = relationship("Op",cascade="all,delete")

    def paths(self):
        return [os.path.join(self.loa,op.get_path()) for op in self.op_roots]

    def op_chains(self):
        return [op.get_chain() for op in self.op_roots]

class Op(Base):
    """
    An op is an edge in a chain, corresponding to a remote path.
    """
    __tablename__="op"

    base_path = Column(Enum(RemoteBases),nullable=False)
    path = Column(String,nullable=False)
    args = Column(String,)

    queryset_name = Column(String,ForeignKey("queryset.name"))

    op_id = Column(Integer,primary_key=True)
    next_op_id = Column(Integer,ForeignKey("op.op_id"))
    next_op = relationship("Op",backref="previous_op",remote_side=[op_id],cascade="all,delete")

    def __str__(self):
        try:
            bp = self.base_path.value
        except AttributeError:
            bp = str(self.base_path)

        components = [bp,self.path]

        if self.base_path == "transform":
            if self.args is None:
                a = "_"
            else:
                a = self.args
            components = components + [a]

        return os.path.join(*components)

    def get_path(self):
        return os.path.join(*[str(op) for op in self.get_chain()])

    def get_chain(self,previous=None):
        if previous is None:
            previous = list()
        previous.append(self)
        if self.next_op:
            return self.next_op.get_chain(previous=previous)
        else:
            return previous

    def __repr__(self):
        return self.__str__()

    def __repr__(self):
        return self.__str__()

def link_ops(ops:List[Op])->Op:
    ops.reverse()
    prev = ops.pop()
    first = prev
    ops.reverse()
    for op in ops:
        op.previous_op = [prev]
        prev = op
    return first

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

def retrieve_data(queryset:Queryset)->pd.DataFrame:
    paths = queryset.paths()
    for p in paths:
        print(f"Getting {os.path.join(env('ROUTER_URL'),p)}")

if __name__ == "__main__":
    engine = create_engine("sqlite://")
    session = sessionmaker(bind=engine)()
    Base.metadata.create_all(engine)

    theme_a = Theme(name="a")
    qs1 = Queryset(name="qs",loa="priogrid_month",theme=theme_a)

    r1 = link_ops(parse_command("base priogrid_month.ged_best_ns |"
        "transform identity |"
        "transform splag 1 1|"
        "transform splag 1 2"))
    r2 = link_ops(parse_command("base country.name"))
    
    qs1.op_roots.append(r1)
    qs1.op_roots.append(r2)
    retrieve_data(qs1)

        
