import os
from typing import List
from sqlalchemy import Column,String,Enum,Integer,ForeignKey,JSON,MetaData,Table
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship,validates

from . import schema

metadata = MetaData()
Base = declarative_base(metadata=metadata)

querysets_themes = Table("querysets_themes", Base.metadata,
        Column("theme_name", String, ForeignKey("theme.name")),
        Column("queryset_name", String, ForeignKey("queryset.name"))
    )

class Theme(Base):
    __tablename__ = "theme"

    name = Column(String,primary_key=True)

    querysets = relationship("Queryset", 
            secondary = querysets_themes, 
            back_populates = "themes"
            )

    def path(self):
        return "theme/"+self.name

    def __repr__(self):
        return f"{self.name} ({len(self.querysets)} querysets)"

class Queryset(Base):
    __tablename__ = "queryset"

    name = Column(String,primary_key=True)
    loa = Column(Enum(schema.RemoteLOAs),nullable=False)

    themes = relationship("Theme", 
            secondary = querysets_themes, 
            back_populates = "querysets",
            )

    op_roots = relationship(
            "Operation",
            cascade="all,delete-orphan"
            )

    def paths(self):
        try:
            loa = self.loa.value
        except AttributeError:
            loa = str(self.loa)
        return [os.path.join(loa,op.get_path()) for op in self.op_roots]

    def op_chains(self):
        return [op.get_chain() for op in self.op_roots]

    def path(self):
        return "queryset/"+self.name

class Operation(Base):
    """
    An op is an edge in a chain, corresponding to a remote path.
    """
    __tablename__ = "op"

    base_path = Column(Enum(schema.RemoteBases),nullable=False)
    path = Column(String,nullable=False)
    args = Column(JSON,)

    queryset_name = Column(String,ForeignKey("queryset.name"),nullable = False)

    op_id = Column(Integer,primary_key=True)
    next_op_id = Column(Integer,ForeignKey("op.op_id"))
    next_op = relationship(
            "Operation",
            backref = "previous_op",
            remote_side = [op_id],
            cascade = "all,delete"
        )

    def __str__(self):
        """
        Show operation as path
        """
        try:
            bp = self.base_path.value
        except AttributeError:
            bp = str(self.base_path)

        components = [bp,self.path]

        if self.args is None:
            args = ["_"]
        else:
            args = self.args
        components.append("_".join([str(a) for a in args]))

        return os.path.join(*components)

    def get_path(self):
        return os.path.join(*[str(op) for op in self.get_chain()])

    def get_chain(self,previous=None):
        if previous is None:
            previous = list()
        previous.append(self)
        if self.next_op:
            return self.next_op.get_chain(previous=previous)
        return previous

    def __repr__(self):
        return self.__str__()

    @validates("args")
    def validate_args(self,_,args):
        try:
            assert isinstance(args,list)
        except AssertionError as ae:
            raise ValueError("Arguments must be a list of values") from ae
        try:
            assert all((isinstance(a,(int,str)) for a in args))
        except AssertionError as ae:
            raise ValueError("Arguments must be either str or int") from ae
        return args

    @classmethod
    def from_command(cls,command):
        components = command.split(" ")
        base_path,path,*args = components
        return cls(base_path=base_path,path=path,args=args)

    @classmethod
    def from_pydantic(cls,pydantic_model):
        return cls(
                base_path=pydantic_model.namespace,
                path=pydantic_model.name,
                args=pydantic_model.arguments,
            )

def link_ops(operations:List[Operation])->Operation:
    first,*operations = operations

    prev = first
    for op in operations:
        op.previous_op = [prev]
        prev = op

    return first
