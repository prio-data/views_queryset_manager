import os
import enum
from sqlalchemy import Column,String,Enum,Integer,ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()

class RemoteBases(enum.Enum):
    trf="trf"
    base="base"

class RemoteLOAs(enum.Enum):
    priogrid_month="priogrid_month"
    country_month = "country_month"

class Theme(Base):
    __tablename__ = "theme"
    name = Column(String,primary_key=True)
    
    def path(self):
        return "theme/"+self.name

class Queryset(Base):
    __tablename__ = "queryset"
    name = Column(String,primary_key=True)
    loa = Column(Enum(RemoteLOAs),nullable=False)

    theme_id = Column(String,ForeignKey("theme.name"))
    theme = relationship("Theme",backref="querysets")

    op_roots = relationship("Op",cascade="all,delete")

    def paths(self,year):
        try:
            loa = self.loa.value
        except AttributeError:
            loa = str(self.loa)
        return [os.path.join(loa,op.get_path(),str(year)) for op in self.op_roots]

    def op_chains(self):
        return [op.get_chain() for op in self.op_roots]
    
    def path(self):
        return "queryset/"+self.name


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

        if self.base_path == RemoteBases.trf:
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
