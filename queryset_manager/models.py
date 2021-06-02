import os
import enum
from typing import List
from sqlalchemy import Column,String,Enum,Integer,ForeignKey,JSON,MetaData,Table
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship,validates

metadata = MetaData()
Base = declarative_base(metadata=metadata)

querysets_themes = Table("querysets_themes", Base.metadata,
        Column("theme_name", String, ForeignKey("theme.name")),
        Column("queryset_name", String, ForeignKey("queryset.name"))
    )

class RemoteNamespaces(enum.Enum):
    """
    An enum representing the available alternatives for remote namespaces.
    """
    trf="trf"
    base="base"

class RemoteLOAs(enum.Enum):
    """
    An enum representing the available levels of analysis.
    """
    priogrid_month="priogrid_month"
    country_month = "country_month"

class Theme(Base):
    __tablename__ = "theme"

    name = Column(String,primary_key=True)
    description = Column(String, nullable=True)

    querysets = relationship("Queryset",
            secondary = querysets_themes,
            back_populates = "themes"
            )

    def path(self):
        return "theme/"+self.name

    def __repr__(self):
        return f"{self.name} ({len(self.querysets)} querysets)"

    @classmethod
    def get_or_create(cls, session, identifier, identifier_name = "name"):
        existing = session.query(cls).get(identifier)

        if existing is not None:
            return existing

        return cls(**{identifier_name: identifier})

class Queryset(Base):
    __tablename__ = "queryset"

    name = Column(String,primary_key=True)
    loa = Column(Enum(RemoteLOAs),nullable=False)

    description = Column(String, nullable = True)

    themes = relationship("Theme",
            secondary = querysets_themes,
            back_populates = "querysets",
            )

    operation_roots = relationship(
            "Operation",
            cascade="all,delete-orphan"
            )

    @classmethod
    def from_pydantic(cls, session, queryset_model):
        queryset = cls(
                name = queryset_model.name,
                loa = RemoteLOAs(queryset_model.loa),
                description = queryset_model.description,
                themes = [Theme.get_or_create(session,th) for th in queryset_model.themes]
            )

        for chain in queryset_model.operations:
            root = chain_operations([Operation.from_pydantic(op) for op in chain])
            queryset.operation_roots.append(root)
        return queryset

    def paths(self):
        try:
            loa = self.loa.value
        except AttributeError:
            loa = str(self.loa)
        return [os.path.join(loa,op.operation_chain_path()) for op in self.operation_roots]

    def op_chains(self):
        return [op.get_chain() for op in self.operation_roots]

    def dict(self):
        return {
            "name": self.name,
            "loa": self.loa.value,
            "description": self.description,
            "themes": [th.name for th in self.themes],
            "operations": [[op.dict() for op in ch] for ch in self.op_chains()]
        }

    def path(self):
        return "queryset/"+self.name

class Operation(Base):
    """
    An op is an edge in a chain, corresponding to a remote path that represents
    a series of operations.
    """

    @classmethod
    def from_pydantic(cls,pydantic_model):
        d = pydantic_model.dict()
        d["namespace"] = RemoteNamespaces(d["namespace"])
        return cls(**d)

    __tablename__ = "operation"

    operation_id = Column(Integer,primary_key=True)

    next_operation_id = Column(Integer,ForeignKey("operation.operation_id"))
    next_operation = relationship(
            "Operation",
            backref = "previous_operation",
            remote_side = [operation_id],
            cascade = "all,delete"
        )

    queryset_name = Column(String,ForeignKey("queryset.name"))

    namespace = Column(Enum(RemoteNamespaces),nullable=False)
    name = Column(String,nullable=False)
    arguments = Column(JSON,)

    def dict(self):
        return {
            "namespace": self.namespace.value,
            "name": self.name,
            "arguments": self.arguments,
            }

    def operation_path(self):
        """
        Show operation as path
        """
        components = [self.namespace.value, self.name]

        if not self.arguments:
            args = ["_"]
        else:
            args = self.arguments
        components.append("_".join([str(a) for a in args]))

        return os.path.join(*components)

    def get_chain(self,previous=None):
        if previous is None:
            previous = list()
        previous.append(self)
        if self.next_operation:
            return self.next_operation.get_chain(previous=previous)
        return previous

    def operation_chain_path(self):
        return os.path.join(*[op.operation_path() for op in self.get_chain()])

    @validates("next_operation")
    def validate_next_operation(self,_,next_operation):
        """
        Make sure that terminal operations do not have subsequent operations.
        """
        try:
            assert self.namespace is not RemoteNamespaces.base and next_operation is not None
        except AssertionError as ae:
            raise ValueError(
                    "Operation of namespace base cannot have a subsequent operation"
                    ) from ae
        return next_operation

    @validates("arguments")
    def validate_arguments(self,_,arguments):
        try:
            assert isinstance(arguments,list)
        except AssertionError as ae:
            raise ValueError("Arguments must be a list of values") from ae
        try:
            assert all((isinstance(a,(int,str)) for a in arguments))
        except AssertionError as ae:
            raise ValueError("Arguments must be either str or int") from ae
        return arguments

    def __repr__(self):
        return f"Operation(namespace={self.namespace.value}, name={self.name})"

def chain_operations(operations: List[Operation])-> Operation:
    """
    Chains a list of operations together, returning the "root" operation that
    points to the next, and so on.
    """
    first,*operations = operations

    prev = first
    for op in operations:
        op.previous_operation = [prev]
        prev = op

    return first
