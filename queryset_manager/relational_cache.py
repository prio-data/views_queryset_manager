from typing import List
from contextlib import closing
import datetime
import logging
import pandas as pd
from pymonad.maybe import Just, Nothing, Maybe
from toolz.functoolz import curry
from sqlalchemy.engine import Engine, Connection
from sqlalchemy import MetaData, Table, select, text

logger = logging.getLogger(__name__)

class RelationalCache():
    """
    Caches queryset data in an SQL database, for fast subsequent retrieval.
    """

    def __init__(self, engine: Engine):
        self.engine = engine
        self.metadata: MetaData = MetaData()

    @property
    def tables(self)-> List[str]:
        return self.engine.table_names()

    def table(self, name) -> Maybe[Table]:
        if self.exists(name):
            return Just(Table(name, self.metadata, autoload_with = self.engine))
        else:
            return Nothing

    def purge(self):
        with closing(self.con()) as con:
            for table in self.tables:
                self._clear(con, table)

    def _clear(self, con, name):
        logger.warning("Clearing %s from cache", name)
        con.execute(f"drop table {name}")

    def clear(self, name):
        with closing(self.con()) as con:
            self._clear(con, name)

    def con(self):
        return self.engine.connect()

    def _add_primary_keys(self, con: Connection, name: str, data: pd.DataFrame)-> None:
        time, unit = data.index.names
        con.execute(
                text("ALTER TABLE :name ADD PRIMARY KEY (:time, :unit)"),
                name = name, time = time, unit = unit)

    def store(self, name: str, data: pd.DataFrame):
        with closing(self.con()) as con:
            data.to_sql(name, con, if_exists = "replace", index = True)
            #self._add_primary_keys(con, name, data)

        logger.warning("Cached %s", name)
        return data

    def exists(self, name):
        return name in self.tables

    def temporal_subset(self,
            start_date: datetime.date,
            end_date: datetime.date,
            data: pd.DataFrame)-> pd.DataFrame:
        return data

    def fetch(self, name, start_date, end_date) -> Maybe[pd.DataFrame]:
        with closing(self.con()) as con:
            return (self.table(name)
                    .then(lambda table: pd.read_sql(str(select("*").select_from(table)), con))
                    .then(curry(self.temporal_subset, start_date, end_date))
                    )
