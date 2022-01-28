import logging
from typing import List
import pandas as pd
from pymonad.maybe import Maybe, Nothing, Just
from toolz.functoolz import compose, reduce, curry

logger = logging.getLogger(__name__)

def pandas_merge(dataframes: List[pd.DataFrame])-> Maybe[pd.DataFrame]:
    """
    pandas_merge
    ============

    parameters:
        dataframes (List[pd.DataFrame])
    returns:
        Maybe[pd.DataFrame]

    Inner merges a list of pandas dataframes using their indices.
    """
    try:
        return Just(reduce(lambda a,b: a.merge(b, left_index = True, right_index = True, how = "inner"), dataframes))
    except pd.errors.MergeError:
        return Nothing

def list_with_distinct_names(dfs: List[pd.DataFrame])-> List[pd.DataFrame]:
    seen = []
    for df in dfs:
        seen_before = len(seen)
        seen = distinct_names(seen + list(df.columns))
        df.columns = seen[seen_before:]
    return dfs

def distinguish_string(by, existing, new):
    if new in existing:
        return distinguish_string(by, existing, by(new))
    else:
        return new

def concat_distinct(by, existing, new):
    return existing + [distinguish_string(by, existing, new)]

distinct_names = lambda names: reduce(
        curry(concat_distinct, lambda s: "_"+s),
        names,
        [])

def ensure_index_names(dataframes: List[pd.DataFrame])-> List[pd.DataFrame]:
    """
    ensure_index_names
    ==================

    parameters:
        dataframes (List[pandas.DataFrame]): A list of doubly-indexed dataframes.
    returns:
        List[pandas.DataFrame]: A list of doubly-indexed dataframes with the same index names.

    This function is run before merging, to ensure that dataframes all share index names.
    This is done to smooth over some problems upstream.
    """

    all_names = {n for n in {tuple(df.index.names) for df in dataframes} if n != (None, None)}
    if all_names:
        names,*_ = all_names
    else:
        logger.warning("No index names found in list of dataframes, using fallback")
        names = ("TIME", "UNIT")

    for df in dataframes:
        df.index.names = names

    return dataframes

merge = compose(
            pandas_merge,
            list_with_distinct_names,
            ensure_index_names,
        )
