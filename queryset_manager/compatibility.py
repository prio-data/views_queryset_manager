"""
This module contains functions related to maintaining compatbility with legacy
code.  This functionality should ideally not be depended upon.
"""
import pandas as pd

def with_index_names(loa: str, df: pd.DataFrame)-> pd.DataFrame:
    """
    Legacy code expects named indices. This function just adds names to indices, with
    the default names being time - unit.
    """

    index_names = {
            "priogrid_month": ("month_id", "pg_id"),
            "country_month": ("month_id", "country_id"),
        }

    try:
        names = index_names[loa.value.lower()]
    except KeyError:
        names = ("time","unit")
    df.index.names = names
    return df
