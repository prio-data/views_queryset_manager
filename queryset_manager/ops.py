import sys
from datetime import datetime,date
import logging
from typing import Optional

from dateutil.relativedelta import relativedelta

import pandas as pd

import constants

logger = logging.getLogger(__name__)

secdelta = lambda time: (datetime.now()-time).seconds

def join(*dataframes):
    """
    Joins two dataframes to the lowest common set of TIME indices, by concatenation.
    Super fast, but relies on two very important assumptions:
    TIME is a continuous series of indices
    UNIT has the same number of unique values for each TIME
    """

    start = 0
    end = sys.maxsize

    for df in dataframes:
        logger.debug("Processing a DF")
        mark = datetime.now()
        if not df.index.is_monotonic:
            logger.debug("Sorting...")
            df.sort_index(inplace=True)
            logger.debug("Sorted (%s)",secdelta(mark))

        logger.debug("Getting bounds")
        first = df.index[0]
        logger.debug("Got first (%s)",secdelta(mark))
        last = df.index[-1]
        logger.debug("Got last (%s)",secdelta(mark))
        this_start,this_end = (t for t,_ in (first,last))
        logger.debug("Got bounds (%s)",secdelta(mark))

        start = max(start,this_start)
        end = min(end,this_end)

    dataframes = [df.loc[start:end,:] for df in dataframes]
    logger.debug("Subset dataframes (%s)",secdelta(mark))

    joined = pd.concat(dataframes,axis=1)
    logger.info("Joined %s dataframes in %s seconds",len(dataframes),secdelta(mark))

    return joined 

def date_from_base(from_date:Optional[date],base)->int:
    if from_date:
        d = relativedelta(from_date,base)
        return d.months + (d.years * 12)
    else:
        return None

def temp_subset(dataframe:pd.DataFrame,start_date:Optional[date],end_date:Optional[date]):
    start,end = (date_from_base(date,constants.BASE_DATE) for date in (start_date,end_date))
    return dataframe.loc[start:end,:]
