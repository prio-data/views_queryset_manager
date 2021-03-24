
import os
import logging
from io import BytesIO
from typing import List,Optional
from datetime import date

import pandas as pd
import requests
from requests.exceptions import HTTPError

from models import Queryset,Operation
import settings
import ops

logger = logging.getLogger(__name__)

class OperationPending(Exception):
    pass

def is_ready(queryset:Queryset)->bool:
    """
    Returns True if a queryset is ready (all touch-requests return 200),
    or False if one or more return 202. Throws if a request returns anything
    else.
    """

    ready = True
    for path in queryset.paths():
        response = requests.get(os.path.join(settings.SOURCE_URL,path)+"?touch=true")
        if response.status_code == 202:
            ready &= False
        elif response.status_code == 200:
            pass
        else:
            raise HTTPError(response=response)
    return ready 

def retrieve_data(queryset:Queryset,
        start_date:Optional[date]=None,end_date:Optional[date]=None)->pd.DataFrame:

    dataset = None
    logger.info("Retrieving data for queryset %s",queryset.name)

    for path in queryset.paths():
        logger.debug("Fetching %s",path)
        response = requests.get(os.path.join(settings.SOURCE_URL,path))

        if response.status_code == 200:
            try:
                data = pd.read_parquet(BytesIO(response.content))
            except OSError as ose:
                logger.error("Failed to deserialize data from %s",path)
                raise ose

            if start_date or end_date:
                data = ops.temp_subset(data,start_date,end_date)

            if dataset is not None:
                logger.info("Joining data with %s",path)
                dataset = ops.join(dataset,data)
            else:
                dataset = data
        else:
            raise requests.HTTPError(response=response)

    return dataset

def link_ops(operations:List[Operation])->Operation:
    # This is silly
    operations.reverse()
    prev = operations.pop()
    first = prev
    operations.reverse()

    for op in operations:
        op.previous_op = [prev]
        prev = op
    return first
