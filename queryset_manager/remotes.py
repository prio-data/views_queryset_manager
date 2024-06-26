"""
Operations that access remote services.
"""
import os
import logging
from typing import Optional
from io import BytesIO
from datetime import date

import pandas as pd
import requests
from requests.exceptions import HTTPError

from . import models
from . import ops

logger = logging.getLogger(__name__)

class OperationPending(Exception):
    pass

class Api():
    def __init__(self,source_url):
        self.source_url = source_url

    def fetch_data_for_queryset(self, queryset: models.Queryset,
            start_date:Optional[date]=None,end_date:Optional[date]=None)->pd.DataFrame:
        """
        Retrieves data corresponding to a queryset with subsetting, if it is ready (cached).
        """

        try:
            assert self.prime_queryset(queryset)
        except AssertionError as ae:
            raise OperationPending from ae

        dataset = self.retrieve_data(queryset,start_date,end_date)
        return dataset

    def prime_queryset(self, queryset: models.Queryset)->bool:
        """
        Primes a queryset, touching all resources needed for fullfilment.

        Returns True if a queryset is ready (all touch-requests return 200),
        or False if one or more return 202. Throws if a request returns anything
        else.
        """

        ready = True
        for path in queryset.paths():
            url = os.path.join(self.source_url,path)+"?touch=true"
            response = requests.get(url)
            if response.status_code == 202:
                ready &= False
            elif response.status_code == 200:
                pass
            else:
                raise HTTPError(response=response)
        return ready

    def retrieve_data(self, queryset: models.Queryset,
            start_date:Optional[date]=None,end_date:Optional[date]=None)->pd.DataFrame:

        dataset = None
        logger.info("Retrieving data for queryset %s",queryset.name)

        for path in queryset.paths():
            url = os.path.join(self.source_url, path)
            logger.debug("Fetching %s",url)
            response = requests.get(url)

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

