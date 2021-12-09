
import unittest
from typing import Tuple
import pandas as pd
import numpy as np
from queryset_manager import retrieval

def midx_df(names: Tuple[str, str])-> pd.DataFrame:
    return pd.DataFrame(np.zeros(9), index = pd.MultiIndex.from_product((range(3), range(3)), names = names))

class TestRetrievalFixes(unittest.TestCase):
    def test_retrieval_fixes(self):
        dataframes = [midx_df(("a", "b")) for _ in range(5)]
        dataframes += [midx_df((None, None))]
        dataframes = retrieval.ensure_index_names(dataframes)
        self.assertEqual({tuple(df.index.names) for df in dataframes}, {("a", "b")})

        dataframes += [midx_df(("b", "c"))]
        dataframes = retrieval.ensure_index_names(dataframes)
        self.assertEqual(len({tuple(df.index.names) for df in dataframes}), 1)
