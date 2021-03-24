
import unittest
import string
from datetime import date
import pandas as pd
import numpy as np
from . import settings,ops

class TestJoin(unittest.TestCase):
    def test_join(self):
        a = pd.DataFrame(np.ones((9,9)))
        a.columns = list(string.ascii_letters[:9])
        a.index = pd.MultiIndex.from_product([(1,2,3),(1,2,3)])

        res = ops.join(a,a)
        self.assertEqual(res.shape[0],a.shape[0])
        self.assertEqual(res.shape[1],a.shape[1]*2)

        b = pd.DataFrame(np.ones((6,9)))
        b.columns = list(string.ascii_letters[-9:])
        b.index = pd.MultiIndex.from_product([(1,2),(1,2,3)])

        res = ops.join(a,b)
        self.assertNotEqual(a.shape[0],res.shape[0])
        self.assertEqual(res.shape[0],b.shape[0])

    def test_date_from_base(self):
        base = date(1979,12,1)
        cases = [
                (date(1980,1,1),1),
                (date(1980,12,1),12),
                (date(1981,1,1),13),
                (date(2000,1,1),241),
            ]
        for input_date,mid in cases:
            self.assertEqual(ops.date_from_base(input_date,base),mid)

    def test_temp_subset(self):
        self.assertEqual(settings.BASE_DATE,date(1979,12,1))
        a = pd.DataFrame(np.ones((100,9)))
        a.columns = list(string.ascii_letters[:9])
        a.index = pd.MultiIndex.from_product([range(100),range(1)])
        ss = ops.temp_subset(a,date(1980,1,1),date(1980,12,1))
        self.assertTrue(ss.index.is_monotonic)
        self.assertEqual(ss.shape[0],12)
        self.assertEqual(ss.index[0][0],1)
        self.assertEqual(ss.index[-1][0],12)

        ss2 = ops.temp_subset(a,None,end_date=date(1981,12,1))
        self.assertTrue(ss2.index.is_monotonic)
        self.assertEqual(ss2.index[0][0],0)
        self.assertEqual(ss2.index[-1][0],24)
