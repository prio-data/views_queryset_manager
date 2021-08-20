"""
Tests for error handling, which is done using pymonad.
"""
import unittest
from pymonad.either import Left,Right
from toolz.functoolz import identity
from queryset_manager.retrieval import lefts, rights, distinct_names

class TestErrorHandling(unittest.TestCase):
    def test_combine_either(self):
        x = [Left(1), Left(1), Right(1)]
        self.assertEqual(len(lefts(x)),2)
        self.assertEqual(len(rights(x)),1)


class TestStringHandling(unittest.TestCase):
    def test_concat_distinct(self):
        x = ["a","a","a"]
        self.assertEqual(len(set(distinct_names(x))), 3)

