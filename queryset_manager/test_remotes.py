
import unittest
from unittest import mock
import re

import httpretty

from . import remotes,models

@httpretty.httprettified
class TestRemotes(unittest.TestCase):
    @mock.patch("queryset_manager.settings.SOURCE_URL","http://src")
    def test_prime_queryset(self):
        httpretty.register_uri(httpretty.GET,
                "http://src/priogrid_month/foo/bar/baz",
                status=202)
        test_queryset = models.Queryset(
                name="My qs",
                loa="priogrid_month",
                op_roots = [models.Operation(base_path="foo",path="bar",args=["baz"])]
            )
        self.assertFalse(remotes.prime_queryset(test_queryset))
