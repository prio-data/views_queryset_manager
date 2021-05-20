import unittest

import warnings
import httpretty
from queryset_manager import remotes, models

remotes = remotes.Remotes(source_url = "http://src")

class TestRemotes(unittest.TestCase):
    def setUp(self):
        warnings.filterwarnings("ignore", category=ResourceWarning, message="unclosed.*")

    @httpretty.activate
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
