import unittest

import warnings
import httpretty
from queryset_manager import remotes, models

remotes = remotes.Api(source_url = "http://src")

class TestRemotes(unittest.TestCase):
    def setUp(self):
        warnings.filterwarnings("ignore", category=ResourceWarning, message="unclosed.*")

    @httpretty.activate
    def test_prime_queryset(self):
        httpretty.register_uri(httpretty.GET,
                "http://src/priogrid_month/trf/bar/baz",
                status=202)

        test_queryset = models.Queryset(
                name="My qs",
                level_of_analysis = models.LevelOfAnalysis(name = "priogrid_month"),
                operation_roots = [
                    models.Operation(
                        namespace = models.RemoteNamespaces("trf"),
                        name = "bar",
                        arguments = ["baz"])
                ]
            )

        self.assertFalse(remotes.prime_queryset(test_queryset))
