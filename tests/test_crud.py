
from unittest import TestCase
from alchemy_mock.mocking import UnifiedAlchemyMagicMock
import views_schema

from queryset_manager import crud,models

class TestCrud(TestCase):
    def setUp(self):
        self.sess = UnifiedAlchemyMagicMock()

    def test_simple_create(self):
        mock_qs = views_schema.Queryset(name = "foobar", loa = "priogrid_month", operations = [])
        crud.create_queryset(self.sess,mock_qs)
        result = self.sess.query(models.Queryset).all()
        self.assertEqual(len(result),1)

    def test_create(self):
        queryset = views_schema.Queryset(
                name = "my_queryset",
                loa = "country_month",
                themes = ["my_theme","my_other_theme"],
                operations = [
                        [
                            views_schema.Operation(
                                namespace = "trf",
                                name="operation.my_transform",
                                arguments=[10],
                                ),
                            views_schema.Operation(
                                namespace = "base",
                                name = "priogrid_month.my_variable",
                                arguments = ["max"]
                                )
                        ],
                        [
                            views_schema.Operation(
                                namespace = "base",
                                name = "country_month.my_variable",
                                arguments = ["values"]
                                )
                        ],
                    ]
                )

        crud.create_queryset(self.sess,queryset)
        result = self.sess.query(models.Queryset).all()
        self.assertEqual(len(result),1)
