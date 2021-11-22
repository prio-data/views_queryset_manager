
from unittest import TestCase

import views_schema
from alchemy_mock.mocking import UnifiedAlchemyMagicMock

from queryset_manager import models

class TestModels(TestCase):
    """
    Tests for checking that database models match up with the views schema pydantic models.

    The ORM model is a bit more complicated than the pydantic models, since it
    needs to keep track of an ordered list (not trivial in SQL).  The way I
    solved this was by using a linked list approach, where each Operation
    object points to the next one, or None.

    This means that it's important to test basic serialization /
    deserialization, since this conversion from list to LL is non-trivial.
    """
    def setUp(self):
        self.sess = UnifiedAlchemyMagicMock()

    def test_operation_chaining(self):
        """
        Test operation chaining when creating ORM representation.
        """
        posted_model = views_schema.Queryset(
                name        = "my_test_queryset",
                loa         = "priogrid_month",
                themes      = ["foo","bar"],
                description = "A description...",
                operations = [
                    [
                        views_schema.TransformOperation(name = "my.transform",arguments = []),
                        views_schema.DatabaseOperation(name = "table.column",arguments = ["values"]),
                    ]
                ]
            )

        orm_model = models.Queryset.from_pydantic(self.sess, posted_model)

        self.assertEqual(len(orm_model.op_chains()),1)
        self.assertEqual(len(orm_model.op_chains()[0]),2)

    def test_serialize_deserialize(self):
        """
        Test serialization back and forth from ORM for equivalence
        """
        pydantic_model = views_schema.Queryset(
                name        = "pydantic_queryset",
                loa         = "priogrid_month",
                themes      = ["a","b"],
                description = "My great description",
                operations = [
                    [
                        views_schema.TransformOperation(name = "some.transform", arguments = []),
                        views_schema.DatabaseOperation(name = "t.c", arguments = ["values"])
                    ],
                    [
                        views_schema.DatabaseOperation(name = "t.c", arguments = ["values"])
                    ],
                ]
            )

        orm_model = models.Queryset.from_pydantic(self.sess, pydantic_model)

        reserialized = views_schema.Queryset(**orm_model.dict())
        self.assertEqual(reserialized,pydantic_model)

    def test_roundtrip(self):
        pydantic_model = views_schema.Queryset(
                name        = "send_me_to_db",
                loa         = "country_month",
                themes      = [":)"],
                description = "This is a queryset used for testing.",
                operations = [
                    [
                        views_schema.TransformOperation(
                            name      = "alpha.beta",
                            arguments = ["1","2","3"]),
                        views_schema.DatabaseOperation(
                            name      = "my_table.my_column",
                            arguments = ["values"])
                    ],
                    [
                        views_schema.DatabaseOperation(
                            name      = "another_table.something",
                            arguments = ["values"])
                    ],
                ]
            )

        self.sess.add(models.Queryset.from_pydantic(self.sess,pydantic_model))
        retrieved = self.sess.query(models.Queryset).first()
        reserialized = views_schema.Queryset(**retrieved.dict())
        self.assertEqual(reserialized,pydantic_model)
