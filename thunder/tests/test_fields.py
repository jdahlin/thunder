import datetime
import decimal
import unittest

from thunder.exceptions import ValidationError
from thunder.fields import (DateTimeField, DecimalField,
                            ObjectIdField,
                            Field, IntField)
from thunder.testutils import StoreTest


class DateTimeFieldTest(StoreTest):
    def testDatetime(self):
        class DateTimeDocument(object):
            mid = ObjectIdField()
            date = DateTimeField()
        collection = self.getCollection(DateTimeDocument)

        # MongoDB has a precision (milliseconds) lower than
        # the datetime module, so create one MongoDB can handle
        d = DateTimeDocument()
        d.date = old = datetime.datetime(2012, 1, 1, 12,
                                         34, 56, 789000)
        self.store.add(d)
        self.store.flush()
        self.store.drop_cache()

        self.assertEquals(collection.ops.pop().name, 'save')

        d = list(self.store.find(DateTimeDocument))[0]
        self.assertEquals(d.date, old)

        self.assertEquals(collection.ops.pop().name, 'find')


class DecimalFieldTest(StoreTest):
    def testDatetime(self):
        class DecimalDocument(object):
            mid = ObjectIdField()
            dec = DecimalField(precision_check=True)
        collection = self.getCollection(DecimalDocument)

        d = DecimalDocument()
        d.dec = old = decimal.Decimal("12.45")
        self.store.add(d)
        self.store.flush()
        self.store.drop_cache()

        self.assertEquals(collection.ops.pop().name, 'save')

        d = list(self.store.find(DecimalDocument))[0]
        self.assertEquals(d.dec, old)

        self.store.drop_cache()
        self.assertRaises(
            ValidationError, setattr, d, 'dec', decimal.Decimal("12.345678"))

        self.assertEquals(collection.ops.pop().name, 'find')


class TestField(unittest.TestCase):
    def test__get__(self):
        class Document(object):
            i = ObjectIdField()
            f = Field()

        d = Document()
        self.assertEquals(d.f, None)
        d.f = 1
        self.assertEquals(d.f, 1)
        self.failUnless(Document.f)


class TestIntField(unittest.TestCase):
    def testSimple(self):
        class Document(object):
            i = ObjectIdField()
            int = IntField()

        d = Document()
        self.assertEquals(d.int, None)
        d.int = 1
        self.assertEquals(d.int, 1)
