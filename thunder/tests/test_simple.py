import datetime
import decimal
import unittest

from thunder.exceptions import ValidationError
from thunder.fields import (DateTimeField, DecimalField,
                            ObjectIdField, StringField)
from thunder.info import get_cls_info
from thunder.store import Store



class StoreTest(unittest.TestCase):
    def setUp(self):
        self.store = Store('localhost', 'thunder-test')
        self.store.trace = True

    def tearDown(self):
        self.store.drop_collections()
        for collection in self.store.collections:
            ops = collection.ops
            collection.ops = []
            if ops:
                self.fail("%r still has %d ops: %r" % (
                    collection, len(ops), ops))

    def assertOp(self, cls, **kwargs):
        collection = self.getCollection(cls)
        op = collection.ops.pop()
        for attr, value in kwargs.items():
            op_value = getattr(op, attr)
            if op_value != value:
                self.fail("op %s: %r != %r" % (attr, op_value, value))

    def getCollection(self, cls):
        return get_cls_info(cls).get_collection(self.store)


class TestStore(StoreTest):

    def testSimple(self):
        class Person(object):
            __thunder_doc__ = 'Person'
            id = ObjectIdField()
            name = StringField()
            full_name = StringField()

        class SPPerson(object):
            __thunder_doc__ = 'Person'
            id = ObjectIdField()
            name = StringField()
        collection = self.getCollection(Person)

        p = Person()
        p.name = "John"
        p.full_name = "Jonathan Doe"
        self.store.add(p)
        self.store.flush()

        op = collection.ops.pop()
        self.assertEquals(op.name, 'save')
        self.failUnless(op.args)
        self.assertEquals(op.kwargs, {})

        np = self.store.get(Person, p.id)
        self.assertEquals(np.full_name, "Jonathan Doe")

        op = collection.ops.pop()
        self.assertEquals(op.name, 'find')
        self.assertEquals(op.kwargs, dict(fields=['id', 'full_name', 'name'],
                                          limit=2))
        self.assertEquals(op.args, ({'_id': np.id},))

        return

        sp = self.store.get(SPPerson, p.id)
        self.assertRaises(AttributeError, getattr, sp, 'full_name')

        op = collection.ops.pop()
        self.assertEquals(op.name, 'find')
        self.assertEquals(op.kwargs, dict(fields=['id', 'full_name', 'name'],
                                          limit=2))
        self.failUnless(op.args)

    def testUpdate(self):
        class Person(object):
            __thunder_doc__ = 'Person'
            id = ObjectIdField()
            name = StringField()
            full_name = StringField()

        p = Person()
        p.name = "John"
        p.full_name = "Jonathan Doe"
        self.store.add(p)
        self.store.flush()
        self.assertOp(Person, name='save')

        p = self.store.get(Person, p.id)
        self.assertOp(Person, name='find')
        self.failUnless(p)
        p.name = 'Foo'
        old_id = p.id
        self.store.flush()
        self.assertEquals(p.id, old_id)
        self.assertOp(Person, name='save')

    def testRemove(self):
        class Person(object):
            __thunder_doc__ = 'Person'
            id = ObjectIdField()
            name = StringField()
            full_name = StringField()

        collection = self.getCollection(Person)

        p = Person()
        p.name = "John"
        p.full_name = "Jonathan Doe"
        self.store.add(p)
        self.assertEquals(collection.count(), 0)
        self.assertOp(Person, name='count')
        self.store.flush()
        self.assertEquals(collection.count(), 1)
        self.assertOp(Person, name='count')
        self.assertOp(Person, name='save')

        p = self.store.get(Person, p.id)
        self.assertOp(Person, name='find')
        obj_id = p.id
        self.failUnless(p)
        self.store.remove(p)
        self.assertEquals(collection.count(), 1)
        self.assertOp(Person, name='count')
        self.store.flush()
        self.assertEquals(collection.count(), 0)
        self.assertOp(Person, name='count')
        self.assertOp(Person, name='remove')

        self.assertRaises(Exception, self.store.remove, p)
        self.failIf(self.store.get(Person, obj_id))
        self.assertOp(Person, name='find')
        self.assertEquals(p.id, obj_id)
        self.assertEquals(p.name, "John")

    def testFind(self):
        class Person(object):
            __thunder_doc__ = 'Person'
            id = ObjectIdField()
            name = StringField()
            full_name = StringField()

        collection = self.getCollection(Person)

        p = Person()
        p.name = "John"
        p.full_name = "Jonathan Doe"
        self.store.add(p)

        p = Person()
        p.name = "Jane"
        p.full_name = "Jane Doe"
        self.store.add(p)

        self.assertEquals(collection.count(), 0)
        self.assertOp(Person, name='count')
        self.store.flush()
        self.assertOp(Person, name='save')
        self.assertOp(Person, name='save')
        self.assertEquals(collection.count(), 2)
        self.assertOp(Person, name='count')

        results = list(self.store.find(Person))
        self.assertOp(Person, name='find')
        self.assertEquals(len(results), 2)
        self.failUnless(isinstance(results[0], Person))
        self.failUnless(isinstance(results[1], Person))

    def testFindArgs(self):
        class Person(object):
            __thunder_doc__ = 'Person'
            id = ObjectIdField()
            name = StringField()
            full_name = StringField()

        p = Person()
        p.name = "John"
        p.full_name = "Jonathan Doe"
        self.store.add(p)

        results = list(self.store.find(Person, { 'name' : 'John' }))
        self.assertOp(Person, name='find')
        self.assertEquals(len(results), 0)
        self.store.flush()
        self.assertOp(Person, name='save')
        results = list(self.store.find(Person, { 'name' : 'John' }))
        self.assertOp(Person, name='find')
        self.assertEquals(len(results), 1)

        self.failUnless(isinstance(results[0], Person))
        self.assertEquals(results[0].name, p.name)
        self.assertEquals(results[0].full_name, p.full_name)

    def testFindOne(self):
        class Person(object):
            __thunder_doc__ = 'Person'
            id = ObjectIdField()
            name = StringField()
            full_name = StringField()

        p = Person()
        p.name = "John"
        p.full_name = "Jonathan Doe"
        collection = self.getCollection(Person)

        self.store.add(p)

        p1 = self.store.find_one(Person, { 'name' : 'John' })
        self.failIf(p1)

        self.assertOp(Person, name='find_one')

        self.store.flush()
        self.assertOp(Person, name='save')

        p1 = self.store.find_one(Person, { 'name' : 'John' })
        self.failUnless(p1)

        self.assertOp(Person, name='find_one')

        self.failUnless(isinstance(p1, Person))
        self.assertEquals(p1.name, p.name)
        self.assertEquals(p1.full_name, p.full_name)

    def testCount(self):
        class Person(object):
            __thunder_doc__ = 'Person'
            id = ObjectIdField()
            name = StringField()
            full_name = StringField()

        p = Person()
        p.name = "John Count"
        p.full_name = "Jonathan Doe Count"
        collection = self.getCollection(Person)

        self.assertEquals(self.store.count(Person), 0)
        self.store.add(p)
        self.assertOp(Person, name='find')
        self.assertEquals(self.store.count(Person), 0)
        self.assertOp(Person, name='find')
        self.store.flush()
        self.assertOp(Person, name='save')

        self.assertEquals(self.store.count(Person), 1)
        self.assertOp(Person, name='find')
        self.store.remove(p)
        self.assertEquals(self.store.count(Person), 1)
        self.assertOp(Person, name='find')
        self.store.flush()
        self.assertOp(Person, name='remove')

        self.assertEquals(self.store.count(Person), 0)
        self.assertOp(Person, name='find')



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
