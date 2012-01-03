import unittest

from thunder.fields import StringField, ObjectIdField
from thunder.info import get_cls_info
from thunder.store import Store


class Person(object):
    __thunder_doc__ = 'Person'
    id = ObjectIdField()
    name = StringField()
    full_name = StringField()


class SPPerson(object):
    __thunder_doc__ = 'Person'
    id = ObjectIdField()
    name = StringField()


class TestStore(unittest.TestCase):
    def setUp(self):
        self.store = Store('localhost', 'thunder-test')

    def _get_collection(self, cls):
        return get_cls_info(cls).get_collection(self.store)

    def testSimple(self):
        p = Person()
        p.name = "John"
        p.full_name = "Jonathan Doe"
        self.store.add(p)
        self.store.flush()

        np = self.store.get(Person, p.id)
        self.assertEquals(np.full_name, "Jonathan Doe")

        sp = self.store.get(SPPerson, p.id)
        self.assertRaises(AttributeError, getattr, sp, 'full_name')

    def testUpdate(self):
        p = Person()
        p.name = "John"
        p.full_name = "Jonathan Doe"
        self.store.add(p)
        self.store.flush()

        p = self.store.get(Person, p.id)
        self.failUnless(p)
        p.name = 'Foo'
        old_id = p.id
        self.store.flush()
        self.assertEquals(p.id, old_id)

    def testRemove(self):
        collection = self._get_collection(Person)

        p = Person()
        p.name = "John"
        p.full_name = "Jonathan Doe"
        self.store.add(p)
        self.assertEquals(collection.count(), 0)
        self.store.flush()
        self.assertEquals(collection.count(), 1)

        p = self.store.get(Person, p.id)
        obj_id = p.id
        self.failUnless(p)
        self.store.remove(p)
        self.assertEquals(collection.count(), 1)
        self.store.flush()
        self.assertEquals(collection.count(), 0)

        self.assertRaises(Exception, self.store.remove, p)
        self.failIf(self.store.get(Person, obj_id))
        self.assertEquals(p.id, obj_id)
        self.assertEquals(p.name, "John")

    def testFind(self):
        collection = self._get_collection(Person)

        p = Person()
        p.name = "John"
        p.full_name = "Jonathan Doe"
        self.store.add(p)

        p = Person()
        p.name = "Jane"
        p.full_name = "Jane Doe"
        self.store.add(p)

        self.assertEquals(collection.count(), 0)
        self.store.flush()
        self.assertEquals(collection.count(), 2)

        results = list(self.store.find(Person))
        self.assertEquals(len(results), 2)
        self.failUnless(isinstance(results[0], Person))
        self.failUnless(isinstance(results[1], Person))

    def testFindArgs(self):
        p = Person()
        p.name = "John"
        p.full_name = "Jonathan Doe"
        self.store.add(p)

        results = list(self.store.find(Person, { 'name' : 'John' }))
        self.assertEquals(len(results), 0)
        self.store.flush()
        results = list(self.store.find(Person, { 'name' : 'John' }))
        self.assertEquals(len(results), 1)

        self.failUnless(isinstance(results[0], Person))
        self.assertEquals(results[0].name, p.name)
        self.assertEquals(results[0].full_name, p.full_name)

    def testFindOne(self):
        p = Person()
        p.name = "John"
        p.full_name = "Jonathan Doe"
        self.store.add(p)

        p1 = self.store.find_one(Person, { 'name' : 'John' })
        self.failIf(p1)

        self.store.flush()
        p1 = self.store.find_one(Person, { 'name' : 'John' })
        self.failUnless(p1)

        self.failUnless(isinstance(p1, Person))
        self.assertEquals(p1.name, p.name)
        self.assertEquals(p1.full_name, p.full_name)

    def testCount(self):
        p = Person()
        p.name = "John"
        p.full_name = "Jonathan Doe"

        self.assertEquals(self.store.count(Person), 0)
        self.store.add(p)
        self.assertEquals(self.store.count(Person), 0)
        self.store.flush()
        self.assertEquals(self.store.count(Person), 1)
        self.store.remove(p)
        self.assertEquals(self.store.count(Person), 1)
        self.store.flush()
        self.assertEquals(self.store.count(Person), 0)

    def tearDown(self):
        self.store.drop_collections()
