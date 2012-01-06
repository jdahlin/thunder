from thunder.fields import ObjectIdField, StringField
from thunder.store import Store
from thunder.testutils import StoreTest


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

        self.failUnless(hasattr(p, '_id'))

        op = collection.ops.pop()
        self.assertEquals(op.name, 'save')
        self.failUnless(op.args)
        self.assertEquals(op.kwargs, {})

        self.store.drop_cache()
        np = self.store.get(Person, p.id)

        self.failUnless(hasattr(np, '_id'))
        self.assertEquals(np.full_name, "Jonathan Doe")

        op = collection.ops.pop()
        self.assertEquals(op.name, 'find')
        self.assertEquals(op.kwargs, dict(fields=['id', 'full_name', 'name'],
                                          limit=2))
        self.assertEquals(op.args, ({'_id': np.id},))

        self.store.drop_cache()
        sp = self.store.get(SPPerson, p.id)

        self.assertRaises(AttributeError, getattr, sp, 'full_name')
        self.failUnless(hasattr(sp, '_id'))

        collection = self.getCollection(SPPerson)
        op = collection.ops.pop()
        self.assertEquals(op.name, 'find')
        self.assertEquals(op.kwargs, dict(fields=['id', 'name'],
                                          limit=2))
        self.failUnless(op.args)

    def testUpdate(self):
        class Person(object):
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
        self.failUnless(p)
        p.name = 'Foo'
        old_id = p.id

        self.store.flush()
        self.assertEquals(p.id, old_id)
        self.assertOp(Person, name='save')

    def testRemove(self):
        class Person(object):
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

    def testFindBy(self):
        class Person(object):
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

        results = list(self.store.find_by(Person, name='Jane'))
        self.assertOp(Person, name='find')
        self.assertEquals(len(results), 1)
        self.failUnless(isinstance(results[0], Person))

    def testFindArgs(self):
        class Person(object):
            id = ObjectIdField()
            name = StringField()
            full_name = StringField()

        p = Person()
        p.name = "John"
        p.full_name = "Jonathan Doe"
        self.store.add(p)

        results = list(self.store.find(Person, {'name': 'John'}))
        self.assertOp(Person, name='find')
        self.assertEquals(len(results), 0)
        self.store.flush()
        self.assertOp(Person, name='save')
        results = list(self.store.find(Person, {'name': 'John'}))
        self.assertOp(Person, name='find')
        self.assertEquals(len(results), 1)

        self.failUnless(isinstance(results[0], Person))
        self.assertEquals(results[0].name, p.name)
        self.assertEquals(results[0].full_name, p.full_name)

    def testFindOne(self):
        class Person(object):
            id = ObjectIdField()
            name = StringField()
            full_name = StringField()

        p = Person()
        p.name = "John"
        p.full_name = "Jonathan Doe"

        self.store.add(p)

        p1 = self.store.find_one(Person, {'name': 'John'})
        self.failIf(p1)

        self.assertOp(Person, name='find_one')

        self.store.flush()
        self.assertOp(Person, name='save')

        p1 = self.store.find_one(Person, {'name': 'John'})
        self.failUnless(p1)

        self.assertOp(Person, name='find_one')

        self.failUnless(isinstance(p1, Person))
        self.assertEquals(p1.name, p.name)
        self.assertEquals(p1.full_name, p.full_name)

    def testFindOneBy(self):
        class Person(object):
            id = ObjectIdField()
            name = StringField()
            full_name = StringField()

        p = Person()
        p.name = "John"
        p.full_name = "Jonathan Doe"

        self.store.add(p)

        p1 = self.store.find_one_by(Person, name='John')
        self.failIf(p1)

        self.assertOp(Person, name='find_one')

        self.store.flush()
        self.assertOp(Person, name='save')

        p1 = self.store.find_one_by(Person, name='John')
        self.failUnless(p1)

        self.assertOp(Person, name='find_one')

        self.failUnless(isinstance(p1, Person))
        self.assertEquals(p1.name, p.name)
        self.assertEquals(p1.full_name, p.full_name)

    def testCount(self):
        class Person(object):
            id = ObjectIdField()
            name = StringField()
            full_name = StringField()

        p = Person()
        p.name = "John Count"
        p.full_name = "Jonathan Doe Count"

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

    def testDropCollection(self):
        class Document(object):
            i = ObjectIdField()
        s = Store('localhost', 'hurricane-test-2')
        d = Document()
        s.add(d)
        s.drop_collection(Document)
