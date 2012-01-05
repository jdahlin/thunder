=======
Thunder
=======

Presentation
------------
Thunder is an ORM for MongoDB_ built on top of the PyMongo driver.

It's inspired by Storm_ and reuses some APIs from mongoengine_.

Tutorial
--------

Thunder provides a Store class which provide apis for indexing and querying documents stored in MongoDB.

>>> from storm.store import Store

To create a simple store you can just provide the name of the database inside the mongo database:

>>> store = Store('mydb')

That's it, we now have a reference to store which can be used to insert and query objects.

First we need to define a document that can be inserted into a store:

>>> from storm.fields import ObjectIdField, StringField

>>> class Person(object):
...     id = ObjectIdField()
...     name = StringField()

All objects inserted into a store needs to at least have an ObjectIdField. There can only be one ObjectIdField
per class.

To create a document you create the class normally:

>>> p = Person()
>>> p.name = 'John Cleese'
>>> store.add(p)

Note that this doesn't actually index the document into the database, that's only done when an explicit flush
is issued:

>>> store.find(Person).count()
0

>>> store.flush()

We can now query the store for the document:

>>> store.find(Person).count()
1

>>> for person in store.find(Person):
>>>     print person
<X object at 0x.....>

.. _MongoDB: http://www.mongodb.org/
.. _Storm: http://storm.canonical.com/
.. _mongoengine: http://www.mongoengine.org/


