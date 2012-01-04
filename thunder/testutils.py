import unittest

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
