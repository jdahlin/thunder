from thunder.testutils import StoreTest


class TestHooks(StoreTest):
    def testLoaded(self):
        class Document(object):
            def __init__(self):
                self.loaded = False

            def __thunder_loaded__(self):
                self.loaded = True

        d = Document()
        self.failIf(d.loaded)
        self.store.add(d)
        self.failIf(d.loaded)
        self.store.flush()
        self.failIf(d.loaded)
        self.store.drop_cache()
        self.failIf(d.loaded)
        self.assertOp(Document, name='save')

        self.failIf(d.loaded)
        d = self.store.find_one(Document)
        self.failUnless(d.loaded)
        self.assertOp(Document, name='find_one')

    def testPreflush(self):
        class Document(object):
            def __init__(self):
                self.flushed = False

            def __thunder_pre_flush__(self):
                self.flushed = True
                if hasattr(self, '_id'):
                    raise AssertionError()
        d = Document()
        self.failIf(d.flushed)
        self.store.add(d)
        self.failIf(d.flushed)
        self.store.flush()
        self.failUnless(d.flushed)
        self.assertOp(Document, name='save')

    def testFlushed(self):
        class Document(object):
            def __init__(self):
                self.flushed = False

            def __thunder_flushed__(self):
                self.flushed = True
                if self._id is None:
                    raise AssertionError()
        d = Document()
        self.failIf(d.flushed)
        self.store.add(d)
        self.failIf(d.flushed)
        self.store.flush()
        self.failUnless(d.flushed)
        self.assertOp(Document, name='save')
