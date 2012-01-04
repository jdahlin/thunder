import os
import collections
import time


Op = collections.namedtuple('Op', ['name', 'args', 'kwargs', 'time'])


THUNDER_DEBUG = os.environ.get('THUNDER_DEBUG') == '1'


class TraceCollection(object):
    def __init__(self, collection):
        self.collection = collection
        self.ops = []

    def add(self, op):
        if THUNDER_DEBUG:  # pragma nocoverage
            s = []
            for arg, value in op.kwargs.items():
                s.append('%s=%r' % (arg, value))
            args = []
            args.append(repr(op.args))
            args.extend(s)
            print '%.6f %-.2f msec: %s%s' % (
                time.time(), op.time * 1000, op.name, ', '.join(args))
        self.ops.append(op)

    def find(self, *args, **kwargs):
        start = time.time()
        retval = self.collection.find(*args, **kwargs)
        end = time.time()
        self.add(Op('find', args, kwargs, end - start))
        return retval

    def find_one(self, *args, **kwargs):
        start = time.time()
        retval = self.collection.find_one(*args, **kwargs)
        end = time.time()
        self.add(Op('find_one', args, kwargs, end - start))
        return retval

    def count(self, *args, **kwargs):
        start = time.time()
        retval = self.collection.count(*args, **kwargs)
        end = time.time()
        self.add(Op('count', args, kwargs, end - start))
        return retval

    def save(self, *args, **kwargs):
        start = time.time()
        retval = self.collection.save(*args, **kwargs)
        end = time.time()
        self.add(Op('save', args, kwargs, end - start))
        return retval

    def remove(self, *args, **kwargs):
        start = time.time()
        retval = self.collection.remove(*args, **kwargs)
        end = time.time()
        self.add(Op('remove', args, kwargs, end - start))
        return retval
