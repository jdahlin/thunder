import collections
import time


Op = collections.namedtuple('Op', ['name', 'args', 'kwargs', 'time'])


class TraceCollection(object):
    def __init__(self, collection):
        self.collection = collection
        self.ops = []

    def find(self, *args, **kwargs):
        start = time.time()
        retval = self.collection.find(*args, **kwargs)
        end = time.time()
        self.ops.append(Op('find', args, kwargs, end - start))
        return retval

    def find_one(self, *args, **kwargs):
        start = time.time()
        retval = self.collection.find_one(*args, **kwargs)
        end = time.time()
        self.ops.append(Op('find_one', args, kwargs, end - start))
        return retval

    def count(self, *args, **kwargs):
        start = time.time()
        retval = self.collection.count(*args, **kwargs)
        end = time.time()
        self.ops.append(Op('count', args, kwargs, end - start))
        return retval

    def save(self, *args, **kwargs):
        start = time.time()
        retval = self.collection.save(*args, **kwargs)
        end = time.time()
        self.ops.append(Op('save', args, kwargs, end - start))
        return retval

    def remove(self, *args, **kwargs):
        start = time.time()
        retval = self.collection.remove(*args, **kwargs)
        end = time.time()
        self.ops.append(Op('remove', args, kwargs, end - start))
        return retval
