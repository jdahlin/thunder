from thunder.exceptions import InvalidObject
from thunder.utils import TraceCollection


def get_obj_info(obj):
    try:
        return obj.__thunder_object_info__
    except AttributeError:
        obj_info = ObjectInfo(obj)
        obj.__thunder_object_info__ = obj_info
        return obj_info


def get_cls_info(cls):
    if "__thunder_class_info__" in cls.__dict__:
        # Can't use attribute access here, otherwise subclassing won't work.
        return cls.__dict__["__thunder_class_info__"]
    else:
        cls.__thunder_class_info__ = ClassInfo(cls)
        return cls.__thunder_class_info__


class ClassInfo(object):
    def __init__(self, cls):
        self.cls = cls
        self.doc_name = cls.__dict__.get('__thunder_doc__', cls.__name__)
        self.collection = None

        # FIXME: move this some place.
        from thunder.fields import Field
        pairs = []
        for attr in dir(cls):
            field = getattr(cls, attr, None)
            if not isinstance(field, Field):
                continue

            if isinstance(field, Field):
                if attr == '_id':  # pragma: nocoverage
                    raise InvalidObject(
                        "Cannot create a fields called '_id', it's reserved "
                        "to mongodb")
                pairs.append((attr, field))

        pairs.sort()

        self.fields = tuple(pair[1] for pair in pairs)
        self.attributes = dict(pairs)

    def get_collection(self, store):
        if not self.collection:
            collection = store.database[self.doc_name]
            if store.trace:
                collection = TraceCollection(collection)
            store.collections.append(collection)
            self.collection = collection
        return self.collection

    def __repr__(self):  # pragma: nocoverage
        return '<ClassInfo (%s, %s)>' % (self.cls.__name__,
                                         self.doc_name)


class ObjectInfo(object):
    def __init__(self, obj):
        self.obj = obj
        self.cls_info = get_cls_info(type(obj))
        self._options = {}
        self.variables = {}
        self.flush_pending = False

    def set(self, name, value):
        self._options[name] = value

    def get(self, name):
        return self._options.get(name)

    def delete(self, name):
        del self._options[name]

    @property
    def doc_name(self):  # pragma: nocoverage
        return self.cls_info.doc_name
