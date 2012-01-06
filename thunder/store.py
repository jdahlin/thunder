from pymongo import Connection
from pymongo.database import Database

from thunder.info import get_cls_info, get_obj_info


class Store(object):
    def __init__(self, conn_string, database, trace=False):
        if not conn_string.startswith('mongodb://'):
            conn_string = 'mongodb://' + conn_string
        self.connection = Connection(conn_string)
        self.database = Database(self.connection, database)
        self.trace = trace
        self.collections = []
        self.obj_infos = set()
        self._cache = {}

    def _load(self, cls_info, operation, *args, **kwargs):
        fields = kwargs.pop('fields', None)
        if not fields:
            fields = cls_info.attributes.keys()

        return operation(*args, fields=fields, **kwargs)

    def _build_doc(self, cls_info, doc):
        obj_id = doc["_id"]

        obj = self._cache.get((cls_info, obj_id))
        if obj is not None:
            return obj
        cls = cls_info.cls
        obj = cls.__new__(cls)

        obj_info = get_obj_info(obj)
        obj_info.set("store", self)

        for attr, value in doc.items():
            if attr == '_id':
                continue
            else:
                field = cls_info.attributes[attr]
            obj_info.variables[field] = value

        obj._id = obj_id
        self._cache[(cls_info, obj_id)] = obj
        func = getattr(obj, '__thunder_loaded__', None)
        if func:
            func()
        return obj

    def _encode(self, obj_info):
        cls_info = obj_info.cls_info
        doc = {}
        for attr, field in cls_info.attributes.items():
            value = obj_info.variables.get(field, field.default)
            doc[attr] = value
        return doc

    def _flush_one(self, obj_info):
        cls_info = obj_info.cls_info
        collection = cls_info.get_collection(self)
        mongo_doc = self._encode(obj_info)
        action = obj_info.get('action')
        obj = obj_info.obj
        func = getattr(obj, '__thunder_pre_flush__', None)
        if func:
            func()

        if action == 'remove':
            collection.remove(mongo_doc)
            obj_info.delete("store")
        else:
            collection.save(mongo_doc)

            obj = obj_info.obj
            obj_id = mongo_doc['_id']
            obj._id = obj_id
            self._cache[(cls_info, obj_id)] = obj

        func = getattr(obj, '__thunder_flushed__', None)
        if func:
            func()

    def get(self, cls, obj_id):
        cls_info = get_cls_info(cls)
        collection = cls_info.get_collection(self)
        obj = self._cache.get((cls_info, obj_id))
        if obj is not None:
            return obj
        cursor = self._load(cls_info, collection.find,
                            {'_id': obj_id}, limit=2)
        if cursor.count():
            return self._build_doc(cls_info, cursor[0])

    def find(self, cls, *args, **kwargs):
        cls_info = get_cls_info(cls)
        collection = cls_info.get_collection(self)
        cursor = self._load(cls_info, collection.find, *args, **kwargs)
        for item in cursor:
            yield self._build_doc(cls_info, item)

    def find_one(self, cls, *args, **kwargs):
        cls_info = get_cls_info(cls)
        collection = cls_info.get_collection(self)
        item = self._load(cls_info, collection.find_one, *args, **kwargs)
        if item is not None:
            return self._build_doc(cls_info, item)

    def find_by(self, cls, **kwargs):
        cls_info = get_cls_info(cls)
        collection = cls_info.get_collection(self)
        cursor = self._load(cls_info, collection.find, kwargs)
        for item in cursor:
            yield self._build_doc(cls_info, item)

    def find_one_by(self, cls, **kwargs):
        cls_info = get_cls_info(cls)
        collection = cls_info.get_collection(self)
        item = self._load(cls_info, collection.find_one, **kwargs)
        if item is not None:
            return self._build_doc(cls_info, item)

    def count(self, cls):
        cls_info = get_cls_info(cls)
        collection = cls_info.get_collection(self)
        return collection.find().count()

    def add(self, obj):
        obj_info = get_obj_info(obj)

        if obj_info.get('store') is not None:  # pragma: nocoverage
            raise TypeError(
                "Document %s is already in a store" % (obj, ))

        obj_info.set('store', self)
        obj_info.flush_pending = True
        self.obj_infos.add(obj_info)

    def remove(self, obj):
        obj_info = get_obj_info(obj)
        store = obj_info.get('store')
        if store != self:
            raise Exception("This object does not belong to this store")

        obj_info.set('action', 'remove')
        obj_info.flush_pending = True
        self.obj_infos.add(obj_info)
        del self._cache[(obj_info.cls_info, obj._id)]

    def flush(self):
        for obj_info in self.obj_infos:
            # FIXME: Use obj_info.flush_pending
            self._flush_one(obj_info)

    def drop_cache(self):
        self._cache = {}

    def drop_collection(self, cls):
        cls_info = get_cls_info(cls)
        collection = cls_info.get_collection(self)
        collection.drop()

    def drop_collections(self):
        for name in self.database.collection_names():
            if name.startswith('system'):
                continue
            collection = self.database[name]
            collection.drop()
