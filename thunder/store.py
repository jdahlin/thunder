from pymongo import Connection
from pymongo.database import Database

from thunder.exceptions import InvalidObject, NotOneError
from thunder.info import get_cls_info, get_obj_info


class Store(object):
    def __init__(self, conn_string, database):
        if not conn_string.startswith('mongodb://'):
            conn_string = 'mongodb://' + conn_string
        self.connection = Connection(conn_string)
        self.database = Database(self.connection, database)

        self.obj_infos = set()
        self._cache = {}

    def _load(self, cls_info, doc):
        obj = self._cache.get((cls_info, doc["_id"]))
        if obj is not None:
            return obj
        cls = cls_info.cls
        obj = cls.__new__(cls)

        obj_info = get_obj_info(obj)
        obj_info.set("store", self)

        for attr, value in doc.items():
            if attr == '_id':
                field = cls_info.id_field
            else:
                try:
                    field = cls_info.attributes[attr]
                # This model doesn't have a field for this document attribute,
                # skip it
                except KeyError:
                    continue
            obj_info.variables[field] = value

        self._cache[(cls_info, doc["_id"])] = obj
        return obj

    def get(self, cls, obj_id):
        cls_info = get_cls_info(cls)
        collection = cls_info.get_collection(self)
        cursor = collection.find({'_id': obj_id}, limit=2)
        if cursor.count() == 2:
            raise NotOneError("One document expected, but more found")
        elif cursor.count() == 1:
            return self._load(cls_info, cursor[0])
        else:
            return None

    def find(self, cls, *args, **kwargs):
        cls_info = get_cls_info(cls)
        collection = cls_info.get_collection(self)
        cursor = collection.find(*args, **kwargs)
        for item in cursor:
            yield self._load(cls_info, item)

    def find_one(self, cls, *args, **kwargs):
        cls_info = get_cls_info(cls)
        collection = cls_info.get_collection(self)
        item = collection.find_one(*args, **kwargs)
        if item is not None:
            return self._load(cls_info, item)

    def count(self, cls):
        cls_info = get_cls_info(cls)
        collection = cls_info.get_collection(self)
        return collection.find().count()

    def add(self, obj):
        obj_info = get_obj_info(obj)

        assert not obj_info.get('store')
        obj_info.set('store', self)
        self.obj_infos.add(obj_info)

    def remove(self, obj):
        obj_info = get_obj_info(obj)
        store = obj_info.get('store')
        if store != self:
            raise Exception("This object does not belong to this store")

        obj_info.set('action', 'remove')
        self.obj_infos.add(obj_info)

    def _flush_one(self, obj_info):
        cls_info = obj_info.cls_info
        collection = cls_info.get_collection(self)
        mongo_doc = obj_info.to_mongo()

        action = obj_info.get('action')
        if action == 'remove':
            obj = obj_info.obj
            collection.remove(mongo_doc)
            obj_info.delete("store")
            return

        collection.save(mongo_doc)

        obj = obj_info.obj
        self._cache[(cls_info, mongo_doc['_id'])] = obj
        if obj_info.get_obj_id() is None:
            obj_info.set_obj_id(mongo_doc['_id'])

    def flush(self):
        for obj_info in self.obj_infos:
            self._flush_one(obj_info)

    def drop_collections(self):
        for name in self.database.collection_names():
            if name.startswith('system'):
                continue
            collection = self.database[name]
            collection.drop()


