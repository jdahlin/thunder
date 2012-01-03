from thunder.exceptions import InvalidObject, NotOneError

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
        self.id_field = None

        # FIXME: move this some place.
        from thunder.fields import Field, ObjectIdField
        pairs = []
        for attr in dir(cls):
            field = getattr(cls, attr, None)
            if isinstance(field, ObjectIdField):
                if self.id_field:
                    raise InvalidObject(
                        "There can only be one ObjectIdField")
                self.id_field = field
            if isinstance(field, Field):
                if attr == '_id':
                    raise InvalidObject(
                        "Cannot create a fields called '_id', it's reserved "
                        "to mongodb")
                pairs.append((attr, field))

        if not self.id_field:
            raise InvalidObject(
                "%r misses an ObjectIdField" % (cls.__name__, ))

        pairs.sort()

        self.fields = tuple(pair[1] for pair in pairs)
        self.attributes = dict(pairs)

    def get_collection(self, store):
        collection = store.database[self.doc_name]
        return collection

    def __repr__(self):
        return '<ClassInfo (%s, %s)>' % (self.cls.__name__,
                                         self.doc_name)


class ObjectInfo(object):
    def __init__(self, obj):
        self.obj = obj
        self.cls_info = get_cls_info(type(obj))
        self._options = {}
        self.variables = {}

    def set(self, name, value):
        self._options[name] = value

    def get(self, name):
        return self._options.get(name)

    def delete(self, name):
        del self._options[name]

    def set_obj_id(self, obj_id):
        self.variables[self.cls_info.id_field] = obj_id

    def get_obj_id(self):
        return self.variables.get(self.cls_info.id_field)

    @property
    def doc_name(self):
        return self.cls_info.doc_name

    def to_mongo(self):
        doc = {}
        for attr, field in self.cls_info.attributes.items():
            if self.cls_info.id_field is field:
                continue
            doc[attr] = self.variables.get(field, field.default)
        return doc


