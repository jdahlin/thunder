from thunder.info import get_obj_info, get_cls_info


class Field(object):
    def __init__(self, default):
        self.default = default

    def __set__(self, obj, value):
        obj_info = get_obj_info(obj)
        obj_info.variables[self] = value

    def __get__(self, obj, cls=None):
        if obj is None:
            return self
        obj_info = get_obj_info(obj)
        if obj_info is not None:
            value = obj_info.variables[self]
            return value


class ObjectIdField(Field):
    def __init__(self):
        Field.__init__(self, default=None)


class StringField(Field):
    def __init__(self):
        Field.__init__(self, default='')
