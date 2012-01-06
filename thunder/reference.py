from thunder.fields import Field
from thunder.info import get_obj_info


class ReferenceField(Field):
    def __init__(self, local_field, remote_cls, remote_attr='_id', **kwargs):
        if not isinstance(local_field, Field):  # pragma nocoverage
            raise TypeError(
                "first argument must be a Field instance, not %r" % (
                local_field, ))
        self.local_field = local_field
        self.remote_cls = remote_cls
        self.remote_attr = remote_attr
        Field.__init__(self, **kwargs)

    def __get__(self, obj, cls):
        if obj is None:
            return obj

        obj_info = get_obj_info(obj)
        if self in obj_info.variables:
            remote_value, remote_id = obj_info.variables[self]
            store = get_obj_info(remote_value).get('store')
            remote = store.get(self.remote_cls, remote_id)
        else:
            remote_id = obj_info.variables[self.local_field]
            store = obj_info.get('store')
            remote = store.get(self.remote_cls, remote_id)
        return remote

    def __set__(self, obj, value):
        if (value is not None and
            not isinstance(value, self.remote_cls)):
            raise TypeError("Must be a %s, not a %r" % (
                self.remote_cls.__name__,
                value.__class__.__name__))
        if value is None:
            remote_value = None
        else:
            remote_value = getattr(value, self.remote_attr)

        obj_info = get_obj_info(obj)
        obj_info.variables[self] = value, remote_value
        obj_info.variables[self.local_field] = remote_value


class GenericReferenceField(Field):
    pass
