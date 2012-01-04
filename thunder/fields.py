import datetime
import decimal
import re

from thunder.exceptions import ValidationError
from thunder.info import get_obj_info


class Field(object):
    def __init__(self, default):
        self.default = default

    def __set__(self, obj, value):
        obj_info = get_obj_info(obj)
        obj_info.variables[self] = self.from_python(value)

    def __get__(self, obj, cls=None):
        if obj is None:
            return self
        obj_info = get_obj_info(obj)
        if obj_info is not None:
            value = obj_info.variables[self]
            return self.to_python(value)

    def to_python(self, value):
        return value

    def from_python(self, value):
        return value


class ObjectIdField(Field):
    def __init__(self):
        Field.__init__(self, default=None)


class StringField(Field):
    def __init__(self):
        Field.__init__(self, default='')


class EmailField(StringField):
    EMAIL_REGEX = re.compile(
        # dot-atom
        r"(^[-!#$%&'*+/=?^_`{}|~0-9A-Z]+(\.[-!#$%&'*+/=?^_`{}|~0-9A-Z]+)*"
        # quoted-string
        r'|^"([\001-\010\013\014\016-\037!#-\[\]-\177]|'
        r'\\[\001-011\013\014\016-\177])*"'
        # domain
        r')@(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+[A-Z]{2,6}\.?$',
        re.IGNORECASE
    )

    def from_python(self, value):  # pragma: nocoverage
        if not EmailField.EMAIL_REGEX.match(value):
            raise ValidationError(
                "Invalid Mail-address: %r" % (value, ))
        return value


class DateTimeField(Field):
    def __init__(self):
        Field.__init__(self, default=None)

    def to_python(self, value):
        return value

    def from_python(self, value):
        if self.default == datetime.datetime.now:  # pragma: nocoverage
            value = datetime.datetime.now()
        return value


class IntField(Field):
    def to_python(self, value):  # pragma: nocoverage
        try:
            return int(value)
        except ValueError, e:
            raise ValidationError(e)


class DecimalField(Field):
    """
    A field for storing fixed precision values where fractional values are
    acceptable and loss of precision is not.
    """

    precision = 2

    def __init__(self, min_value=None, max_value=None, precision_check=False,
                 **kwargs):
        self.min_value = min_value
        self.max_value = max_value
        self.precision_multiplier = decimal.Decimal(
            10 ** -decimal.Decimal(self.precision))
        self.precision_check = precision_check
        Field.__init__(self, default=decimal.Decimal("0"))

    def _validate(self, value):  # pragma: nocoverage
        if self.min_value is not None and value < self.min_value:
            raise ValidationError(
                'Decimal value %r is too small, min is %r' % (
                value, self.min_value))

        if self.max_value is not None and value > self.max_value:
            raise ValidationError(
                'Decimal value %r is too large, max is %r' % (
                value, self.max_value))

    def to_python(self, value):  # pragma: nocoverage
        if not isinstance(value, basestring):
            value = unicode(value)

        try:
            value = decimal.Decimal(value) * self.precision_multiplier
        except Exception, e:
            raise ValidationError(
                "Could not convert %r to decimal: %s" % (
                value, e))

        try:
            value.quantize(self.precision_multiplier,
                           context=decimal.Context(traps=[decimal.Inexact]))
        except decimal.Inexact, e:
            raise ValidationError(
                "At most %d decimals allowed, got %d (%r)" % (
                self.precision, abs(value.as_tuple().exponent), e))

        self._validate(value)
        return value

    def from_python(self, value):
        if not isinstance(value, decimal.Decimal):  # pragma: nocoverage
            value = decimal.Decimal(value)

        if self.precision_check:
            if abs(value.as_tuple().exponent) > self.precision:
                raise ValidationError(
                    "At most %d decimals allowed, got %d" % (
                    self.precision, abs(value.as_tuple().exponent)))

        self._validate(value)
        return int(value / self.precision_multiplier)


# ReferenceField
# ListField
# DictField
