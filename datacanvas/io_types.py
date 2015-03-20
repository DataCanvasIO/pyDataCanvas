import re
import json
from collections import namedtuple


def _create_property(field_name, docstring, read_only=False, from_meta=False):
    """Helper for creating properties to IO objects to files.
    """

    def getter(self):
        if from_meta:
            if 'Meta' not in self:
                return None
            return self['Meta'].get(field_name, None)
        return self.get(field_name, None)

    def setter(self, value):
        if from_meta:
            if 'Meta' not in self:
                self['Meta'] = {}
            self['Meta'][field_name] = value
        else:
            self[field_name] = value

    def deleter(self):
        if from_meta:
            del self['Meta'][field_name]
        else:
            del self[field_name]

    if read_only:
        docstring = docstring + "\n\nThis attribute is read-only."

    if not read_only:
        return property(getter, setter, deleter, doc=docstring)
    return property(getter, doc=docstring)


class BaseIO(dict):
    Type = _create_property("Type", "", read_only=False, from_meta=False)

    def _init_attrs(self, attrs):
        for attr_name in attrs.keys():
            if hasattr(self.__class__, attr_name) and isinstance(getattr(DS_Hive, attr_name), property):
                setattr(self, attr_name, attrs.pop(attr_name))

    def __init__(self, seq=None, **attrs):
        self._init_attrs(attrs)
        if seq:
            super(BaseIO, self).__init__(seq)
        else:
            super(BaseIO, self).__init__(**attrs)


class DS_File(BaseIO):
    URL = _create_property("URL", "", read_only=False, from_meta=False)

    def __init__(self, seq=None, **attrs):
        if 'Type' not in attrs:
            attrs["Type"] = 'datasource.file'
        if seq:
            super(DS_File, self).__init__(seq)
        else:
            super(DS_File, self).__init__(**attrs)


class DS_S3(BaseIO):
    URL = _create_property("URL", "", read_only=False, from_meta=False)
    aws_key = _create_property("key", "", read_only=False, from_meta=True)
    aws_security = _create_property("token", "", read_only=False, from_meta=True)

    def __init__(self, seq=None, **attrs):
        if 'Type' not in attrs:
            attrs["Type"] = 'datasource.s3'
        if seq:
            super(DS_S3, self).__init__(seq)
        else:
            super(DS_S3, self).__init__(**attrs)


class DS_HDFS(BaseIO):
    URL = _create_property("URL", "", read_only=False, from_meta=False)
    port = _create_property("port", "", read_only=False, from_meta=True)

    def __init__(self, seq=None, **attrs):
        if 'Type' not in attrs:
            attrs["Type"] = 'datasource.hdfs'
        if seq:
            super(DS_HDFS, self).__init__(seq)
        else:
            super(DS_HDFS, self).__init__(**attrs)


class DS_Hive(BaseIO):
    URL = _create_property("URL", "", read_only=False, from_meta=False)
    meta_server = _create_property("hive_server2_host", "", read_only=False, from_meta=True)
    meta_port = _create_property("hive_server2_port", "", read_only=False, from_meta=True)

    def __init__(self, seq=None, **attrs):
        if 'Type' not in attrs:
            attrs["Type"] = 'datasource.hive'
        if seq:
            super(DS_Hive, self).__init__(seq)
        else:
            super(DS_Hive, self).__init__(**attrs)


class DS_Database(BaseIO):
    URL = _create_property("URL", "", read_only=False, from_meta=False)
    meta_type = _create_property("type", "", read_only=False, from_meta=True)
    meta_host = _create_property("host", "", read_only=False, from_meta=True)
    meta_port = _create_property("port", "", read_only=False, from_meta=True)
    meta_user = _create_property("user", "", read_only=False, from_meta=True)
    meta_password = _create_property("password", "", read_only=False, from_meta=True)
    meta_database = _create_property("database", "", read_only=False, from_meta=True)

    def __init__(self, seq=None, **attrs):
        if 'Type' not in attrs:
            attrs["Type"] = 'datasource.db'
        if seq:
            super(DS_Database, self).__init__(seq)
        else:
            super(DS_Database, self).__init__(**attrs)


_handler_callbacks = {}

TypeHandler = namedtuple("TypeHandler", ["handler", "type_pattern", "is_regex"])


def register_handler(type_name, type_pattern, handler, is_regex=False):
    if not type_pattern:
        type_pattern = type_name
    _handler_callbacks[type_name] = TypeHandler(handler=handler, type_pattern=type_pattern, is_regex=is_regex)


def specific_types():
    return dict((k, v) for k, v in _handler_callbacks.items() if not v.is_regex)


def generic_types():
    return dict((k, v) for k, v in _handler_callbacks.items() if v.is_regex)


def all_types():
    return _handler_callbacks


def is_subtype_of(type_name, target_type):
    gts = all_types()
    if type_name not in gts:
        return False
    type_handler = gts[type_name]
    if type_handler.is_regex:
        return re.match(type_handler.type_pattern, target_type)
    else:
        return type_handler.type_pattern == target_type


def is_type_of(type_name, target_type):
    return is_subtype_of(type_name, target_type) or is_subtype_of(type_name + ".*", target_type)


register_handler("LocalFile", None, DS_File)
register_handler("Http", None, DS_File)
register_handler("Ftp", None, DS_File)
register_handler("AWS_S3", None, DS_S3)
register_handler("HDFS", None, DS_HDFS)
register_handler("Hive", None, DS_Hive)
register_handler("DB", None, DS_Database)

register_handler("datasource.file", None, DS_File)
register_handler("datasource.s3", None, DS_S3)
register_handler("datasource.hdfs", None, DS_HDFS)
register_handler("datasource.hive", None, DS_Hive)
register_handler("datasource.db", None, DS_Database)
register_handler("datasource", None, BaseIO)

register_handler("s3", None, DS_S3)
register_handler("s3.*", "s3[\.\w+]+", DS_S3, is_regex=True)
register_handler("hdfs", None, DS_HDFS)
register_handler("hdfs.*", "hdfs[\.\w+]+", DS_HDFS, is_regex=True)
register_handler("hive", None, DS_Hive)
register_handler("hive.*", "hive[\.\w+]+", DS_Hive, is_regex=True)


def from_json(json_object):
    if 'Type' in json_object:
        jo_type = json_object['Type']

        stypes = specific_types()
        gtypes = generic_types()

        # Specific Type
        if jo_type in stypes:
            return stypes[jo_type].handler(json_object)
        else:
            # Generic Type
            for gt_name, gt_obj in gtypes.items():
                if re.match(gt_obj.type_pattern, jo_type):
                    return gt_obj.handler(json_object)
            else:
                return BaseIO(json_object)
    return json_object


def load_io_obj(filename):
    with open(filename, 'r') as f:
        return json.load(f, object_hook=from_json)
