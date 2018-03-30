# -*- coding: utf-8 -*-

from .io import Io
from .schema import Schema


class DataSet(object):
    def __init__(self, schema_name, url, spec=None):
        io = Io.get(url)
        self.__io = io
        self.__schema = Schema.get(schema_name, io, spec)

    def read(self):
        schema = self.__schema
        return schema.read()

    def write(self, content):
        schema = self.__schema
        return schema.write(content)
