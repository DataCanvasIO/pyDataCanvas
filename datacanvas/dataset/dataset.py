# -*- coding: utf-8 -*-

from .io import Io
from .schema import Schema


class DataSet(object):
    def __init__(self, schema_name, url):
        self._io = Io.get(url)
        self._schema = Schema.get(schema_name, self._io)

    def schema(self):
        return self._schema
