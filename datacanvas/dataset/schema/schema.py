# -*- coding: utf-8 -*-

import sys
from importlib import import_module


class Schema(object):
    def __init__(self, io):
        self._io = io

    @staticmethod
    def get(schema_name, io):
        package = sys.modules[__name__].__package__
        module = import_module(package + '.' + schema_name)
        clazz = getattr(module, schema_name.capitalize())
        return clazz(io)

    def read_all(self):
        io = self._io
        return io.read_all()

    def write_all(self, content):
        io = self._io
        return io.write_all(content)
