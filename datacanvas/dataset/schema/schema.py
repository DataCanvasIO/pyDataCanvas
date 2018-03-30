# -*- coding: utf-8 -*-

import sys
from importlib import import_module

from ..common import to_class_name


class Schema(object):
    def __init__(self, io, spec=None):
        self.__io = io
        self.__spec = spec

    @staticmethod
    def get(schema_name, io, spec=None):
        package = sys.modules[__name__].__package__
        module = import_module(package + '.' + schema_name)
        clazz = getattr(module, to_class_name(schema_name))
        return clazz(io, spec)

    def read(self):
        io = self.__io
        content = io.read()
        content = self._deserialize(content)
        spec = self.__spec
        if spec:
            content = spec.input(content)
        return content

    def write(self, content):
        spec = self.__spec
        if spec:
            content = spec.output(content)
        content = self._serialize(content)
        io = self.__io
        return io.write(content)

    def _deserialize(self, content):
        return content

    def _serialize(self, content):
        return content
