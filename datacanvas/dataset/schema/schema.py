# -*- coding: utf-8 -*-

import sys

from ..common import get_module_class


class Schema(object):
    def __init__(self, io, spec=None):
        self.__io = io
        self.__spec = spec

    @staticmethod
    def get(schema_name, io, spec=None):
        clazz = get_module_class(schema_name, sys.modules[__name__].__package__)
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
