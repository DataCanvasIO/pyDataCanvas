# -*- coding: utf-8 -*-

from ..common import get_module_class


class Parser(object):
    @staticmethod
    def get(parser_name):
        clazz = get_module_class(parser_name, __name__)
        return clazz()

    def loads(self, content):
        return content

    def dumps(self, content):
        return content

    def load(self, f):
        return NotImplemented

    def dump(self, content, f):
        return NotImplemented
