# -*- coding: utf-8 -*-

from ..common import get_module_class


class Spec:
    @staticmethod
    def get(spec_type, *args):
        clazz = get_module_class(spec_type, __name__)
        return clazz(*args)

    def input(self, content):
        return content

    def output(self, content):
        return content
