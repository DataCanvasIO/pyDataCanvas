# -*- coding: utf-8 -*-

import sys
from importlib import import_module

from ..common import to_class_name


class Spec:
    @staticmethod
    def get(spec_type, *args):
        package = sys.modules[__name__].__package__
        module = import_module(package + '.' + spec_type)
        clazz = getattr(module, to_class_name(spec_type))
        return clazz(*args)

    def input(self, content):
        return content

    def output(self, content):
        return content
