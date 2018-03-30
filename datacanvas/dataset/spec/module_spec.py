# -*- coding: utf-8 -*-

from collections import namedtuple

from .spec import Spec


class ModuleSpec(Spec):
    def __init__(self):
        pass

    def input(self, content):
        moderate_keys = ['Name', 'Description', 'Version', 'Param', 'Input', 'Output', 'Cmd']
        if not all(k in content for k in moderate_keys):
            raise ValueError("One of param from %s may not exist in 'spec.json'" % str(moderate_keys))
        Module = namedtuple('Module', moderate_keys)
        return Module(**{k: content[k] for k in moderate_keys})

    def output(self, content):
        return content
