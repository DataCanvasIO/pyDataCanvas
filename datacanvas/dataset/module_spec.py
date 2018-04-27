# -*- coding: utf-8 -*-

from collections import namedtuple

from .dataset import DataSet


class ModuleSpec(DataSet):
    def __init__(self, url):
        DataSet.__init__(self, url=url, format='json')

    def get_module(self):
        content = self.get_raw()
        required_keys = ['Name', 'Description', 'Version', 'Param', 'Input', 'Output', 'Cmd']
        if not all(k in content for k in required_keys):
            raise ValueError('Spec of module must have these keys: %s!' % str(required_keys))
        Module = namedtuple('Module', required_keys)
        return Module(**{k: content[k] for k in required_keys})
