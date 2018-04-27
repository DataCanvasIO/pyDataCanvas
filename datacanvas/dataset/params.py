# -*- coding: utf-8 -*-

from collections import namedtuple

from .dataset import DataSet


class Params(DataSet):
    def __init__(self, url, spec):
        DataSet.__init__(self, url=url, format='json')
        self.__spec = spec

    def get_params(self):
        spec = self.__spec
        content = self.get_raw()
        Param = namedtuple('Param', list(spec.keys()))
        d = {k: content.get(k, v['Default']) for k, v in spec.items()}
        return Param(**d)
