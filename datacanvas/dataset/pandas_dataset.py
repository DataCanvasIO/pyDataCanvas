# -*- coding: utf-8 -*-

from .dataset import DataSet
from .scheme import Scheme


class PandasDataSet(DataSet):
    def __init__(self, *args, **kwargs):
        DataSet.__init__(self, *args, **kwargs)
        self.set_parser('pandas.' + self.fmt())

    def get_dataframe(self):
        scheme = Scheme.get(self.spec())
        return scheme.read()

    def put_dataframe(self, content):
        scheme = Scheme.get(self.spec())
        return scheme.write(content)
