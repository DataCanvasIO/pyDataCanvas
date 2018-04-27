# -*- coding: utf-8 -*-

import pandas as pd

from ..parser import Parser


class Pickle(Parser):
    def __init__(self):
        self.mode = 'b'

    def loads(self, content):
        return pd.read_pickle(content)

    def dumps(self, content):
        return content.to_pickle()

    def load(self, f):
        return pd.read_pickle(f)

    def dump(self, content, f):
        return content.to_pickle(f)
