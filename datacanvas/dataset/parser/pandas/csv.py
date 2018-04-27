# -*- coding: utf-8 -*-

import pandas as pd

from ..parser import Parser


class Csv(Parser):
    def __init__(self):
        self.mode = 't'

    def loads(self, content):
        return pd.read_csv(content)

    def dumps(self, content):
        return content.to_csv()

    def load(self, f):
        return pd.read_csv(f)

    def dump(self, content, f):
        return content.to_csv(f)
