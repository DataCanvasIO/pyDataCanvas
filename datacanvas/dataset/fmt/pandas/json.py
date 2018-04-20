# -*- coding: utf-8 -*-

import pandas as pd

from datacanvas.dataset.fmt.fmt import Fmt


class Json(Fmt):
    def __init__(self):
        self.mode = 't'

    def loads(self, content):
        return pd.read_json(content)

    def dumps(self, content):
        return content.to_json()

    def load(self, f):
        return pd.read_json(f)

    def dump(self, content, f):
        return content.to_json(f)
