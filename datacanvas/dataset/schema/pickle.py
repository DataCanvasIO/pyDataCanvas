# -*- coding: utf-8 -*-

import pickle

from .schema import Schema


class Pickle(Schema):
    def __init__(self):
        self.mode = 'b'

    def loads(self, content):
        return pickle.loads(content)

    def dumps(self, content):
        return pickle.dumps(content)

    def load(self, f):
        return pickle.load(f)

    def dump(self, content, f):
        return pickle.dump(content, f)
