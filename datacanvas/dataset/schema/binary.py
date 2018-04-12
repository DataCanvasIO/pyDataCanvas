# -*- coding: utf-8 -*-

from .schema import Schema


class Binary(Schema):
    def __init__(self):
        self.mode = 'b'

    def load(self, f):
        return f.read()

    def dump(self, content, f):
        return f.write(content)
