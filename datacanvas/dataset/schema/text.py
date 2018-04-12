# -*- coding: utf-8 -*-

from .schema import Schema


class Text(Schema):
    def __init__(self):
        self.mode = 't'

    def load(self, f):
        return f.read()

    def dump(self, content, f):
        return f.write(content)
