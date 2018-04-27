# -*- coding: utf-8 -*-

from .parser import Parser


class Text(Parser):
    def __init__(self):
        self.mode = 't'

    def load(self, f):
        return f.read()

    def dump(self, content, f):
        return f.write(content)
