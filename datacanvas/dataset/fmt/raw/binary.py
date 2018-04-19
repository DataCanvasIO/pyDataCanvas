# -*- coding: utf-8 -*-

from datacanvas.dataset.fmt.fmt import Fmt


class Binary(Fmt):
    def __init__(self):
        self.mode = 'b'

    def load(self, f):
        return f.read()

    def dump(self, content, f):
        return f.write(content)
