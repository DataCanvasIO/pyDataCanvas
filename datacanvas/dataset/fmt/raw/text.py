# -*- coding: utf-8 -*-

from datacanvas.dataset.fmt.fmt import Fmt


class Text(Fmt):
    def __init__(self):
        self.mode = 't'

    def load(self, f):
        return f.read()

    def dump(self, content, f):
        return f.write(content)
