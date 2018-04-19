# -*- coding: utf-8 -*-

import json

from datacanvas.dataset.fmt.fmt import Fmt


class Json(Fmt):
    def __init__(self):
        self.mode = 't'

    def loads(self, content):
        return json.loads(content)

    def dumps(self, content):
        return json.dumps(content)

    def load(self, f):
        return json.load(f)

    def dump(self, content, f):
        return json.dump(content, f)
