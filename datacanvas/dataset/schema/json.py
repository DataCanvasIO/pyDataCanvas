# -*- coding: utf-8 -*-

import json
from .schema import Schema


class Json(Schema):
    def read_all(self):
        io = self._io
        if hasattr(io, 'open') and callable(io.open):
            with io.open('r') as f:
                return json.load(f)
        else:
            content = io.read_all()
            return json.loads(content)
