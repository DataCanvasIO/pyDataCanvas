# -*- coding: utf-8 -*-

import json

from .schema import Schema


class Json(Schema):
    def _deserialize(self, content):
        return json.loads(content)

    def _serialize(self, content):
        return json.dumps(content)
