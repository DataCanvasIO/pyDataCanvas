# -*- coding: utf-8 -*-

from .schema import Schema


class Text(Schema):
    def __init__(self):
        self.mode = 't'
