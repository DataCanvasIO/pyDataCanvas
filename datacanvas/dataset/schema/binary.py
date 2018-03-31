# -*- coding: utf-8 -*-

from .schema import Schema


class Binary(Schema):
    def __init__(self):
        self.mode = 'b'
