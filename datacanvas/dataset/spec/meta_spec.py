# -*- coding: utf-8 -*-

from collections import namedtuple

from .spec import Spec


class MetaSpec(Spec):
    def __init__(self):
        pass

    def input(self, content):
        moderate_keys = ['url', 'format']
        if not all(k in content for k in moderate_keys):
            raise ValueError('One of param from %s may not exist.' % str(moderate_keys))
        Meta = namedtuple('Meta', moderate_keys)
        return Meta(**{k: content[k] for k in moderate_keys})
