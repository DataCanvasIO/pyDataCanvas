# -*- coding: utf-8 -*-

import json
from collections import namedtuple

from .spec import Spec


class ParamSpec(Spec):
    def __init__(self, config):
        if isinstance(config, str):
            self._config = json.loads(config)
        else:
            self._config = config

    def input(self, content):
        config = self._config
        Param = namedtuple('Param', list(config.keys()))
        d = {k: content.get(k, v['Default']) for k, v in config.items()}
        return Param(**d)
