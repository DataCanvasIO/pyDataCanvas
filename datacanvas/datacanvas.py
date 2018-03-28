# -*- coding: utf-8 -*-

import os
import functools
from .runtime import Runtime
from .validate import Validator


class DataCanvas(object):
    def __init__(self, name):
        self._name = name
        self._graph = []
        self._rt = None
        is_validate = os.environ.get("isValidate")
        if is_validate == 'true':
            Validator.validate()

    def basic_runtime(self, spec_json="spec.json"):
        def decorator(method):
            rt = Runtime(spec_filename=spec_json)
            params = rt.settings.Param
            inputs = rt.settings.Input
            outputs = rt.settings.Output

            @functools.wraps(method)
            def wrapper(_rt=rt, _params=params, _inputs=inputs, _outputs=outputs):
                print(rt)
                method(_rt, _params, _inputs, _outputs)

            self._graph.append(wrapper)
            return wrapper

        return decorator

    def run(self):
        for m in self._graph:
            m()
