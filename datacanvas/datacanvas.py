# -*- coding: utf-8 -*-

import functools
import os
import sys

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

    def runtime(self, spec_file_url="spec.json", param_file_url="param.json", args=None):
        if not args:
            args = dict(arg.split("=") for arg in sys.argv[1:])

        def decorator(method):
            runtime = Runtime(spec_file_url)
            runtime.set_params(param_file_url)
            runtime.set_inputs(args)
            runtime.set_outputs(args)
            params = runtime.Params
            inputs = runtime.Inputs
            outputs = runtime.Outputs

            @functools.wraps(method)
            def wrapper(_runtime=runtime, _params=params, _inputs=inputs, _outputs=outputs):
                print(_runtime)
                method(_runtime, _params, _inputs, _outputs)

            self._graph.append(wrapper)
            return wrapper

        return decorator

    def run(self):
        for m in self._graph:
            m()
