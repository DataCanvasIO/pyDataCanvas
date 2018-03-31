# -*- coding: utf-8 -*-

import os

from datacanvas import DataCanvas

dc = DataCanvas(__name__)

my_dir = os.path.dirname(__file__)
input_X_url = 'json:' + my_dir + '/input_X.json'
input_Y_url = 'json:' + my_dir + '/input_Y.json'
output_model_url = 'pickle:' + my_dir + '/test_output_model.pickle'
output_model_meta_url = 'json:' + my_dir + '/test_output_model_meta.json'


@dc.runtime(
    spec_file_url='json:' + my_dir + '/spec.json',
    param_file_url='json:' + my_dir + '/param.json',
    args={
        'X': input_X_url,
        'Y': input_Y_url,
        'model': output_model_url,
        'model_meta': output_model_meta_url
    },
)
def main(runtime, params, inputs, outputs):
    from .run import main
    main(params, inputs, outputs)


def test_main():
    dc.run()
