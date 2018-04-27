# -*- coding: utf-8 -*-

import os

from datacanvas import DataCanvas
from datacanvas.dataset import DataSet

dc = DataCanvas(__name__)

my_dir = os.path.dirname(__file__)
input_X_url = my_dir + '/input_X.json'
input_Y_url = my_dir + '/input_Y.json'
output_model_url = my_dir + '/test_output_model.pickle'
output_model_meta_url = my_dir + '/test_output_model_meta.json'


@dc.runtime(
    spec_file_url=my_dir + '/spec.json',
    param_file_url=my_dir + '/param.json',
    args={
        'X': {'url': input_X_url, 'format': 'json'},
        'Y': {'url': input_Y_url, 'format': 'json'},
        'model': {'url': output_model_url, 'format': 'pickle'},
        'model_meta': {'url': output_model_meta_url, 'format': 'json'},
    },
)
def task(runtime, params, inputs, outputs):
    from .run import main
    main(params, inputs, outputs)


def test_main():
    dc.run()
    result = DataSet(url=output_model_meta_url, format='json').get_raw()
    assert result['model_name'] == 'test_lr'
