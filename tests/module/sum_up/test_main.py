# -*- coding: utf-8 -*-

import os

from datacanvas import DataCanvas
from datacanvas.dataset import DataSet

dc = DataCanvas(__name__)

my_dir = 'file://' + os.path.dirname(__file__)
output_result_url = my_dir + '/test_output_result.txt'


@dc.runtime(
    spec_file_url=my_dir + '/spec.json',
    param_file_url=my_dir + '/param.json',
    args={
        'array': 'here://{"url": "' + my_dir + '/input_array.json", "format": "json"}',
        'result': 'here://{"url": "' + output_result_url + '", "format":"text"}',
    }
)
def main(runtime, params, inputs, outputs):
    from .run import main
    main(params, inputs, outputs)


def test_main():
    dc.run()
    result = DataSet(output_result_url, 'text').get_raw()
    assert result == '25'
