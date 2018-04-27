# -*- coding: utf-8 -*-

import pandas as pd

from datacanvas.dataset import PandasDataSet


def test_pandas_json():
    url = 'here:{ "col1": [1, 2, 3, 4], "col2": [5, 6, 7, 8] }'
    i = PandasDataSet(url=url, format='json')
    df = i.get_dataframe()
    assert isinstance(df, pd.DataFrame)
    assert df.at[2, 'col2'] == 7

    url = 'file://test_output_pandas_json.json'
    o = PandasDataSet(url=url, format='json')
    o.put_dataframe(df)
    i = PandasDataSet(url=url, format='json')
    df = i.get_dataframe()
    assert isinstance(df, pd.DataFrame)
    assert df.at[3, 'col1'] == 4
