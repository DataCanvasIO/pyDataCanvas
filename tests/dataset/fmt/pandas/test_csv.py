# -*- coding: utf-8 -*-

import pandas as pd

from datacanvas.dataset import DataSet


def test_pandas_csv():
    url = 'here://{ "col1": [1, 2, 3, 4], "col2": [5, 6, 7, 8] }'
    i = DataSet(url, 'json')
    df = i.get_dataframe()
    assert isinstance(df, pd.DataFrame)
    assert df.at[2, 'col2'] == 7
    url = 'file://test_output_pandas_csv.csv'
    o = DataSet(url, 'csv')
    o.put_dataframe(df)
    i = DataSet(url, 'csv')
    df = i.get_dataframe()
    assert isinstance(df, pd.DataFrame)
    assert df.at[3, 'col1'] == 4
