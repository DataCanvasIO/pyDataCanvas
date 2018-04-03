# -*- coding: utf-8 -*-

import time
from multiprocessing import Process

import pytest
from flask import Flask

from datacanvas.dataset import DataSet


def test_json_here():
    url = 'json:here://{ "root": { "leaf1": "1", "leaf2": 2 } }'
    i = DataSet(url)
    content_read = i.read()
    assert content_read['root']['leaf1'] == '1'
    assert content_read['root']['leaf2'] == 2
    o = DataSet(url)
    with pytest.raises(NotImplementedError):
        o.write('')


def test_text_http():
    msg = '{"hello": "world"}'
    app = Flask('app')

    @app.route('/')
    def hello():
        return msg

    p = Process(target=lambda: app.run())
    p.start()
    time.sleep(1)

    url = 'json:http://localhost:5000'
    i = DataSet(url)
    content_read = i.read()
    assert content_read['hello'] == 'world'
    p.terminate()
