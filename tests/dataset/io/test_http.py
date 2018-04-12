# -*- coding: utf-8 -*-

import time
from multiprocessing import Process

from flask import Flask

from datacanvas.dataset import DataSet


def test_json_http():
    msg = '{ "hello": "world" }'

    app = Flask('app')

    @app.route('/')
    def hello():
        return msg

    p = Process(target=lambda: app.run())
    p.start()
    time.sleep(1)
    if not p.is_alive():
        assert False

    url = 'json:http://localhost:5000'
    i = DataSet(url)
    content_read = i.read()
    assert content_read['hello'] == 'world'
    p.terminate()
