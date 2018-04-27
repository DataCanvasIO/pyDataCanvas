# -*- coding: utf-8 -*-
import random
import pandas
import datacanvas.dataset


def sample(p):
    x, y = random.random(), random.random()
    return 1 if x * x + y * y < 1 else 0


def main(params, inputs, outputs):
    NUM_SAMPLES = 1000
    count = sc.parallelize(range(NUM_SAMPLES)).map(sample).reduce(lambda a, b: a + b)
    print(datacanvas)
    print(count)
