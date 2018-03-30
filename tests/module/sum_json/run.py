# -*- coding: utf-8 -*-


def main(params, inputs, outputs):
    array = inputs.array
    result = outputs.result
    sum = 0
    for i in array.read():
        sum += i
    result.write(str(sum))
