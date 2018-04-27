# -*- coding: utf-8 -*-


def main(params, inputs, outputs):
    array = inputs.array.get_raw()
    result = outputs.result
    sum = 0
    for i in array:
        sum += i
    result.put_raw(str(sum))
