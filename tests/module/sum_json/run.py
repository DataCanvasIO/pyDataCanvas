# -*- coding: utf-8 -*-


def main(params, inputs, outputs):
    array = inputs.array.read()
    print(array)
    result = outputs.result
    sum = 0
    for i in array:
        sum += i
    result.write(str(sum))
