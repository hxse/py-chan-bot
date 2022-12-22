#!/usr/bin/env python3
# coding: utf-8
from copy import deepcopy


def find_value(dict, keyArr, number=0):
    if len(keyArr) <= number:
        if number == 0:
            return None
        else:
            return dict

    k = keyArr[number]
    if k not in dict:
        return None
    value = dict[k]
    if isinstance(value, type(dict)):
        return find_value(value, keyArr, number + 1)
    return value


dict = {"hello": {"apple": 233}}
keyArr = ["hello", "apple", 1234]
d = find_value(dict, keyArr)
import pdb

pdb.set_trace()


def merge_value(dict, keyArr, number=0):
    if len(keyArr) <= number:
        if number == 0:
            return None
        else:
            return dict

    k = keyArr[number]
    if k not in dict:
        return None
    value = dict[k]
    if isinstance(value, type(dict)):
        return merge_value(value, keyArr, number + 1)
    return value


def merge_two_dict(dic1, dic2):
    """合并两个key相同的多层嵌套字典"""
    dic = deepcopy(dic1)
    for key in dic2.keys():
        if key in dic:
            dic[key].update(dic2[key])
    print(dic)


if __name__ == "__main__":
    dic1 = {"小明": {"name": "owen", "age": 180, "test": {"hello": "world"}}}
    dic2 = {
        "小明": {"birthday": "1999-11-22", "height": 180, "test": {"hello2": "world2"}}
    }
    merge_two_dict(dic1, dic2)
