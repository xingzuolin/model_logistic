#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/12/6 15:53
# @Author  : Jun
# @File    : d01_read_data.py


import os
import gzip
import numpy as np
import pandas as pd
import json


def get_var(bom_json):
    app_var = bom_json['applyInfo']
    mob_var = bom_json['mobile']
    td_var = bom_json['tongdun']
    zm_var = bom_json['zhima']
    yys_var = bom_json['yysDetail']
    taobao_var = bom_json['taobao']
    usr_var = bom_json['userInfo']
    dic_i = dict(app_var, **mob_var)
    # dic_i = dict(dic_i, **td_var)
    dic_i = dict(dic_i, **zm_var)
    dic_i = dict(dic_i, **yys_var)
    # dic_i = dict(dic_i, **taobao_var)
    # dic_i = dict(dic_i, **usr_var)
    dic_i['appid'] = bom_json['appId']
    dic_i['policyid'] = bom_json['policyId']
    return dic_i


def read_gzip_data(path):
    data_list = []
    num = 0
    if os.path.exists(path):
        with gzip.open(path, 'r') as f:
            for i, j in enumerate(f):
                bom_json = json.loads(j[1:-2])
                dic_i = get_var(bom_json)
                print(num, bom_json['appId'])
                num += 1
                data_list.append(dic_i)
    else:
        print('the path [{}] is not exist!'.format(path))
    return data_list

############################
#
# 读取压缩包中csv文件
#
############################


def missing_null(x):
    if x == '':
        return -1
    else:
        return x


def change_vars_type(in_data, string_list=[]):
    num = 0
    for col in in_data.columns:
        if col in string_list:
            continue
        elif num < 20:
            num += 1
            in_data[col] = in_data[col].apply(lambda x: missing_null(x)).astype('float')
    return in_data


def read_gz_csv(path):
    data_list = []
    col_names = []
    if os.path.exists(path):
        with gzip.open(path, 'r') as f:
            for i, j in enumerate(f):
                line = str(j, encoding='utf-8').upper()
                if i == 0:
                    col_names = line.split(',')
                else:
                    data_list.append(line.split(','))
    else:
        print('the path [{}] is not exist!'.format(path))
    df = pd.DataFrame(data_list, columns=col_names)
    return df
