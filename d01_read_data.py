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
from sklearn import model_selection
import time


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

############################
#
# 读取csv文件
#
############################


def read_csv_data(path):
    df = pd.read_csv(path)
    return df


def data_split(df_data, tag, data_size, only_gb=True):
    if only_gb:
        df_data = df_data[df_data[tag].isin([0, 1])]
    if len(df_data[df_data[tag] == 0]) == 0 or len(df_data[df_data[tag] == 1]) == 0:
        print('Error: there is only the good or bad data')
        return df_data, df_data
    df_y = df_data[tag]
    df_x = df_data.drop(tag, axis=1)
    x_train, x_test, y_train, y_test = model_selection.train_test_split(
        df_x, df_y, test_size=data_size, random_state=256)
    df_train = pd.concat([x_train, y_train], axis=1)
    df_test = pd.concat([x_test, y_test], axis=1)
    return df_train, df_test


def data_path(base_path, save_path):
    time_format = '%Y-%m-%d %X'
    time_now = time.strftime(time_format, time.localtime())
    try:
        os.mkdir(base_path + '/' + str(time_now)[:10] + '_' + save_path)
    except:
        print('PATH HAS EXIST')
    path_ = base_path + '/' + str(time_now)[:10] + '_' + save_path
    return path_
