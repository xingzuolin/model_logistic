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
import preprocess_var


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
                if 0 <= i <= 10:
                    bom_json = json.loads(j[1:-2])
                    dic_i = get_var(bom_json)
                    print(num, bom_json['appId'])
                    num += 1
                    data_list.append(dic_i)
    else:
        print('the path [{}] is not exist!'.format(path))
    return data_list

gzip_data = read_gzip_data(r'E:\dev_code\model_ori1120_08.gz')
df = pd.DataFrame(gzip_data)


def col_group(x):
    if x.startswith('call'):
        return 'call'
    elif x.startswith('net'):
        return 'net'
    elif x.startswith('sms'):
        return 'sms'
    return 'other'

df_col = df.columns
col_names = map(lambda x: col_group(x), df_col)
col_zip = zip(col_names, df_col)
df1 = df.ix[:, [col2 for (col1, col2) in col_zip if col1 in ('other', 'call')]]

print(df1.shape)



# df1 = pd.read_csv(r'E:\dev_code\model_ori1120_08.csv', encoding='GBK')
miss_df = preprocess_var.missing_vars(df1, missing_flag=['NaN', '-1.0', '', '-1'], drop_cols=['applyid', 'apply_no'])

print('output.....')
df1.to_csv(r'E:\dev_code\model_ori1120_08_1.csv', index=False)
print('done.....')