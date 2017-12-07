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
from d01_read_data import read_gz_csv,change_vars_type


def col_group(x):
    if x.startswith('call'):
        return 'call'
    elif x.startswith('net'):
        return 'net'
    elif x.startswith('sms'):
        return 'sms'
    return 'other'

# gzip_data = read_gzip_data(r'E:\dev_code\model_ori1120_08.gz')
# df = pd.DataFrame(gzip_data)
# df_col = df.columns
# col_names = map(lambda x: col_group(x), df_col)
# col_zip = zip(col_names, df_col)
# df1 = df.ix[:, [col2 for (col1, col2) in col_zip if col1 in ('other', 'call')]]


##########################
#
# 读取压缩包中csv文件
#
#########################

path = r'F:\Quark\dev_code\model_ori1120_01_s.gz'

df = read_gz_csv(path)

print(df.shape)
df1 = change_vars_type(df,['APPID', 'APPLYNO', 'APPLYTIME', 'APPLYDATE', 'SUBMITAPPLYDATE', 'SUBMITAPPLYTIME'])
print(df.dtypes)

# df1 = pd.read_csv(r'E:\dev_code\model_ori1120_08.csv', encoding='GBK')
# miss_df = preprocess_var.missing_vars(df, missing_flag=['NaN', '-1.0', '', '-1'], drop_cols=['applyid', 'apply_no'])
#
print('output.....')
# miss_df.to_csv(r'F:\Quark\dev_code\model_ori1120_08_1.csv', index=False)
print('done.....')
