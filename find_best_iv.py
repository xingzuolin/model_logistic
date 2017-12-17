#!/usr/bin/env python
# @Author  : Jun
# @Time    : 2017/12/17 15:15
# @File    : find_best_iv.py

import numpy as np
import pandas as pd
import re
import math
import time
import itertools
from itertools import combinations
from numpy import array
from math import sqrt
from multiprocessing import Pool


def path_df(path, factor_name, tag):
    data = pd.read_csv(path)
    data[factor_name] = data[factor_name].astype(str).map(lambda x: x.upper())
    data[factor_name] = data[factor_name].apply(lambda x: re.sub(' ', 'MISSING', x))
    data = data[data[tag].isin([0, 1])]
    return data.ix[:, [factor_name, tag]]


def verify_factor(x):
    if x in ['NA', 'NAN', '', ' ', 'MISSING', 'NONE', 'NULL']:
        return 'NAN'
    if re.match('^\-?\d*\.?\d+$', x):
        x = float(x)
    return x


def sum_by_gb(data, var_name, tag, bad_name, good_name):
    data_df = data.drop(data[data[tag].isnull()].index)
    if len(data_df) == 0:
        print('Error: the wrong data')
        return data_df
    tmp = data_df[data_df[tag] > 1]
    if len(tmp) != 0:
        print('Error: there exits the number bigger than one in the data')
        data_df = pd.DataFrame()
        return data_df
    if tag != '':
        try:
            data_df[tag] = data_df[tag].astype(int)
        except:
            print('Error: the data is wrong')
            data_df = pd.DataFrame()
            return data_df
    data_df = data_df[tag].groupby([data_df[var_name], data_df[tag]]).count().unstack().reset_index().fillna(0)
    data_df.columns = [var_name, good_name, bad_name]
    data_df[var_name] = data_df[var_name].map(verify_factor)
    data_df = data_df.sort_values(by=[var_name], ascending=True)
    data_df.index = range(len(data_df))
    data_df[var_name] = data_df[var_name].astype(str)
    return data_df


def find_best_bin(path, data=pd.DataFrame(), var_name='', tag='', total_name='total', bad_name='bad',
                  good_name='good', not_in_list=[]):
    # none_list = ['NA', 'NAN', '', ' ', 'MISSING', 'NONE', 'NULL']
    if path != '':
        data = path_df(path, var_name, tag)
    elif len(data) == 0:
        print('Error: there is no data!')
        return data
    data[var_name] = data[var_name].apply(lambda x: str(x).upper())
    data_df = sum_by_gb(data, var_name, tag, bad_name, good_name)
    if len(data_df) == 0:
        return data_df
    tot_good_cnt = data_df[good_name].sum()
    tot_bad_cnt = data_df[bad_name].sum()
    tot_cnt = tot_bad_cnt + tot_good_cnt
    if not_in_list:
        not_name = [str(x).upper() for x in not_in_list]
        na_df = data_df[data_df[var_name].isin(not_name)]
        not_na_df = data_df.drop(data_df[data_df[var_name].isin(not_name)].index)
    else:
        not_na_df = data_df
        na_df = pd.DataFrame()
    if len(data_df) == 0:
        print('Error: the data is wrong')
        data = pd.DataFrame()
        return data
    return not_na_df
if __name__ == '__main__':
    print(find_best_bin(path=r'F:\Quark\dev_code\model_data.csv', var_name='antiscore', tag='acquisition_is_bad'
      , not_in_list=['NaN', '-1.0', '', '-1']))

