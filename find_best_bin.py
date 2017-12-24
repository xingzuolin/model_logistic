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


def get_max_ks(data_df, start, end, bad_name, good_name):
    ks = -1
    if start == end:
        return ks
    bad = data_df.ix[start:end, bad_name]
    good = data_df.ix[start:end, good_name]
    bad_good_cum = list(abs(np.cumsum(bad/sum(bad)) - np.cumsum(good/sum(good))))
    if bad_good_cum:
        ks = start + bad_good_cum.index(max(bad_good_cum))
    # print('---------------------')
    # print([(start + i, j) for i, j in enumerate(bad_good_cum)])
    # print(ks)
    return ks


def find_ks_points(data_df, start, end, bin_num, bad_name, good_name, counts):
    ks_points = []
    if counts >= bin_num or len(ks_points) >= pow(2, bin_num-1):
        return []
    ks_point_idx = get_max_ks(data_df, start, end, bad_name, good_name)
    if ks_point_idx >= 0:
        if ks_point_idx != -1:
            ks_up = find_ks_points(data_df, start, ks_point_idx, bin_num, bad_name, good_name, counts+1)
        else:
            ks_up = []
        ks_down = find_ks_points(data_df, ks_point_idx+1, end, bin_num, bad_name, good_name, counts+1)
    else:
        ks_up = []
        ks_down = []
    ks_points = ks_up + [ks_point_idx] + ks_down
    return ks_points


def path_df(path, factor_name, tag):
    data = pd.read_csv(path)
    data[factor_name] = data[factor_name].astype(str).map(lambda x: x.upper())
    data[factor_name] = data[factor_name].apply(lambda x: re.sub(' ', 'MISSING', x))
    data = data[data[tag].isin([0, 1])]
    return data.ix[:, [factor_name, tag]]


def get_bin_points(data_df, bin_num, rate, bad_name, good_name):
    ks_point = list(set(find_ks_points(data_df, 0, len(data_df)-1, bin_num, bad_name, good_name, 1)))
    drop_na_point = list(filter(lambda x: x != -1, ks_point))
    ks_all_points = [0] + drop_na_point + [len(data_df)-1]
    return ks_all_points


def ks_points_combination(points_list, data_df, bin_num):
    t1 = 0
    t2 = len(data_df)-1
    k_list = points_list[1:len(data_df)-1]
    combine = []
    if len(points_list)-2 < bin_num:
        c = len(points_list) - 2
    else:
        c = bin_num - 1
    list_1 = list(itertools.combinations(k_list, c))
    if list_1:
        combine = list(map(lambda x: sorted(x + (t1-1,t2)), list_1))
    return combine


def cal_iv(data_df, items, bad_name, good_name, rate, tot_cnt):
    iv_tmp = 0
    tot_rate = [sum(data_df.ix[x[0]:x[1], bad_name]+data_df.ix[x[0]:x[1], good_name])*1.0/tot_cnt for x in items]
    if [k for k in tot_rate if k < rate]:
        return 0
    bad_tmp = array(list(map(lambda x: sum(data_df.ix[x[0]:x[1], bad_name]), items)))
    good_tmp = array(list(map(lambda x: sum(data_df.ix[x[0]:x[1], good_name]), items)))
    bad_rate_tmp = bad_tmp*1.0/(bad_tmp + good_tmp)
    if 0 in bad_tmp or 0 in good_tmp:
        return 0
    good_pct_tmp = good_tmp*1.0/sum(data_df[good_name])
    bad_pct_tmp = bad_tmp*1.0/sum(data_df[bad_name])
    woe_tmp = list(map(lambda x: math.log(x, math.e), good_pct_tmp/bad_pct_tmp))
    if sorted(woe_tmp, reverse=False) == woe_tmp and sorted(bad_rate_tmp, reverse=True) == list(bad_rate_tmp):
        iv_tmp = sum(woe_tmp*(good_pct_tmp - bad_pct_tmp))
    elif sorted(woe_tmp, reverse=True) == woe_tmp and sorted(bad_rate_tmp, reverse=False) == list(bad_rate_tmp):
        iv_tmp = sum(woe_tmp*(good_pct_tmp - bad_pct_tmp))
    return iv_tmp


def choose_best_combine(data_df, combine, bad_name, good_name, rate, tot_cnt):
    tmp = [0]*len(combine)
    for i, item in enumerate(combine):
        tmp[i] = tuple(zip(map(lambda x: x+1, item[0:len(item)-1]), item[1:]))
    iv_list = list(map(lambda x: cal_iv(data_df, x, bad_name, good_name, rate, tot_cnt), tmp))
    iv_max = max(iv_list)
    idx_max = iv_list.index(iv_max)
    combine_max = tmp[idx_max]
    return combine_max


def verify_woe(x):
    if re.match('^\d*\.?\d+$', str(x)):
        return x
    else:
        return 0


def best_df(data_df, items, na_df, var_name, bad_name, good_name,tot_cnt,tot_good_cnt,tot_bad_cnt):
    df_ = pd.DataFrame()
    if items:
        q_cut = map(lambda x: '(' + str(data_df.ix[x[0], var_name]) + ',' +
                              str(data_df.ix[x[1], var_name]) + ')', items)
        bad0 = map(lambda x: sum(data_df.ix[x[0]:x[1], bad_name]), items)
        good0 = map(lambda x: sum(data_df.ix[x[0]:x[1], good_name]), items)
        if len(na_df) > 0:
            q_cut = array(list(q_cut) + list(map(lambda x: '(' + str(x) + ',' + str(x) + ')', list(na_df[var_name]))))
            bad0 = array(list(bad0) + list(na_df[bad_name]))
            good0 = array(list(good0) + list(na_df[good_name]))
        else:
            q_cut = array(list(q_cut))
            bad0 = array(list(bad0))
            good0 = array(list(good0))
        total0 = bad0 + good0
        total_per0 = total0 * 1.0 / tot_cnt
        bad_rate0 = bad0 * 1.0 / total0
        good_rate0 = 1 - bad_rate0
        good_per0 = good0 * 1.0 / tot_good_cnt
        bad_per0 = bad0 * 1.0 / tot_bad_cnt
        df0 = pd.DataFrame(list(zip(q_cut, total0, bad0, good0, total_per0, good_rate0, bad_rate0, good_per0, bad_per0)),
                           columns=[var_name, 'tot_cnt', 'bad_cnt', 'good_cnt', 'tot_pct', 'Good_Rate', 'Bad_Rate',
                                    'Good_Pcnt', 'Bad_Pcnt'])
        df0 = df0.sort_values(by='Bad_Rate', ascending=False)
        df0.index = range(len(df0))
        bad_per0 = array(list(df0['Bad_Pcnt']))
        good_per0 = array(list(df0['Good_Pcnt']))
        bad_rate0 = array(list(df0['Bad_Rate']))
        good_rate0 = array(list(df0['Good_Rate']))
        bad_cum = np.cumsum(bad_per0)
        good_cum = np.cumsum(good_per0)
        woe0 = list(map(lambda x: math.log(x, math.e), good_per0 / bad_per0))
        if 'inf' in woe0:
            woe0 = list(map(lambda x: verify_woe(x), woe0))
        iv0 = woe0 * (good_per0 - bad_per0)
        gini = 1 - pow(good_rate0, 2) - pow(bad_rate0, 2)
        df0['Bad_Cum'] = bad_cum
        df0['Good_Cum'] = good_cum
        df0["WOE"] = woe0
        df0["IV"] = iv0
        df0['Gini'] = gini
        df0['KS'] = round(abs(df0['Good_Cum'] - df0['Bad_Cum']), 4)
        df0.drop('Good_Rate', axis=1, inplace=True)
    return df0


def woe_information(not_na_df, na_df, bin_num, rate, var_name, bad_name, good_name,tot_cnt,tot_good_cnt,tot_bad_cnt):
    bin_n = list(range(bin_num + 1))
    bin_n = bin_n[::-1]
    bin_point_list = get_bin_points(not_na_df, bin_num, rate, bad_name, good_name)
    if not bin_point_list:
        df_woe = pd.DataFrame()
        print('warning: the data is cannot get bins')
        return df_woe
    df_woe = pd.DataFrame()
    for idx in bin_n[:bin_num-1]:
        combine = ks_points_combination(bin_point_list, not_na_df, idx)
        best_combine = choose_best_combine(not_na_df, combine, bad_name, good_name, rate, tot_cnt)
        df_woe = best_df(not_na_df, best_combine, na_df, var_name, bad_name, good_name, tot_cnt, tot_good_cnt, tot_bad_cnt)
        if len(df_woe) > 0:
            print('tot_cnt:', len(df_woe))
            print('iv_max_all:', sum(df_woe['IV']))
            print('max_ks:', max(df_woe['KS']))
            return df_woe
    if len(df_woe) == 0:
        print('warning: the data is cannot get bins')
        return df_woe


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
    # data_df = data_df.sort_values(by=[var_name], ascending=True)
    # data_df.index = range(len(data_df))
    data_df[var_name] = data_df[var_name].astype(str)
    return data_df


def find_best_bin(path, data=pd.DataFrame(), var_name='', tag='', total_name='total', bad_name='bad',rate=0.05,
                  good_name='good', bin_num=5, not_in_list=[], value_type=True):
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
    if len(not_na_df) == 0:
        print('Error: the data is wrong')
        data = pd.DataFrame()
        return data
    if value_type:
        not_na_df[var_name] = not_na_df[var_name].map(verify_factor)
        type_dif = set([type(i) for i in list(not_na_df[var_name])])
        if len(type_dif) > 1:
            str_df = not_na_df[var_name].apply(lambda x: type(x) == str)
            float_df = not_na_df[var_name].apply(lambda x: type(x) == float)
            str_df = str_df.sort_values(by=var_name)
            float_df = float_df.sort_values(by=var_name)
            not_na_df = str_df.append(float_df)
        else:
            not_na_df = not_na_df.sort_values(by=var_name)
    not_na_df['bad_rate'] = not_na_df[bad_name]*1.0/(not_na_df[bad_name] + not_na_df[good_name])
    not_na_df[var_name] = not_na_df[var_name].astype(str)
    not_na_df.index = range(len(not_na_df))
    bin_df = woe_information(not_na_df, na_df, bin_num, rate, var_name, bad_name, good_name, tot_cnt, tot_good_cnt,
                             tot_bad_cnt)
    return bin_df
if __name__ == '__main__':
    # print(find_best_bin(path=r'F:\Quark\dev_code\model_data.csv', var_name='call_2way_0106_days_sum_j1m', tag='acquisition_is_bad',
    #                     not_in_list=['NaN', '-1.0', '', '-1']))
    print(find_best_bin(path=r'F:\Quark\dev_code\model_data.csv', var_name='score',
                        tag='acquisition_is_bad', not_in_list=['NaN', '-1.0', '', '-1']))

