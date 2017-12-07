#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/12/6 17:36
# @Author  : Jun
# @File    : preprocess_var.py

import pandas as pd


def missing_same_flag(x_value, missing_flag):
    if str(x_value) in missing_flag:
        return -1
    else:
        return x_value


def missing_vars(in_data, missing_flag=[], drop_cols=[]):
    in_data.columns = [k.upper() for k in in_data.columns]
    col_names = in_data.columns
    drop_cols = [k.upper() for k in drop_cols]
    merge_col_names = list(set(col_names) - set(drop_cols))
    data_rows = in_data.shape[0]
    missing_pct = []
    for i in merge_col_names:
        if i in col_names:
            try:
                data_md = in_data[i].fillna(-1).apply(lambda x: missing_same_flag(x, missing_flag))
                missing_cnt = len(data_md[data_md == -1])
                dis_cnt = len(data_md.unique())
                vars_mss_pct = round(missing_cnt/float(data_rows), 2)
                missing_pct.append([i, data_rows, vars_mss_pct, dis_cnt])
            except:
                pass
    return pd.DataFrame(missing_pct, columns=['var_name', 'total_cnt', 'missing_pct', 'unique'])



