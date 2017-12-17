#!/usr/bin/env python
# @Author  : Jun
# @Time    : 2017/12/17 14:38
# @File    : run_scripts.py

import d01_read_data


# def cut_bin_best_ks(data_df, drop_cols, save_path, tag, enable_best_ks=None, mono=True):
#     pth_ = d01_read_data.data_path('F:\Quark\dev_code', save_path)
#     data = data_df.copy()
#     all_col_vars = data.columns.tolist()
#     keep_vars = list(set(all_col_vars)-set(drop_cols))
#     for var_col in keep_vars:
#         df_rst =

def program_steps(data_df, tag, key, drop_cols, save_path, data_size, enable_best_ks=None):
    print('start to prepare the path')
    pth_ = d01_read_data.data_path('F:\Quark\dev_code', save_path)
    print('start to split the data')
    train_df, test_df = d01_read_data.data_split(data_df, tag, data_size)
    print('start to process the data')
    return train_df, test_df

path = r'F:\Quark\dev_code\model_data.csv'
data = d01_read_data.read_csv_data(path)
df_train, df_test = program_steps(data, tag='acquisition_is_bad', key='appid',\
                                  drop_cols=[], save_path='test',data_size=0.3)
print(df_train.shape)
print(df_train['acquisition_is_bad'].value_counts())
print(df_test.shape)
print(df_test['acquisition_is_bad'].value_counts())
