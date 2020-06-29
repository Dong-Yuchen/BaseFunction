#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 3/27/2018 10:39 AM
# @Author  : jiagnzhenkai
# @File    : common.py
# @Software: PyCharm
"""
通用函数
"""

import pandas as pd
import numpy as np
import datetime
import calendar
import math
import os


def date_to_str(date):
    """将日期转换为 yyyymmdd 的字符串形式"""
    return datetime.datetime.strftime(date, '%Y%m%d')


def str_to_date(string):
    """将 yyyymmdd 格式的字符串解析为日期格式"""
    return datetime.datetime.strptime(string, '%Y%m%d')


def date_format_suntime(date_string):
    """将某种格式的日期字符串解析为合适朝阳永续数据库的日期格式"""
    return str(pd.to_datetime(date_string).date())

def code_format_suntime(code_string):
    """将 000001.SZ 格式的股票代码解析为合适朝阳永续数据库的格式"""
    return code_string[:6]
    
def code_from_suntime(code_sumtime):
    """将 000001 格式的股票代码转为 000001.SZ 的格式"""
    return code_sumtime+'.SZ' if int(code_sumtime[0]) < 6 else code_sumtime+'.SH'


def half_life(length=252,half_life=42):
    """半衰期参数计算
    parameters:
    -------------
    length: 时间长度
    half-life : 半衰期时间长度，即经过half-life后，系数衰减为原来0.5
    
    returns:
    --------------
    return a series
    """
    ind = pd.Series(range(length))-length+1
    res = ind.apply(lambda x:math.pow(2,x/half_life))
    res = res/res.sum()  #归一化处理
    return res
