"""
winddataapi 提供常用的WIND数据访问接口
"""

from datetime import datetime, timedelta
import time
from functools import reduce

import pandas as pd
import numpy as np

from utils.common import date_to_str, str_to_date
from sqlalchemy import create_engine

host = '192.168.201.210:10087'
username = 'shixisheng'
password = 'password1'
db = 'wind'

wind_engine = create_engine('mysql://' + username + ':' + password + '@' + host 
                            + '/' + db+'?charset=utf8')


#%% 交易日期函数
def get_trading_days(start_date=None, end_date=None):
    """获取指定区间内的交易日列表

    Parameters
    ----------
    start_date : str
        开始日期，格式为 yyyymmdd 字符串，为 None 时从最早的开始
    end_date : str
        结束如期，格式为 yyyymmdd 字符串，为 None 时截止到今天

    Returns
    -------
    list
    """
    if start_date is None:
        start_date = '19491001'
    if end_date is None:
        end_date = date_to_str(datetime.today())

    assert end_date >= start_date, 'until date must be greater than since date'

    sql = """
        select distinct TRADE_DAYS as date
        from ASHARECALENDAR
        where TRADE_DAYS between '{0}' and '{1}'
        order by TRADE_DAYS;
        """.format(start_date, end_date)
        
    dates = pd.read_sql(sql, wind_engine,
                       parse_dates=['date'],
                       index_col='date')
    dates = [date_to_str(i) for i in dates.index]
    return dates


def get_previous_trade_dt(date=None, count=1):
    """
    获取指定日期之前 count 个交易日的日期

    Parameters
    ----------
    date : str
        结束如期，格式为 yyyymmdd 字符串，为 None 时截至到今天
    count : int
        回溯天数，默认为 1，表示上一个交易日

    Returns
    -------
    str
        回溯之后的交易日，格式为 yyyymmdd 字符串

    Notes
    -----
    当回溯日期过长时，None
    """
    if date is None:
        date = date_to_str(datetime.today())
    
    assert count > 0, "回溯日期必须大于 0"

    sql = """
        select distinct TRADE_DAYS as date
        from ASHARECALENDAR
        where TRADE_DAYS < '{0}'
        order by TRADE_DAYS desc limit 1 offset {1}
    """.format(date, count - 1)

    s = pd.read_sql(sql, wind_engine)['date']

    if not s.empty:
        return s.iloc[0]


def get_subsequent_trade_dt(date=None, count=1):
    """
    获取指定日期之后 count 个交易日的日期

    Parameters
    ----------
    date : str
        结束如期，格式为 yyyymmdd 字符串，为 None 时截至到今天
    count : int
        向后天数，默认为 1，表示下一个交易日

    Returns
    -------
    str
        格式为 yyyymmdd 字符串

    Notes
    -----
    当向后日期过长时，返回 None
    """
    if date is None:
        date = date_to_str(datetime.today())
        
    assert count > 0, "回溯日期必须大于 0"

    sql = """
        select distinct TRADE_DAYS as date
        from ASHARECALENDAR
        where TRADE_DAYS > '{0}'
        order by TRADE_DAYS limit 1 offset {1}
    """.format(date, count - 1)

    s = pd.read_sql(sql, wind_engine)['date']

    if not s.empty:
        return s.iloc[0]

#%% 获取股票基础数据
def get_ipo_date(codes=None):
    """
    从数据库中获取公司上市日期
    Parameters
    ----------
    codes : list
        股票代码 list
    
    Returns
    -------
    pd.Series
        index 为股票代码
    """
    if codes is None:
        codes = get_all_stocks()
    
    sql = """select S_INFO_WINDCODE as asset, S_IPO_LISTDATE as date
        from AShareIPO
        where S_IPO_LISTDATE is not null and S_INFO_WINDCODE IN {0}
        order by S_INFO_WINDCODE
        """.format(tuple(codes))

    data = pd.read_sql(sql, wind_engine, parse_dates=['date'])
    data['date'] = [date_to_str(i) for i in data['date']]
    
    return data.set_index(['asset'])['date']

#%% 获取交易股票代码
def get_all_stocks(start_date=None, end_date=None):
    """
    获取指定区间内的交易过的股票代码列表，包括停牌，退市等

    Parameters
    ----------
    start_date : str
        开始日期，格式为 yyyymmdd 字符串
    end_date : str
        结束如期，格式为 yyyymmdd 字符串

    Returns
    -------
    list
        股票列表
    """
    if start_date is None:
        start_date = '19491001'
    if end_date is None:
        end_date = date_to_str(datetime.today())

    assert end_date >= start_date, 'until date must be greater than since date'

    sql = """
        select distinct S_INFO_WINDCODE as asset
        from ASHAREEODPRICES
        where TRADE_DT between '{0}' and '{1}'
        order by S_INFO_WINDCODE;
        """.format(start_date, end_date)

    return pd.read_sql(sql, wind_engine)['asset'].tolist()


def get_active_stocks(trade_dt=None):
    """
    获取指定日期正常交易的股票

    Parameters
    ----------
    trade_dt : str
        交易日期，格式为 yyyymmdd 字符串

    Returns
    -------
    list
        股票列表
    """

    dates = get_trading_days()
    if trade_dt:
        assert trade_dt in dates, '指定日期非交易日'
    else:
        trade_dt = dates[-1] 

    sql = """
        select distinct S_INFO_WINDCODE as asset
        from ASHAREEODPRICES
        where TRADE_DT = '{0}' and S_DQ_TRADESTATUS != '停牌'
        order by S_INFO_WINDCODE;
        """.format(trade_dt)

    return pd.read_sql(sql, wind_engine)['asset'].tolist()


def get_index_members(code = '000300.SH', trade_dt = None):
    '''
    获取date日指数成分股
    
    Parameters
    ----------
    code : str
        指数代码 str
    trade_dt : str
        交易日期，格式为 yyyymmdd 字符串
        
    Returns
    -------
    list
        股票列表
    '''
    if trade_dt is None:
        trade_dt = date_to_str(datetime.today())
        
    sql = """select S_CON_INDATE, S_INFO_WINDCODE as asset,S_CON_WINDCODE,S_CON_OUTDATE
                 from AINDEXMEMBERS
                 where S_INFO_WINDCODE = '{0}' and S_CON_INDATE <= '{1}'
                 """.format(code, trade_dt)
    members = pd.read_sql(sql, wind_engine)
    tomorrow = date_to_str(datetime.today()+timedelta(days=1))
    members.fillna(tomorrow,inplace=True)
    members = members[members['S_CON_OUTDATE']>trade_dt]
    
    return members['S_CON_WINDCODE'].tolist()

#%% 获取行业分类数据
def get_industries(codes=None, start_date=None, end_date=None, method='sw', level=1):
    """
    获取股票指定日期间的行业信息

    Parameters
    ----------
    codes : list
        股票代码 list
    start_date : str
        开始日期，yyyymmdd 字符串
    end_date : str
        结束日期，yyyymmdd 字符串
    method : str, sw or citic
        分类方法，默认为为申万行业分类
    level : int
        行业级别，默认为一级行业

    Returns
    -------
    pd.Series
        行业信息，index 为 [date, asset]
    
    注：
    """
    if start_date is None:
        start_date = '20021231'
    if end_date is None:
        end_date = date_to_str(datetime.today())
    if codes is None:
        codes = get_all_stocks(start_date, end_date)
    
    if method == 'citic':
        sql = """select a.entry_dt as date, a.s_info_windcode as asset,
                    b.Industriesname as industry
                from ASHAREINDUSTRIESCLASSCITICS a,
                      ASHAREINDUSTRIESCODE  b
                where substring(a.citics_ind_code, 1, 2 * ({0} + 1)) =
                substring(b.IndustriesCode, 1, 2 * ({0} + 1))
                    and b.levelnum = 1 + {0} and a.s_info_windcode IN {1}
                order by S_INFO_WINDCODE, ENTRY_DT, REMOVE_DT
                """.format(level,tuple(codes))

    elif method == 'sw':
        sql = """select a.entry_dt as date, a.s_info_windcode as asset,
                        b.Industriesname as industry
                from ashareswindustriesclass a,
                      ASHAREINDUSTRIESCODE  b
                where substring(a.sw_ind_code, 1, 2 * ({0} + 1)) =
                substring(b.IndustriesCode, 1, 2 * ({0} + 1))
                    and b.levelnum = 1 + {0} and a.s_info_windcode IN {1}
                order by S_INFO_WINDCODE, ENTRY_DT, REMOVE_DT
                """.format(level,tuple(codes))

    # 所有行业信息
    # index 为 date，一列一个股票
    industries = pd.read_sql(sql, wind_engine,
                             parse_dates=['date'],
                             index_col=['date', 'asset'])
    industries = industries['industry'].unstack().fillna(np.nan).ffill()#.bfill()
    industries.index = [date_to_str(i) for i in industries.index]

    # 获取区间交易日
    # 中信行业分类从 20030101 开始
    dts = get_trading_days(start_date, end_date)

    # 设置指定区间每个股票每一个交易日的行业分类
    # reindex 时也必须 ffill，因为存在行业信息在非交易日的情况
    industries = industries.reindex(dts, method='ffill', copy=False)
    industries.index = pd.to_datetime(industries.index)

    return industries.stack().rename('industry')


#%% 获取行情数据
def get_stock_prices(codes=None, start_date=None, end_date=None, fields=None, paused=False):
    """
    获取股票指定区间内行情数据

    Parameters
    ----------
    codes : list or str
        股票代码 list 或 单个代码 str
    start_date : str
        字符串类型日期 yyyymmdd，譬如 '20150101'，表示起始日期
    end_date : str
        字符串类型日期 yyyymmdd，譬如 '20160101'，表示结束日期
    fields : list
        字符串list, 选择要获取的行情数据字段, 默认是None(表示['S_DQ_PRECLOSE','S_DQ_OPEN','S_DQ_HIGH','S_DQ_LOW','S_DQ_CLOSE','S_DQ_PCTCHANGE','S_DQ_VOLUME','S_DQ_AMOUNT']这几个标准字段)
        相对应：前收盘价、开盘价、最高价、最低价、收盘价、涨跌幅(%)、成交量(手)、成交金额(千元)
        其他可选：['S_DQ_ADJPRECLOSE','S_DQ_ADJOPEN','S_DQ_ADJHIGH','S_DQ_ADJLOW','S_DQ_ADJCLOSE','S_DQ_ADJFACTOR','S_DQ_AVGPRICE','S_DQ_TRADESTATUS']
        相对应：复权前收盘价、复权开盘价、复权最高价、复权最低价、复权收盘价、复权因子、均价(VWAP)、交易状态( -1:交易 -2:待核查 0:停牌 XD:除息 XR:除权 DR:除权除息 N:上市首日)
    paused : bool
        布尔型变量，为False时数据填充为前非停牌日收盘价，为Ture时跳过停牌数据

    Returns
    -------
    pd.DataFrame
        index 为 [date, asset] , columns 为 fields
    """
    if fields is None:
        fields = ['S_DQ_PRECLOSE','S_DQ_OPEN','S_DQ_HIGH','S_DQ_LOW','S_DQ_CLOSE','S_DQ_PCTCHANGE','S_DQ_VOLUME','S_DQ_AMOUNT']
    if start_date is None:
        start_date = '19901001'
    if end_date is None:
        end_date = date_to_str(datetime.today())
    if codes is None:
        codes = get_all_stocks(start_date, end_date)
    
    fields_sql = ','.join(fields)
    
    if paused:
        paused_sql = '''and S_DQ_TRADESTATUS != '停牌' '''
    else:
        paused_sql = ''
    
    sql1 = """select TRADE_DT as date, S_INFO_WINDCODE as asset,{0}
                 from ASHAREEODPRICES
                 where TRADE_DT >= '{1}' and TRADE_DT <= '{2}' {3}
                 """.format(fields_sql, start_date, end_date, paused_sql)
    
    #codes输入类型影响
    if isinstance(codes, list):
        sql2 = """ and S_INFO_WINDCODE IN {0} 
                 order by TRADE_DT, S_INFO_WINDCODE
                 """.format(tuple(codes))
    elif isinstance(codes, str):
        sql2 = """ and S_INFO_WINDCODE = '{0}' 
                 order by TRADE_DT, S_INFO_WINDCODE
                 """.format(codes)
    else:
        print('Codes格式有误,应为股票代码list或单个代码str,请检查!')
        return
    
    prices = pd.read_sql(sql1+sql2, wind_engine,
                         index_col=['date', 'asset'],
                         parse_dates=['date'])
    
#    prices.columns = [i.split('_')[-1].lower() for i in prices.columns]
    
    return prices    


def get_month_prices(codes=None, start_date=None, end_date=None, fields=None):
    """
    获取股票指定区间内行情数据

    Parameters
    ----------
    codes : list or str
        股票代码 list 或 单个代码 str
    start_date : str
        字符串类型日期 yyyymmdd，譬如 '20150101'，表示起始日期
    end_date : str
        字符串类型日期 yyyymmdd，譬如 '20160101'，表示结束日期
    fields : list
        字符串list, 选择要获取的行情数据字段, 默认是None(表示['S_MQ_PCTCHANGE','S_MQ_TURN','S_MQ_AMOUNT']这几个标准字段)
        相对应：月收益率(%)、月换手率(合计)(%)、月成交金额(万元)

    Returns
    -------
    pd.DataFrame
        index 为 [date, asset] , columns 为 fields
    """
    if fields is None:
        fields = ['S_MQ_PCTCHANGE','S_MQ_TURN','S_MQ_AMOUNT']
    if start_date is None:
        start_date = '19901001'
    if end_date is None:
        end_date = date_to_str(datetime.today())
    if codes is None:
        codes = get_all_stocks(start_date, end_date)
    
    fields_sql = ','.join(fields)
    
    sql1 = """select TRADE_DT as date, S_INFO_WINDCODE as asset,{0}
                 from AShareMonthlyYield
                 where TRADE_DT >= '{1}' and TRADE_DT <= '{2}'
                 """.format(fields_sql, start_date, end_date)
    
    #codes输入类型影响
    if isinstance(codes, list):
        sql2 = """ and S_INFO_WINDCODE IN {0} 
                 order by TRADE_DT, S_INFO_WINDCODE
                 """.format(tuple(codes))
    elif isinstance(codes, str):
        sql2 = """ and S_INFO_WINDCODE = '{0}' 
                 order by TRADE_DT, S_INFO_WINDCODE
                 """.format(codes)
    else:
        print('Codes格式有误,应为股票代码list或单个代码str,请检查!')
        return
    
    prices = pd.read_sql(sql1+sql2, wind_engine,
                         index_col=['date', 'asset'],
                         parse_dates=['date'])
        
    return prices

def get_index_prices(codes=None, start_date=None, end_date=None, fields=None):
    """
    获取某个指数一段时间内的数据

    Parameters
    ----------
    codes : list or str
        股票代码 list 或 单个代码 str
    start_date : str
        字符串类型日期 yyyymmdd，譬如 '20150101'，表示起始日期
    end_date : str
        字符串类型日期 yyyymmdd，譬如 '20160101'，表示结束日期
    fields : list
        字符串list, 选择要获取的行情数据字段, 默认是None(表示['S_DQ_PRECLOSE','S_DQ_OPEN','S_DQ_HIGH','S_DQ_LOW','S_DQ_CLOSE','S_DQ_PCTCHANGE','S_DQ_VOLUME','S_DQ_AMOUNT']这几个标准字段)
        相对应：前收盘价、开盘价、最高价、最低价、收盘价、涨跌幅(%)、成交量(手)、成交金额(千元)
        其他可选：['S_DQ_ADJPRECLOSE','S_DQ_ADJOPEN','S_DQ_ADJHIGH','S_DQ_ADJLOW','S_DQ_ADJCLOSE','S_DQ_ADJFACTOR','S_DQ_AVGPRICE','S_DQ_TRADESTATUS']
        相对应：复权前收盘价、复权开盘价、复权最高价、复权最低价、复权收盘价、复权因子、均价(VWAP)、交易状态( -1:交易 -2:待核查 0:停牌 XD:除息 XR:除权 DR:除权除息 N:上市首日)
    
    Returns
    -------
    pd.DataFrame
        index 为 [date, asset] , columns 为 fields
    """
    if fields is None:
        fields = ['S_DQ_PRECLOSE','S_DQ_OPEN','S_DQ_HIGH','S_DQ_LOW','S_DQ_CLOSE','S_DQ_PCTCHANGE','S_DQ_VOLUME','S_DQ_AMOUNT']
    if start_date is None:
        start_date = '19901001'
    if end_date is None:
        end_date = date_to_str(datetime.today())
    if codes is None:
        codes = ['000016.SH','000300.SH','000905.SH','000852.SH']
   
    fields_sql = ','.join(fields)
    
    sql1 = """select TRADE_DT as date, S_INFO_WINDCODE as asset,{0}
                 from AIndexEodPrices
                 where TRADE_DT >= '{1}' and TRADE_DT <= '{2}'
                 """.format(fields_sql, start_date, end_date)
    
    #codes输入类型影响
    if isinstance(codes, list):
        sql2 = """ and S_INFO_WINDCODE IN {0} 
                 order by TRADE_DT, S_INFO_WINDCODE
                 """.format(tuple(codes))
    elif isinstance(codes, str):
        sql2 = """ and S_INFO_WINDCODE = '{0}' 
                 order by TRADE_DT, S_INFO_WINDCODE
                 """.format(codes)
    else:
        print('Codes格式有误,应为股票代码list或单个代码str,请检查!')
        return
    
    prices = pd.read_sql(sql1+sql2, wind_engine,
                         index_col=['date', 'asset'],
                         parse_dates=['date'])

    return prices

def get_prices(codes=None, start_date=None, end_date=None, fields=None, paused=False, index=False):
    """
    获取股票指定区间内行情数据

    Parameters
    ----------
    codes : list or str
        股票代码 list 或 单个代码 str
    start_date : str
        字符串类型日期 yyyymmdd，譬如 '20150101'，表示起始日期
    end_date : str
        字符串类型日期 yyyymmdd，譬如 '20160101'，表示结束日期
    fields : list
        字符串list, 选择要获取的行情数据字段, 默认是None(表示['S_DQ_PRECLOSE','S_DQ_OPEN','S_DQ_HIGH','S_DQ_LOW','S_DQ_CLOSE','S_DQ_PCTCHANGE','S_DQ_VOLUME','S_DQ_AMOUNT']这几个标准字段)
        相对应：前收盘价、开盘价、最高价、最低价、收盘价、涨跌幅(%)、成交量(手)、成交金额(千元)
        其他可选：['S_DQ_ADJPRECLOSE','S_DQ_ADJOPEN','S_DQ_ADJHIGH','S_DQ_ADJLOW','S_DQ_ADJCLOSE','S_DQ_ADJFACTOR','S_DQ_AVGPRICE','S_DQ_TRADESTATUS']
        相对应：复权前收盘价、复权开盘价、复权最高价、复权最低价、复权收盘价、复权因子、均价(VWAP)、交易状态( -1:交易 -2:待核查 0:停牌 XD:除息 XR:除权 DR:除权除息 N:上市首日)
    paused : bool
        布尔型变量，为False时数据填充为前非停牌日收盘价，为Ture时跳过停牌数据
    index : bool
        布尔型变量，为False时参数codes为股票，为Ture时为指数

    Returns
    -------
    pd.DataFrame
        index 为 [date, asset] , columns 为 fields
    """
    if fields is None:
        fields = ['S_DQ_PRECLOSE','S_DQ_OPEN','S_DQ_HIGH','S_DQ_LOW','S_DQ_CLOSE','S_DQ_PCTCHANGE','S_DQ_VOLUME','S_DQ_AMOUNT']
    if start_date is None:
        start_date = '19901001'
    if end_date is None:
        end_date = date_to_str(datetime.today())
    if codes is None:
        codes = ['000016.SH','000300.SH','000905.SH'] if index else get_all_stocks(start_date, end_date)

    fields_sql = ','.join(fields)
    
    #paused参数影响
    paused_sql = '''and S_DQ_TRADESTATUS != '停牌' ''' if paused else ''
    
    #index参数
    index_sql = 'AIndexEodPrices' if index else 'ASHAREEODPRICES'
    
    sql1 = """select TRADE_DT as date, S_INFO_WINDCODE as asset,{0}
                 from {1}
                 where TRADE_DT >= '{2}' and TRADE_DT <= '{3}' {4}
                 """.format(fields_sql, index_sql, start_date, end_date, paused_sql)
    
    #codes输入类型影响
    if isinstance(codes, list):
        sql2 = """ and S_INFO_WINDCODE IN {0} 
                 order by TRADE_DT, S_INFO_WINDCODE
                 """.format(tuple(codes))
    elif isinstance(codes, str):
        sql2 = """ and S_INFO_WINDCODE = '{0}' 
                 order by TRADE_DT, S_INFO_WINDCODE
                 """.format(codes)
    else:
        print('Codes格式有误,应为股票代码list或单个代码str,请检查!')
        return

    prices = pd.read_sql(sql1 + sql2, wind_engine,
                         index_col=['date', 'asset'],
                         parse_dates=['date'])
    
#    prices.columns = [i.split('_')[-1].lower() for i in prices.columns]
    
    return prices

#%% 获取股票衍生数据
def get_mv_data(codes=None, start_date=None, end_date=None, fields=None):
    """
    获取市值类数据

    Parameters
    ----------
    codes : list
        股票代码 list
    start_date : str
        字符串类型日期 yyyymmdd，譬如 '20150101'，表示起始日期
    end_date : str
        字符串类型日期 yyyymmdd，譬如 '20160101'，表示结束日期
    fields : list
        选择要获取的行情数据字段, 默认是None(表示['S_VAL_MV','S_DQ_MV','TOT_SHR_TODAY','FLOAT_A_SHR_TODAY'])
        相对应：当日总市值、当日流通市值、当日总股本、当日流通股本

    Returns
    -------
    pd.DataFrame
        index : [date, asset] , columns 为 fields
    """
    if fields is None:
        fields = ['S_VAL_MV','S_DQ_MV','TOT_SHR_TODAY','FLOAT_A_SHR_TODAY']
    if start_date is None:
        start_date = '20070101'
    if end_date is None:
        end_date = date_to_str(datetime.today())
    if codes is None:
        codes = get_all_stocks(start_date, end_date)

    assert end_date >= start_date, '获取时间区间起始时间晚于截至时间'
    
    fields_sql = ','.join(fields)
    dts = get_trading_days(start_date, end_date)
    
    if start_date == end_date:
        dts = dts*2
    
    sql = """
        SELECT TRADE_DT as date, S_INFO_WINDCODE as asset, {0}
        FROM AShareEODDerivativeIndicator
        WHERE TRADE_DT in {1} and S_INFO_WINDCODE IN {2}
        order by TRADE_DT, S_INFO_WINDCODE
        """.format(fields_sql, tuple(dts), tuple(codes))

    data = pd.read_sql(sql, wind_engine, 
                         index_col=['date', 'asset'],
                         parse_dates=['date'])
    
    return data


def get_turnover(codes=None, start_date=None, end_date=None):
    """
    从数据库获取换手率

    Parameters
    ----------
    codes : list
        股票代码 list
    start_date : str
        字符串类型日期 yyyymmdd，譬如 '20150101'，表示起始日期
    end_date : str
        字符串类型日期 yyyymmdd，譬如 '20160101'，表示结束日期

    Returns
    -------
    pd.Series
        index : [trade_dt, asset]
    """
    if start_date is None:
        start_date = '20070101'
    if end_date is None:
        end_date = date_to_str(datetime.today())
    if codes is None:
        codes = get_all_stocks(start_date, end_date)

    assert end_date >= start_date, '获取时间区间起始时间晚于截至时间'

    sql = """
        SELECT TRADE_DT as date, S_INFO_WINDCODE as asset, S_DQ_TURN as turnover
        FROM AShareEODDerivativeIndicator
        WHERE TRADE_DT >= '{0}' and TRADE_DT <= '{1}' and S_INFO_WINDCODE IN {2}
            and S_DQ_TURN is not null
        order by TRADE_DT, S_INFO_WINDCODE
        """.format(start_date, end_date, tuple(codes))

    data = pd.read_sql(sql, wind_engine,
                         index_col=['date', 'asset'],
                         parse_dates=['date'])

    return data

def get_free_turnover(codes=None, start_date=None, end_date=None):
    """
    从数据库获取自由换手率

    Parameters
    ----------
    codes : list
        股票代码 list
    start_date : str
        字符串类型日期 yyyymmdd，譬如 '20150101'，表示起始日期
    end_date : str
        字符串类型日期 yyyymmdd，譬如 '20160101'，表示结束日期

    Returns
    -------
    pd.Series
        index : [trade_dt, asset]
    """
    if start_date is None:
        start_date = '20070101'
    if end_date is None:
        end_date = date_to_str(datetime.today())
    if codes is None:
        codes = get_all_stocks(start_date, end_date)

    assert end_date >= start_date, '获取时间区间起始时间晚于截至时间'

    sql = """
        SELECT TRADE_DT as date, S_INFO_WINDCODE as asset, S_DQ_FREETURNOVER as freeturnover
        FROM AShareEODDerivativeIndicator
        WHERE TRADE_DT >= '{0}' and TRADE_DT <= '{1}' and S_INFO_WINDCODE IN {2}
            and S_DQ_FREETURNOVER is not null
        order by TRADE_DT, S_INFO_WINDCODE
        """.format(start_date, end_date, tuple(codes))

    data = pd.read_sql(sql, wind_engine,
                         index_col=['date', 'asset'],
                         parse_dates=['date'])

    return data


def get_valuation(codes=None, start_date=None, end_date=None, fields=None):
    """
    获取估值类数据

    Parameters
    ----------
    codes : list
        股票代码 list
    start_date : str
        字符串类型日期 yyyymmdd，譬如 '20150101'，表示起始日期
    end_date : str
        字符串类型日期 yyyymmdd，譬如 '20160101'，表示结束日期
    fields : list
        选择要获取的行情数据字段, 默认是None(表示['S_VAL_PE','S_VAL_PB_NEW','S_VAL_PE_TTM'])
        相对应：市盈率(PE)、市净率(PB)、市盈率(PE,TTM)

    Returns
    -------
    pd.DataFrame
        index : [date, asset] , columns 为 fields
    """
    if fields is None:
        fields = ['S_VAL_PE','S_VAL_PB_NEW','S_VAL_PE_TTM']
    if start_date is None:
        start_date = '20070101'
    if end_date is None:
        end_date = date_to_str(datetime.today())
    if codes is None:
        codes = get_all_stocks(start_date, end_date)

    assert end_date >= start_date, '获取时间区间起始时间晚于截至时间'
    


    fields_sql = ','.join(fields)
    dts = get_trading_days(start_date, end_date)
    
    if start_date == end_date:
        dts = dts*2

    sql = """
        SELECT TRADE_DT as date, S_INFO_WINDCODE as asset, {0}
        FROM AShareEODDerivativeIndicator
        WHERE TRADE_DT in {1} and S_INFO_WINDCODE IN {2}
        order by TRADE_DT, S_INFO_WINDCODE
        """.format(fields_sql, tuple(dts), tuple(codes))

    data = pd.read_sql(sql, wind_engine,
                         index_col=['date', 'asset'],
                         parse_dates=['date'])

    return data

#%% 获取wind因子数据
def get_daily_valuation_factor(codes=None, start_date=None, end_date=None, fields=None):
    """
    获取估值类数据

    Parameters
    ----------
    codes : list
        股票代码 list
    start_date : str
        字符串类型日期 yyyymmdd，譬如 '20150101'，表示起始日期
    end_date : str
        字符串类型日期 yyyymmdd，譬如 '20160101'，表示结束日期
    fields : list
        选择要获取的行情数据字段, 默认是None(表示['S_DFA_LNMV','S_DFA_LNTOTASSETS','S_DFA_FLOATMV','S_DFA_LNFLOATMV','S_DFA_OPPS','S_DFA_PETTM_DEDUCTED','S_DFA_MVTOEBITDA','S_DFA_PROFITTOMV'])
        相对应：对数市值、对数总资产、自由流通市值、对数自由流通市值、每股营业利润(TTM)、扣非后市盈率(TTM)、总市值/EBITDA(TTM反推法)、收益市值比(TTM)（%）


    Returns
    -------
    pd.DataFrame
        index : [date, asset] , columns 为 fields
    """
    if fields is None:
        fields = ['S_DFA_LNMV','S_DFA_LNTOTASSETS','S_DFA_FLOATMV','S_DFA_LNFLOATMV','S_DFA_OPPS','S_DFA_PETTM_DEDUCTED','S_DFA_MVTOEBITDA','S_DFA_PROFITTOMV']
    if start_date is None:
        start_date = '20070101'
    if end_date is None:
        end_date = date_to_str(datetime.today())
    if codes is None:
        codes = get_all_stocks(start_date, end_date)

    assert end_date >= start_date, '获取时间区间起始时间晚于截至时间'

    fields_sql = ','.join(fields)
    dts = get_trading_days(start_date, end_date)
    
    if start_date == end_date:
        dts = dts*2    

    sql = """
        SELECT TRADE_DT as date, S_INFO_WINDCODE as asset, {0}
        FROM DailyValuationFactor
        WHERE TRADE_DT in {1} and S_INFO_WINDCODE IN {2}
        order by TRADE_DT, S_INFO_WINDCODE
        """.format(fields_sql, tuple(dts), tuple(codes))

    data = pd.read_sql(sql, wind_engine,
                         index_col=['date', 'asset'],
                         parse_dates=['date'])

    return data


def get_pit_financial_factor(codes=None, start_date=None, end_date=None, fields=None):
    """
    获取估值类数据

    Parameters
    ----------
    codes : list
        股票代码 list
    start_date : str
        字符串类型日期 yyyymmdd，譬如 '20150101'，表示起始日期
    end_date : str
        字符串类型日期 yyyymmdd，譬如 '20160101'，表示结束日期
    fields : list
        选择要获取的行情数据字段, 默认是None(表示['S_DFA_OP_TTM','S_DFA_EBT_TTM','S_DFA_PROFIT_TTM','S_DFA_NETPROFIT_TTM','S_DFA_TOTASSETS','S_DFA_TOTLIAB','S_DFA_TOTEQUITY'])
        相对应：营业利润(TTM)、利润总额(TTM)、净利润(TTM)、归属母公司股东的净利润(TTM)、资产总计、负债总计、股东权益


    Returns
    -------
    pd.DataFrame
        index : [date, asset] , columns 为 fields
    """
    if fields is None:
        fields = ['S_DFA_OP_TTM','S_DFA_EBT_TTM','S_DFA_PROFIT_TTM','S_DFA_NETPROFIT_TTM','S_DFA_TOTASSETS','S_DFA_TOTLIAB','S_DFA_TOTEQUITY']
    if start_date is None:
        start_date = '20070101'
    if end_date is None:
        end_date = date_to_str(datetime.today())
    if codes is None:
        codes = get_all_stocks(start_date, end_date)

    assert end_date >= start_date, '获取时间区间起始时间晚于截至时间'

    fields_sql = ','.join(fields)
    dts = get_trading_days(start_date, end_date)
    
    if start_date == end_date:
        dts = dts*2
        

    sql = """
        SELECT TRADE_DT as date, S_INFO_WINDCODE as asset, {0}
        FROM PITFinancialFactor
        WHERE TRADE_DT in {1} and S_INFO_WINDCODE IN {2}
        order by TRADE_DT, S_INFO_WINDCODE
        """.format(fields_sql, tuple(dts), tuple(codes))

    data = pd.read_sql(sql, wind_engine,
                         index_col=['date', 'asset'],
                         parse_dates=['date'])

    return data







