# -*- coding: utf-8 -*-

"""
汇鸿汇升数据访问接口

最近更新日期：2020-06-23 10:16:00

最近更新：新增Fama三因子数据获取端口，EquityData类新增股票市值获取端口
"""
import numpy as np
import pandas as pd
import sys
from datetime import datetime, timedelta
import time
from functools import reduce
from utils.common import date_to_str, str_to_date
from hsdataapi import winddataapi

#%%辅助函数
def date_format(date):
    """
    将格式为yyyymmdd的字符串转变为yyyy-mm-dd的格式
    """
    strlist = list(date)
    strlist.insert(4,'-')
    strlist.insert(7,'-')
    date = ''.join(strlist)
    return date

def isTradeDate(date):
    tradeday = winddataapi.get_trading_days(date)
    if date not in tradeday:
        return False
    else:
        return True
#%%EquityData数据接口的类封装
class EquityData(object):
    def __init__(self):
        self.path = 'http://api.hhhstz.com:8111/cube?'
        
    #股票交易信息获取函数       
    def get_all_stocks(self,start_date=None, end_date=None,flag =False,\
                       remove_st=False):
        """
        获取指定区间内的交易过的股票代码
        
        Parameters
        ----------
        start_date : str
            开始日期，格式为 yyyymmdd 字符串
        end_date : str
            结束如期，格式为 yyyymmdd 字符串
        flag : bool
            flag表示是否筛选停牌数据默认是False

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
        if start_date == end_date and not isTradeDate(start_date):
            start_date = winddataapi.get_previous_trade_dt(start_date)
            end_date = start_date
        start_date = date_format(start_date) 
        end_date = date_format(end_date)
        tableName1 = 'b_stocka_marketday'
        query_str1 = self.path+'tableName='+tableName1+'&begDate='+start_date+\
            '&endDate='+end_date+'&fields=c_code,c_tradeStatus,t_tradingDate'
        data1 = pd.read_csv(query_str1)
        if flag:
            index1 = [i for i in range(len(data1)) if data1.iloc[i,1]\
                      =='停牌']
            data1 = data1.drop(index1)
            data1 = data1.reset_index(drop=True)
        if remove_st:
            tableName2 = 'b_stocka_warning'
            query_str2 = self.path+'tableName='+tableName2+'&begDate='+start_date+\
                '&endDate='+end_date+'&fields=c_code,t_tradingDate'
            data2 = pd.read_csv(query_str2)
            index2 =[]
            for i in range(len(data2)):
                for j in range(len(data1)):
                    if data2.iloc[i,0]==data1.iloc[j,0] and data2.iloc[i,1]==data1.iloc[j,2]:
                        index2.append(j)
            data1 = data1.drop(index2)
            data1 = data1.reset_index(drop=True)
        return data1['c_code'].unique().tolist()
            
            
            
    def get_stocks_price(self, start_date=None, end_date=None,fields = None):
        """
        Parameters
        ----------
        start_date : str
            开始日期，格式为 yyyymmdd 字符串
        end_date : str
            结束如期，格式为 yyyymmdd 字符串
        fields : list
            想要获取的指定数据

        Returns
        -------
        dataframe
            股票列表 交易日期 收盘价 交易状态
        """
        if start_date is None:
            start_date = '20070101'
        if end_date is None:
            end_date = date_to_str(datetime.today())
        if fields is None:
            fields_str = 'c_code,t_tradingDate,n_close,c_tradeStatus,n_pctChange'
        else:
            fields_str = 'c_code,t_tradingDate,'+','.join(fields)

            
        assert end_date >= start_date, 'until date must be greater than since date'
        start_date = date_format(start_date) 
        end_date = date_format(end_date)
        tableName = 'b_stocka_marketday'
        query_str = self.path+'tableName='+tableName+'&begDate='+start_date+\
            '&endDate='+end_date+'&fields='+fields_str
        data = pd.read_csv(query_str)
        return data
    
    #股票衍生指标
    def get_stocks_valuation(self,start_date = None,end_date = None,fields = None):
        '''
        对应数据库中的股票日行情估值指标表
        
        Parameters
        ----------
        start_date : str
            开始日期，格式为 yyyymmdd 字符串
        end_date : str
            结束如期，格式为 yyyymmdd 字符串
        fields : list
            想要获取的指定数据
        
        '''
        if start_date is None:
            start_date = '20070101'
        if end_date is None:
            end_date = date_to_str(datetime.today())
        if fields is None:
            fields_str = 'c_code,t_tradingDate,n_sValPe,n_sValPbNew,n_sValPeTtm'
        else:
            fields_str = 'c_code,t_tradingDate,'+','.join(fields)
        assert end_date >= start_date, 'until date must be greater than since date'
        start_date = date_format(start_date) 
        end_date = date_format(end_date)
        tableName = 'b_stocka_derivativeindicator'
        query_str = self.path+'tableName='+tableName+'&begDate='+start_date+\
            '&endDate='+end_date+'&fields='+fields_str
        data = pd.read_csv(query_str)
        return data
        
    def get_mv(self,start_date = None,end_date = None):
        '''
        对应数据库中的股票日行情估值指标表
        
        Parameters
        ----------
        start_date : str
            开始日期，格式为 yyyymmdd 字符串
        end_date : str
            结束如期，格式为 yyyymmdd 字符串
        
        Returns
        -------
        dataframe
            股票列表 交易日期 总市值
            
        '''
        if start_date is None:
            start_date = '20070101'
        if end_date is None:
            end_date = date_to_str(datetime.today())
        start_date = date_format(start_date) 
        end_date = date_format(end_date)
        tableName = 'b_stocka_derivativeindicator'
        query_str = self.path+'tableName='+tableName+'&begDate='+start_date+\
            '&endDate='+end_date+'&fields=c_code,t_tradingDate,n_sValMv'
        data = pd.read_csv(query_str)  
        return data
    
    #股票财务数据获取函数    
    def get_ashare_balancesheet(self,start_date= None,end_date = None,fields=None):
        '''
        对应数据接口中的资产负债表
        Parameters
        ----------
        start_date : str
            开始日期，格式为 yyyymmdd 字符串（以报告期为准）
        end_date : str
            结束如期，格式为 yyyymmdd 字符串（以报告期为准）
        fields : list
            想要获取的指定数据
        '''
        if start_date is None:
            start_date = '20070101'
        if end_date is None:
            end_date = date_to_str(datetime.today())
        
        assert end_date >= start_date, 'until date must be greater than since date'
        
        start_date = date_format(start_date) 
        end_date = date_format(end_date)
        
        tableName = 'b_stocka_balancesheet'
        
        if fields is None:
            query_str = self.path+'tableName='+tableName+'&begDate='+start_date+\
            '&endDate='+end_date           
        else:
            fields_str= 'c_code,t_actualAnnDt,c_reportPeriod,c_statementType,'+','.join(fields)
            query_str = self.path+'tableName='+tableName+'&begDate='+start_date+\
            '&endDate='+end_date+'&fields='+fields_str
            
        data = pd.read_csv(query_str)
        #wind数据库中没有说这里有408002000 与408003000即单季度合并报表类型
        report_types = [408001000,408002000,408003000,408004000,40800500]
        
        #index1 = [i for i in range(len(data)) if data.iloc[i,5] not in statement_type_select ]
        data = data[data["c_statementType"].isin(report_types)]
        return data
    
    def get_ashare_cashflow(self,start_date= None,end_date = None,fields = None):
        '''
        对应数据接口中的现金流量表
        Parameters
        ----------
        start_date : str
            开始日期，格式为 yyyymmdd 字符串（以报告期为准）
        end_date : str
            结束如期，格式为 yyyymmdd 字符串（以报告期为准）
        fields : list
            想要获取的指定数据
        '''
        if start_date is None:
            start_date = '20070101'
        if end_date is None:
            end_date = date_to_str(datetime.today())
    

        assert end_date >= start_date, 'until date must be greater than since date'
        start_date = date_format(start_date) 
        end_date = date_format(end_date)
        
        tableName = 'b_stocka_cashflow'
        
        if fields is None:
            query_str = self.path+'tableName='+tableName+'&begDate='+start_date+\
            '&endDate='+end_date           
        else:
            fields_str= 'c_code,t_actualAnnDt,c_reportPeriod,c_statementType,'+','.join(fields)
            query_str = self.path+'tableName='+tableName+'&begDate='+start_date+\
            '&endDate='+end_date+'&fields='+fields_str
         
        
        data = pd.read_csv(query_str)
        report_types = [408001000,408002000,408003000,408004000,40800500]
        #index1 = [i for i in range(len(data)) if data.iloc[i,5] not in statement_type_select ]
        data = data[data["c_statementType"].isin(report_types)]
        return data
    
    def get_ashare_income(self,start_date= None,end_date = None,fields = None):
        '''
        对应数据接口中的利润表
        Parameters
        ----------
        start_date : str
            开始日期，格式为 yyyymmdd 字符串（以报告期为准）
        end_date : str
            结束如期，格式为 yyyymmdd 字符串（以报告期为准）
        fields : list
            想要获取的指定数据
        '''
        if start_date is None:
            start_date = '20070101'
        if end_date is None:
            end_date = date_to_str(datetime.today())
       

        assert end_date >= start_date, 'until date must be greater than since date'
        
        start_date = date_format(start_date) 
        end_date = date_format(end_date)
            
        
        tableName = 'b_stocka_income'
        if fields is None:
            query_str = self.path+'tableName='+tableName+'&begDate='+start_date+\
            '&endDate='+end_date           
        else:
            fields_str= 'c_code,t_actualAnnDt,c_reportPeriod,c_statementType,'+','.join(fields)
            query_str = self.path+'tableName='+tableName+'&begDate='+start_date+\
            '&endDate='+end_date+'&fields='+fields_str
        
        data = pd.read_csv(query_str)
        
        report_types = [408001000,408002000,408003000,408004000,40800500]
        #index1 = [i for i in range(len(data)) if data.iloc[i,5] not in statement_type_select ]
        data = data[data["c_statementType"].isin(report_types)]
        return data
    
    def get_ashare_profit_notice(self,start_date= None,end_date = None,fields = None):
        '''
        对应数据接口中的业绩预告表
        Parameters
        ----------
        start_date : str
            开始日期，格式为 yyyymmdd 字符串（以报告期为准）
        end_date : str
            结束如期，格式为 yyyymmdd 字符串（以报告期为准）
        fields : list
            想要获取的指定数据
        '''
        if start_date is None:
            start_date = '20070101'
        if end_date is None:
            end_date = date_to_str(datetime.today())
       

        assert end_date >= start_date, 'until date must be greater than since date'
        
        start_date = date_format(start_date) 
        end_date = date_format(end_date)
            
        
        tableName = 'b_stocka_profitNotice'
        if fields is None:
            query_str = self.path+'tableName='+tableName+'&begDate='+start_date+\
            '&endDate='+end_date           
        else:
            fields_str= 'c_code,t_actualAnnDt,c_reportPeriod,c_statementType,'+','.join(fields)
            query_str = self.path+'tableName='+tableName+'&begDate='+start_date+\
            '&endDate='+end_date+'&fields='+fields_str
        
        data = pd.read_csv(query_str)        
        return data
    
    #股票基本信息获取函数    
    def get_ipo_date(self):
        '''
        Parameters
        ----------
        start_date : str
            入库日期，格式为 yyyymmdd 字符串（开始交易日）
        
        fields : list
            想要获取的指定数据
        '''
        start_date = '20070101'
        end_date = date_to_str(datetime.today())
        start_date = date_format(start_date) 
        end_date = date_format(end_date) 
        tableName = 'b_stocka_ipodate'
        query_str = self.path+'tableName='+tableName+'&begDate='+start_date+'&endDate='+end_date
        data = pd.read_csv(query_str)
        return data
    #股票行业信息获取函数   
    def get_industry(self,start_date=None,end_date=None,fields = None):
        """
        获取指定区间内所有股票的行业描述性信息
        
        Parameters
        ----------
        start_date : str
            开始日期，格式为 yyyymmdd 字符串
            
        end_date : str
            结束如期，格式为 yyyymmdd 字符串
            
        fields : list
            想要获取的指定数据
            
        Returns
        -------
        DataFrame
            行业描述性信息表
        """
        if start_date is None:
            start_date = '19491001'
        if end_date is None:
            end_date = date_to_str(datetime.today())
        assert end_date >= start_date, 'until date must be greater than since date'
        """
        if start_date == end_date and not isTradeDate(start_date):
            start_date = winddataapi.get_previous_trade_dt(start_date)
            end_date = start_date
        """
        if fields is None:
            fields_str = 't_date,c_code,c_swIndustryName1'
        else:
            fields_str = 't_date,c_code,'+','.join(fields)
        start_date = date_format(start_date) 
        end_date = date_format(end_date)
        query_str = self.path+'tableName=b_stocka_plateinfo'+'&begDate='+start_date+\
            '&endDate='+end_date+'&fields='+fields_str
        data = pd.read_csv(query_str)
        return data
    
    #指数基本信息获取函数 
    def get_index_name(self):
        """
        获取最新的指数名称和代码,
        可能存在问题，需要继续改进
        
        Parameters
        ----------
            
        Returns
        -------
        DataFrame
            指数名称和代码信息表
        """
   
        start_date = '20200601'
        end_date = date_to_str(datetime.today())
        
        start_date = date_format(start_date) 
        end_date = date_format(end_date)
        #fields_str = 'index,c_indexCode,c_indexName'
        query_str = self.path+'tableName=b_stocka_indexinfo'+'&begDate='+start_date+'&endDate='+end_date
            
        data = pd.read_csv(query_str)
        data.drop('t_updateTime',axis=1,inplace=True)
        return data.drop('index',axis=1)
       
    def get_index_daily_price(self,code=None,start_date=None,end_date=None,fields=None):
        """
        获取指定区间内指数的日行情信息
        
        Parameters
        ----------
        code : str 或者 list
            输入指数的代码，例如字符串型的'000001.SH'，列表型的['000001.SH','H30186.CSI']
            默认为None时，则获取所有指数的日行情
        start_date : str
            开始日期，格式为 yyyymmdd 字符串
            
        end_date : str
            结束如期，格式为 yyyymmdd 字符串
        
        fields : list
            想要获取的指定数据,默认字段是n_pctChange
            
        Returns
        -------
        DataFrame
            返回指数的日行情信息表
        """
        if start_date is None:
            start_date = '19491001'
        if end_date is None:
            end_date = date_to_str(datetime.today())
        assert end_date >= start_date, 'until date must be greater than since date' 
        if fields is None:
            fields_str ='c_indexCode,t_tradingDate,n_pctChange'
        else:
            fields_str ='c_indexCode,t_tradingDate,'+','.join(fields)
        start_date = date_format(start_date) 
        end_date = date_format(end_date)
        query_str = self.path+'tableName=b_stocka_indexmarketday'+'&begDate='+start_date+\
            '&endDate='+end_date+'&fields='+fields_str
        data = pd.read_csv(query_str)
        if code==None:
           return data  
        elif isinstance(code,str):
           return data[data['c_indexCode']==code].reset_index(drop=True)
        elif isinstance(code,list):
           return data.set_index('c_indexCode').T[code].T.reset_index()
       
    #陆港通通道持股数量统计获取函数(中央结算系统)
    def get_shsc_channelHoldings(self,start_date=None,end_date=None):
        """
        获取指定区间内的陆港通通道持股数量统计信息
        Parameters
        ----------
        start_date : str
            开始日期，格式为 yyyymmdd 字符串
            
        end_date : str
            结束如期，格式为 yyyymmdd 字符串
            
        Returns
        -------
        DataFrame
            返回陆港通通道持股数量统计表
        """
        if start_date is None:
            start_date = '19491001'
        if end_date is None:
            end_date = date_to_str(datetime.today())
        assert end_date >= start_date, 'until date must be greater than since date' 
        start_date = date_format(start_date) 
        end_date = date_format(end_date)
        query_str = self.path+'tableName=b_shsc_channelHoldings'+'&begDate='+start_date+'&endDate='+end_date
        data = pd.read_csv(query_str)
        return data.sort_values(by=['c_windCode','t_tradingDate'])
    
    # 沪港通每日成交统计获取函数
    def get_shhk_transaction(self,start_date=None,end_date=None):
        """
        获取指定区间内的陆港通通道持股总量统计信息
        Parameters
        ----------
        start_date : str
            开始日期，格式为 yyyymmdd 字符串
            
        end_date : str
            结束如期，格式为 yyyymmdd 字符串
            
        Returns
        -------
        DataFrame
            返回陆港通通道持股总量统计表
        """
        if start_date is None:
            start_date = '19491001'
        if end_date is None:
            end_date = date_to_str(datetime.today())
        assert end_date >= start_date, 'until date must be greater than since date' 
        start_date = date_format(start_date) 
        end_date = date_format(end_date)
        query_str = self.path+'tableName=b_shhk_transaction'+'&begDate='+start_date+'&endDate='+end_date
        data = pd.read_csv(query_str)
        return data.sort_values(by=['t_date'])
    
    # 深港通每日成交统计获取函数
    def get_szhk_transaction(self,start_date=None,end_date=None):
        """
        获取指定区间内的陆港通通道持股总量统计信息
        Parameters
        ----------
        start_date : str
            开始日期，格式为 yyyymmdd 字符串
            
        end_date : str
            结束如期，格式为 yyyymmdd 字符串
            
        Returns
        -------
        DataFrame
            返回陆港通通道持股总量统计表
        """
        if start_date is None:
            start_date = '19491001'
        if end_date is None:
            end_date = date_to_str(datetime.today())
        assert end_date >= start_date, 'until date must be greater than since date' 
        start_date = date_format(start_date) 
        end_date = date_format(end_date)
        query_str = self.path+'tableName=b_szhk_transaction'+'&begDate='+start_date+'&endDate='+end_date
        data = pd.read_csv(query_str)
        return data.sort_values(by=['t_date'])
#%%BarraData数据接口的类封装
class BarraData(object):
    def __init__(self):
        self.path = 'http://api.hhhstz.com:8111/cube?'
            
    def get_descripter(self,name,start_date=None,end_date=None):
        """
        获取指定区间内的交易过的因子描述性信息
        
        Parameters
        ----------
        name : 因子名
             
        start_date : str
            开始日期，格式为 yyyymmdd 字符串
            
        end_date : str
            结束如期，格式为 yyyymmdd 字符串
            
        Returns
        -------
        DataFrame
            因子描述性信息表
        """
        if start_date is None:
            start_date = '19491001'
        if end_date is None:
            end_date = date_to_str(datetime.today())
        assert end_date >= start_date, 'until date must be greater than since date'
        """
        if start_date == end_date and not isTradeDate(start_date):
            start_date = winddataapi.get_previous_trade_dt(start_date)
            end_date = start_date
        """
        start_date = date_format(start_date) 
        end_date = date_format(end_date)
        query_str = self.path+'tableName='+name+'&begDate='+start_date+\
            '&endDate='+end_date
        data = pd.read_csv(query_str)
        return data
#%%Fama三因子数据接口的类封装      
class FamaFrenchData(object):
    def __init__(self):
        self.path = 'http://api.hhhstz.com:8111/cube?'
            
    def get_ff3f(self,start_date=None,end_date=None,fields=['RM','SMB','HML']):
        """
        获取指定区间内的交易过的因子描述性信息
        
        Parameters
        ----------
        name : 因子名
             f
        start_date : str
            开始日期，格式为 yyyymmdd 字符串
            
        end_date : str
            结束如期，格式为 yyyymmdd 字符串
            
        Returns
        -------
        DataFrame
            因子描述性信息表
        """
        if start_date is None:
            start_date = '19491001'
        if end_date is None:
            end_date = date_to_str(datetime.today())
        assert end_date >= start_date, 'until date must be greater than since date'
        """
        if start_date == end_date and not isTradeDate(start_date):
            start_date = winddataapi.get_previous_trade_dt(start_date)
            end_date = start_date
        """
        start_date = date_format(start_date) 
        end_date = date_format(end_date)
        name = 'ff3f'
        fields_str ='index,'+','.join(fields)
        
        query_str = self.path+'tableName='+name+'&begDate='+start_date+\
            '&endDate='+end_date+'&fields='+fields_str
        data = pd.read_csv(query_str)
        return data.set_index('index')

    
if __name__=='__main__':
    '''
    ed = EquityData()
    aa=ed.get_index_daily_price(code=['000001.SH','H30186.CSI'],start_date='20200501',end_date='20200620',fields=None)
    print(aa)
    ed_stocks = ed.get_all_stocks('20150420','20160420')
    ed_price = ed.get_stocks_price('20150420','20160420')
    ed_valuation = ed.get_stocks_valuation('20150420','20160420')
    ed_balancesheet = ed.get_ashare_balancesheet('20150420','20160420')
    ed_cashflow = ed.get_ashare_cashflow('20150420','20160420')
    ed_income = ed.get_ashare_income('20150420','20160420')
    ed_ipo_date = ed.get_ipo_date()
    '''
    