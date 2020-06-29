#  -*- coding: utf-8 -*-

from abc import abstractmethod

"""
因子的基类。
"""

class MacroFactor(object):
    def __init__(self, name,category,description):
        """
        初始化因子
        :param name: 因子名称
        """
        self.name = name
        self.category = category
        self.description = description

    def name(self):
        return self.name

    @abstractmethod
    def compute(self, start_date, end_date):
        """
        计算指定周期内的因子值，并存入数据库
        :param start_date:  开始日期
        :param end_date:  结束日期
        """
        pass
    
    
class BarraFactor(object):
    def __init__(self, name,category1,category2,description):
        """
        初始化因子
        :param name: 因子名称
        """
        self.name = name
        self.category_level1 = category1
        self.category_level2 = category2
        self.description = description

    def name(self):
        return self.name

    @abstractmethod
    def compute(self, start_date, end_date):
        """
        计算指定周期内的因子值，并存入数据库
        :param start_date:  开始日期
        :param end_date:  结束日期
        """
        pass


class StockFactor(object):
    def __init__(self, name,category,description):
        """
        初始化因子
        :param name: 因子名称
        """
        self.name = name
        self.category = category
        self.description = description

    def name(self):
        return self.name

    @abstractmethod
    def compute(self, start_date, end_date):
        """
        计算指定周期内的因子值，并存入数据库
        :param start_date:  开始日期
        :param end_date:  结束日期
        """
        pass


class MarketFactor(object):
    def __init__(self, name,code,category,description):
        """
        初始化因子
        :param name: 因子名称
        """
        self.name = name
        self.category = category
        self.description = description

    def name(self):
        return self.name

    @abstractmethod
    def compute(self, start_date, end_date):
        """
        计算指定周期内的因子值，并存入数据库
        :param start_date:  开始日期
        :param end_date:  结束日期
        """
        pass

class IndustryFactor(object):
    def __init__(self, name,category,description):
        """
        初始化因子
        :param name: 因子名称
        """
        self.name = name
        self.category = category
        self.description = description

    def name(self):
        return self.name

    @abstractmethod
    def compute(self, start_date, end_date):
        """
        计算指定周期内的因子值，并存入数据库
        :param start_date:  开始日期
        :param end_date:  结束日期
        """
        pass
    
class Option(object):
    def __init__(self, name,underlying_asset,category,description):
        """
        初始化因子
        :param name: 因子名称
        """
        self.name = name
        self.underlying_asset = underlying_asset
        self.category = category
        self.description = description

    def name(self):
        return self.name

    @abstractmethod
    def compute(self, start_date, end_date):
        """
        计算指定周期内的因子值，并存入数据库
        :param start_date:  开始日期
        :param end_date:  结束日期
        """
        pass