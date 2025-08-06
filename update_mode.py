# 各子系统数据更新模式配置，集中管理
# 可选: 'full' 或 'increment'

CB_ARCHIVER_UPDATE_MODE = {
    'cb_basic': 'full',      # 可转债基本信息
    'cb_issue': 'increment',      # 可转债发行数据
    'cb_call': 'increment',       # 可转债赎回信息
    'cb_daily': 'increment',      # 可转债行情
    'cb_share': 'increment',      # 可转债转股结果
    'repo_daily': 'increment',    # 债券回购日行情
    'bond_blk': 'increment',      # 债券大宗交易
}

BASIC_ARCHIVER_UPDATE_MODE = {
    'trade_cal': 'full',     # 交易日历
    # 未来可扩展
}

STOCK_INFO_ARCHIVER_UPDATE_MODE = {
    'stock_basic': 'full',           # 股票基本信息
    'stock_namechange': 'increment', # 股票曾用名
    'stock_daily': 'increment',      # A股日线行情
    'stock_income': 'increment',     # 股票利润表
    'stock_cashflow': 'increment',   # 股票现金流量表
    'stock_balancesheet': 'increment', # 股票资产负债表
    'stock_forecast': 'increment',   # 股票业绩预告
    'stock_express': 'increment',    # 股票业绩快报
    'stock_fina_indicator': 'increment', # 股票财务指标
    'stock_fina_mainbz': 'increment', # 股票主营业务构成
    'stock_dividend': 'increment',   # 股票分红送股
    'stock_block_trade': 'increment', # 股票大宗交易
    'stock_margin': 'increment',     # 融资融券交易汇总
    'stock_kpl_concept': 'increment', # 开盘啦题材库
    'stock_kpl_concept_cons': 'increment', # 开盘啦题材成分
    'stock_kpl_list': 'increment',   # 开盘啦榜单数据
    'stock_holder_trade': 'increment', # 股东增减持
    'stock_dc_index': 'increment',     # 东方财富概念板块
    'stock_dc_member': 'increment',    # 东方财富板块成分
    # 未来股票信息相关数据更新模式配置
} 