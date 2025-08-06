import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import tushare as ts
import pandas as pd
import numpy as np
import pymysql
from config import Config
from loguru import logger
from update_mode import CB_ARCHIVER_UPDATE_MODE
from rich.progress import track
from utils import (
    get_empty_dates_for_updater, 
    filter_dates_for_update, 
    update_empty_dates_after_fetch,
    get_recent_trade_dates,
    convert_dates,
    safe_db_ready,
    normalize_dates,
    get_trade_dates
)

class CBBaseUpdater:
    """
    可转债相关数据更新基类，负责数据库连接、Tushare初始化、建表等通用操作。
    """
    def __init__(self):
        self.conn = pymysql.connect(
            host=Config.MYSQL_HOST,
            port=Config.MYSQL_PORT,
            user=Config.MYSQL_USER,
            password=Config.MYSQL_PASSWORD,
            database=Config.MYSQL_DATABASE,
            charset='utf8mb4'
        )
        self.pro = ts.pro_api(Config.TUSHARE_TOKEN)

    def create_table(self, create_sql):
        with self.conn.cursor() as cursor:
            cursor.execute(create_sql)
        self.conn.commit()

    def truncate_table(self, table_name):
        with self.conn.cursor() as cursor:
            cursor.execute(f"TRUNCATE TABLE {table_name}")
        self.conn.commit()

    def fetch_existing_keys(self, table_name, key_col):
        with self.conn.cursor() as cursor:
            cursor.execute(f"SELECT {key_col} FROM {table_name}")
            return set(row[0] for row in cursor.fetchall())

    def close(self):
        self.conn.close()

class CB_BasicUpdater(CBBaseUpdater):
    """
    可转债基本信息表的更新器，支持全量/增量更新，字段与Tushare官方文档保持一致。
    https://tushare.pro/document/2?doc_id=185
    """
    def __init__(self):
        super().__init__()
        self.table_name = 'cb_basic'
        # 官方文档所有字段
        self.columns = [
            'ts_code', 'bond_full_name', 'bond_short_name', 'cb_code', 'stk_code', 'stk_short_name',
            'maturity', 'par', 'issue_price', 'issue_size', 'remain_size', 'value_date', 'maturity_date',
            'rate_type', 'coupon_rate', 'add_rate', 'pay_per_year', 'list_date', 'delist_date', 'exchange',
            'conv_start_date', 'conv_end_date', 'conv_stop_date', 'first_conv_price', 'conv_price',
            'rate_clause', 'put_clause', 'maturity_put_price', 'call_clause', 'reset_clause', 'conv_clause',
            'guarantor', 'guarantee_type', 'issue_rating', 'newest_rating', 'rating_comp'
        ]
        self.create_table(self._get_create_sql())

    def _get_create_sql(self):
        # 字段类型与接口文档保持一致
        return f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            ts_code VARCHAR(20) PRIMARY KEY,
            bond_full_name VARCHAR(100),
            bond_short_name VARCHAR(100),
            cb_code VARCHAR(20),
            stk_code VARCHAR(20),
            stk_short_name VARCHAR(100),
            maturity FLOAT,
            par FLOAT,
            issue_price FLOAT,
            issue_size FLOAT,
            remain_size FLOAT,
            value_date VARCHAR(20),
            maturity_date VARCHAR(20),
            rate_type VARCHAR(20),
            coupon_rate FLOAT,
            add_rate FLOAT,
            pay_per_year INT,
            list_date VARCHAR(20),
            delist_date VARCHAR(20),
            exchange VARCHAR(20),
            conv_start_date VARCHAR(20),
            conv_end_date VARCHAR(20),
            conv_stop_date VARCHAR(20),
            first_conv_price FLOAT,
            conv_price FLOAT,
            rate_clause TEXT,
            put_clause TEXT,
            maturity_put_price VARCHAR(50),
            call_clause TEXT,
            reset_clause TEXT,
            conv_clause TEXT,
            guarantor VARCHAR(100),
            guarantee_type VARCHAR(100),
            issue_rating VARCHAR(50),
            newest_rating VARCHAR(50),
            rating_comp VARCHAR(100)
        ) DEFAULT CHARSET=utf8mb4;
        """

    def update(self, mode='full', fields=None):
        """
        更新可转债基本信息表
        mode: 'full' 全量更新（覆写），'increment' 增量更新
        fields: 指定字段列表，默认全字段
        """
        use_fields = fields if fields else self.columns
        # tushare接口支持fields参数，提升效率
        df = self.pro.cb_basic(fields=','.join(use_fields))
        # 统一日期字段
        date_fields = [
            'value_date', 'maturity_date', 'list_date', 'delist_date',
            'conv_start_date', 'conv_end_date', 'conv_stop_date'
        ]
        df = convert_dates(df, [f for f in date_fields if f in use_fields])
        if mode == 'full':
            self.truncate_table(self.table_name)
        elif mode == 'increment':
            exist_codes = self.fetch_existing_keys(self.table_name, 'ts_code')
            df = df[~df['ts_code'].isin(exist_codes)]
        if not df.empty:
            df[use_fields] = safe_db_ready(df[use_fields], use_fields)
            insert_sql = f"""
            REPLACE INTO {self.table_name} ({', '.join(use_fields)}) VALUES ({', '.join(['%s']*len(use_fields))})
            """
            with self.conn.cursor() as cursor:
                cursor.executemany(insert_sql, df[use_fields].values.tolist())
            self.conn.commit()

# 未来可扩展：可转债行情、财务等数据
class CB_QuoteUpdater(CBBaseUpdater):
    """
    可转债行情数据更新器（预留结构，具体实现可参考CB_BasicUpdater）
    """
    def update(self, mode='full'):
        pass

class CB_IssueUpdater(CBBaseUpdater):
    """
    可转债发行数据更新器，支持全量/增量更新，字段与Tushare官方文档保持一致。
    https://tushare.pro/document/2?doc_id=186
    """
    def __init__(self):
        super().__init__()
        self.table_name = 'cb_issue'
        self.columns = [
            'ts_code', 'ann_date', 'res_ann_date', 'plan_issue_size', 'issue_size', 'issue_price', 'issue_type',
            'issue_cost', 'onl_code', 'onl_name', 'onl_date', 'onl_size', 'onl_pch_vol', 'onl_pch_num',
            'onl_pch_excess', 'onl_winning_rate', 'shd_ration_code', 'shd_ration_name', 'shd_ration_date',
            'shd_ration_record_date', 'shd_ration_pay_date', 'shd_ration_price', 'shd_ration_ratio',
            'shd_ration_size', 'shd_ration_vol', 'shd_ration_num', 'shd_ration_excess', 'offl_size',
            'offl_deposit', 'offl_pch_vol', 'offl_pch_num', 'offl_pch_excess', 'offl_winning_rate',
            'lead_underwriter', 'lead_underwriter_vol'
        ]
        self.create_table(self._get_create_sql())

    def _get_create_sql(self):
        return f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            ts_code VARCHAR(20),
            ann_date VARCHAR(20),
            res_ann_date VARCHAR(20),
            plan_issue_size FLOAT,
            issue_size FLOAT,
            issue_price FLOAT,
            issue_type VARCHAR(20),
            issue_cost FLOAT,
            onl_code VARCHAR(20),
            onl_name VARCHAR(50),
            onl_date VARCHAR(20),
            onl_size FLOAT,
            onl_pch_vol FLOAT,
            onl_pch_num INT,
            onl_pch_excess FLOAT,
            onl_winning_rate FLOAT,
            shd_ration_code VARCHAR(20),
            shd_ration_name VARCHAR(50),
            shd_ration_date VARCHAR(20),
            shd_ration_record_date VARCHAR(20),
            shd_ration_pay_date VARCHAR(20),
            shd_ration_price FLOAT,
            shd_ration_ratio FLOAT,
            shd_ration_size FLOAT,
            shd_ration_vol FLOAT,
            shd_ration_num INT,
            shd_ration_excess FLOAT,
            offl_size FLOAT,
            offl_deposit FLOAT,
            offl_pch_vol FLOAT,
            offl_pch_num INT,
            offl_pch_excess FLOAT,
            offl_winning_rate FLOAT,
            lead_underwriter VARCHAR(100),
            lead_underwriter_vol FLOAT,
            PRIMARY KEY(ts_code, ann_date)
        ) DEFAULT CHARSET=utf8mb4;
        """

    def update(self, mode='full', fields=None, trade_dates=None):
        use_fields = fields if fields else self.columns
        if trade_dates is None:
            raise ValueError('CB_IssueUpdater.update: trade_dates参数不能为空，必须分批拉取！')
        
        # 获取empty_dates和最近交易日
        empty_dates = get_empty_dates_for_updater('CBArchiver', 'cb_issue')
        recent_trade_dates = get_recent_trade_dates(trade_dates, 5)
        
        if mode == 'full':
            self.truncate_table(self.table_name)
            dates_to_update = trade_dates
        else:
            # 获取已存在的日期
            exist_dates = set()
            with self.conn.cursor() as cursor:
                cursor.execute(f"SELECT DISTINCT ann_date FROM {self.table_name}")
                exist_dates = normalize_dates([row[0] for row in cursor.fetchall()])
            
            # 过滤出需要更新的日期（排除已存在和empty的日期）
            dates_to_update = filter_dates_for_update(
                trade_dates, exist_dates, empty_dates, 
                'CBArchiver', 'cb_issue', recent_trade_dates
            )
        
        for trade_date in track(dates_to_update, description=f"cb_issue {mode} updating"):
            df = self.pro.cb_issue(ann_date=trade_date, fields=','.join(use_fields))
            
            # 更新empty_dates
            update_empty_dates_after_fetch(
                'CBArchiver', 'cb_issue', trade_date, 
                df.empty, recent_trade_dates
            )
            
            if df.empty:
                continue
                
            df = convert_dates(df, [f for f in ['ann_date', 'res_ann_date', 'onl_date', 'shd_ration_date', 'shd_ration_record_date', 'shd_ration_pay_date'] if f in use_fields])
            if not df.empty:
                df[use_fields] = safe_db_ready(df[use_fields], use_fields)
                insert_sql = f"""
                REPLACE INTO {self.table_name} ({', '.join(use_fields)}) VALUES ({', '.join(['%s']*len(use_fields))})
                """
                with self.conn.cursor() as cursor:
                    cursor.executemany(insert_sql, df[use_fields].values.tolist())
                self.conn.commit()

class CB_CallUpdater(CBBaseUpdater):
    """
    可转债赎回信息更新器，支持全量/增量更新，字段与Tushare官方文档保持一致。
    https://tushare.pro/document/2?doc_id=269
    """
    def __init__(self):
        super().__init__()
        self.table_name = 'cb_call'
        self.columns = [
            'ts_code', 'call_type', 'is_call', 'ann_date', 'call_date', 'call_price', 'call_price_tax',
            'call_vol', 'call_amount', 'payment_date', 'call_reg_date'
        ]
        self.create_table(self._get_create_sql())

    def _get_create_sql(self):
        return f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            ts_code VARCHAR(20),
            call_type VARCHAR(20),
            is_call VARCHAR(50),
            ann_date VARCHAR(20),
            call_date VARCHAR(20),
            call_price FLOAT,
            call_price_tax FLOAT,
            call_vol FLOAT,
            call_amount FLOAT,
            payment_date VARCHAR(20),
            call_reg_date VARCHAR(20),
            PRIMARY KEY(ts_code, ann_date, call_type)
        ) DEFAULT CHARSET=utf8mb4;
        """

    def update(self, mode='full', fields=None, trade_dates=None):
        use_fields = fields if fields else self.columns
        if trade_dates is None:
            raise ValueError('CB_CallUpdater.update: trade_dates参数不能为空，必须分批拉取！')
        
        # 获取empty_dates和最近交易日
        empty_dates = get_empty_dates_for_updater('CBArchiver', 'cb_call')
        recent_trade_dates = get_recent_trade_dates(trade_dates, 5)
        
        if mode == 'full':
            self.truncate_table(self.table_name)
            dates_to_update = trade_dates
        else:
            # 获取已存在的日期
            with self.conn.cursor() as cursor:
                cursor.execute(f"SELECT DISTINCT ann_date FROM {self.table_name}")
                exist_dates = normalize_dates([row[0] for row in cursor.fetchall()])
            
            # 过滤出需要更新的日期（排除已存在和empty的日期）
            dates_to_update = filter_dates_for_update(
                trade_dates, exist_dates, empty_dates, 
                'CBArchiver', 'cb_call', recent_trade_dates
            )
        
        for trade_date in track(dates_to_update, description=f"cb_call {mode} updating"):
            df = self.pro.cb_call(ann_date=trade_date, fields=','.join(use_fields))
            
            # 更新empty_dates
            update_empty_dates_after_fetch(
                'CBArchiver', 'cb_call', trade_date, 
                df.empty, recent_trade_dates
            )
            
            if df.empty:
                continue
                
            df = convert_dates(df, [f for f in ['ann_date', 'call_date', 'payment_date', 'call_reg_date'] if f in use_fields])
            if not df.empty:
                df[use_fields] = safe_db_ready(df[use_fields], use_fields)
                insert_sql = f"""
                REPLACE INTO {self.table_name} ({', '.join(use_fields)}) VALUES ({', '.join(['%s']*len(use_fields))})
                """
                with self.conn.cursor() as cursor:
                    cursor.executemany(insert_sql, df[use_fields].values.tolist())
                self.conn.commit()

class CB_DailyUpdater(CBBaseUpdater):
    """
    可转债行情数据更新器，支持分批按交易日拉取，字段与Tushare官方文档一致。
    https://tushare.pro/document/2?doc_id=187
    """
    def __init__(self):
        super().__init__()
        self.table_name = 'cb_daily'
        self.columns = [
            'ts_code', 'trade_date', 'pre_close', 'open', 'high', 'low', 'close', 'change', 'pct_chg',
            'vol', 'amount', 'bond_value', 'bond_over_rate', 'cb_value', 'cb_over_rate'
        ]
        self.create_table(self._get_create_sql())

    def _get_create_sql(self):
        return f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            ts_code VARCHAR(20),
            trade_date DATE,
            pre_close FLOAT,
            open FLOAT,
            high FLOAT,
            low FLOAT,
            close FLOAT,
            `change` FLOAT,
            pct_chg FLOAT,
            vol FLOAT,
            amount FLOAT,
            bond_value FLOAT,
            bond_over_rate FLOAT,
            cb_value FLOAT,
            cb_over_rate FLOAT,
            PRIMARY KEY(ts_code, trade_date)
        ) DEFAULT CHARSET=utf8mb4;
        """

    def update(self, mode='full', fields=None, trade_dates=None):
        use_fields = [f'`change`' if f == 'change' else f for f in (fields if fields else self.columns)]
        raw_fields = [f.replace('`', '') for f in use_fields]
        if trade_dates is None:
            raise ValueError('CB_DailyUpdater.update: trade_dates参数不能为空，必须分批拉取！')
        
        # 获取empty_dates和最近交易日
        empty_dates = get_empty_dates_for_updater('CBArchiver', 'cb_daily')
        recent_trade_dates = get_recent_trade_dates(trade_dates, 5)
        
        if mode == 'full':
            self.truncate_table(self.table_name)
            dates_to_update = trade_dates
        else:
            # 获取已存在的日期
            with self.conn.cursor() as cursor:
                cursor.execute(f"SELECT DISTINCT trade_date FROM {self.table_name}")
                exist_dates = normalize_dates([row[0] for row in cursor.fetchall()])
            
            # 过滤出需要更新的日期（排除已存在和empty的日期）
            dates_to_update = filter_dates_for_update(
                trade_dates, exist_dates, empty_dates, 
                'CBArchiver', 'cb_daily', recent_trade_dates
            )
        
        for trade_date in track(dates_to_update, description=f"cb_daily {mode} updating"):
            df = self.pro.cb_daily(trade_date=trade_date, fields=','.join(raw_fields))
            
            # 更新empty_dates
            update_empty_dates_after_fetch(
                'CBArchiver', 'cb_daily', trade_date, 
                df.empty, recent_trade_dates
            )
            
            if df.empty:
                continue
                
            df = convert_dates(df, [f for f in ['trade_date'] if f in raw_fields])
            if not df.empty:
                df[raw_fields] = safe_db_ready(df[raw_fields], raw_fields)
                insert_sql = f"""
                REPLACE INTO {self.table_name} ({', '.join(use_fields)}) VALUES ({', '.join(['%s']*len(use_fields))})
                """
                with self.conn.cursor() as cursor:
                    cursor.executemany(insert_sql, df[raw_fields].values.tolist())
                self.conn.commit()

class CB_ShareUpdater(CBBaseUpdater):
    """
    可转债转股结果数据更新器，支持分批按公告日拉取，字段与Tushare官方文档一致。
    https://tushare.pro/document/2?doc_id=247
    """
    def __init__(self):
        super().__init__()
        self.table_name = 'cb_share'
        self.columns = [
            'ts_code', 'bond_short_name', 'publish_date', 'end_date', 'issue_size', 'convert_price_initial',
            'convert_price', 'convert_val', 'convert_vol', 'convert_ratio', 'acc_convert_val', 'acc_convert_vol',
            'acc_convert_ratio', 'remain_size', 'total_shares'
        ]
        self.create_table(self._get_create_sql())

    def _get_create_sql(self):
        return f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            ts_code VARCHAR(20),
            bond_short_name VARCHAR(100),
            publish_date DATE,
            end_date DATE,
            issue_size FLOAT,
            convert_price_initial FLOAT,
            convert_price FLOAT,
            convert_val FLOAT,
            convert_vol FLOAT,
            convert_ratio FLOAT,
            acc_convert_val FLOAT,
            acc_convert_vol FLOAT,
            acc_convert_ratio FLOAT,
            remain_size FLOAT,
            total_shares FLOAT,
            PRIMARY KEY(ts_code, publish_date, end_date)
        ) DEFAULT CHARSET=utf8mb4;
        """

    def update(self, mode='full', fields=None, trade_dates=None):
        use_fields = fields if fields else self.columns
        if trade_dates is None:
            raise ValueError('CB_ShareUpdater.update: trade_dates参数不能为空，必须分批拉取！')
        
        # 获取empty_dates和最近交易日
        empty_dates = get_empty_dates_for_updater('CBArchiver', 'cb_share')
        recent_trade_dates = get_recent_trade_dates(trade_dates, 5)
        
        if mode == 'full':
            self.truncate_table(self.table_name)
            dates_to_update = trade_dates
        else:
            # 获取已存在的日期
            with self.conn.cursor() as cursor:
                cursor.execute(f"SELECT DISTINCT publish_date FROM {self.table_name}")
                exist_dates = normalize_dates([row[0] for row in cursor.fetchall()])
            
            # 过滤出需要更新的日期（排除已存在和empty的日期）
            dates_to_update = filter_dates_for_update(
                trade_dates, exist_dates, empty_dates, 
                'CBArchiver', 'cb_share', recent_trade_dates
            )
        
        for trade_date in track(dates_to_update, description=f"cb_share {mode} updating"):
            # 按公告日分批拉取
            df = self.pro.cb_share(ann_date=trade_date, fields=','.join(use_fields))
            
            # 更新empty_dates
            update_empty_dates_after_fetch(
                'CBArchiver', 'cb_share', trade_date, 
                df.empty, recent_trade_dates
            )
            
            if df.empty:
                continue
                
            df = convert_dates(df, [f for f in ['publish_date', 'end_date'] if f in use_fields])
            if not df.empty:
                df[use_fields] = safe_db_ready(df[use_fields], use_fields)
                insert_sql = f"""
                REPLACE INTO {self.table_name} ({', '.join(use_fields)}) VALUES ({', '.join(['%s']*len(use_fields))})
                """
                with self.conn.cursor() as cursor:
                    cursor.executemany(insert_sql, df[use_fields].values.tolist())
                self.conn.commit()

class RepoDailyUpdater(CBBaseUpdater):
    """
    债券回购日行情数据更新器，支持分批按交易日拉取，字段与Tushare官方文档一致。
    https://tushare.pro/document/2?doc_id=256
    """
    def __init__(self):
        super().__init__()
        self.table_name = 'repo_daily'
        self.columns = [
            'ts_code', 'trade_date', 'repo_maturity', 'pre_close', 'open', 'high', 'low', 'close',
            'weight', 'weight_r', 'amount', 'num'
        ]
        self.create_table(self._get_create_sql())

    def _get_create_sql(self):
        return f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            ts_code VARCHAR(20),
            trade_date DATE,
            repo_maturity VARCHAR(20),
            pre_close FLOAT,
            open FLOAT,
            high FLOAT,
            low FLOAT,
            close FLOAT,
            weight FLOAT,
            weight_r FLOAT,
            amount FLOAT,
            num INT,
            PRIMARY KEY(ts_code, trade_date, repo_maturity)
        ) DEFAULT CHARSET=utf8mb4;
        """

    def update(self, mode='full', fields=None, trade_dates=None):
        use_fields = fields if fields else self.columns
        if trade_dates is None:
            raise ValueError('RepoDailyUpdater.update: trade_dates参数不能为空，必须分批拉取！')
        
        # 获取empty_dates和最近交易日
        empty_dates = get_empty_dates_for_updater('CBArchiver', 'repo_daily')
        recent_trade_dates = get_recent_trade_dates(trade_dates, 5)
        
        if mode == 'full':
            self.truncate_table(self.table_name)
            dates_to_update = trade_dates
        else:
            # 获取已存在的日期
            with self.conn.cursor() as cursor:
                cursor.execute(f"SELECT DISTINCT trade_date FROM {self.table_name}")
                exist_dates = normalize_dates([row[0] for row in cursor.fetchall()])
            
            # 过滤出需要更新的日期（排除已存在和empty的日期）
            dates_to_update = filter_dates_for_update(
                trade_dates, exist_dates, empty_dates, 
                'CBArchiver', 'repo_daily', recent_trade_dates
            )
        
        for trade_date in track(dates_to_update, description=f"repo_daily {mode} updating"):
            df = self.pro.repo_daily(trade_date=trade_date, fields=','.join(use_fields))
            
            # 更新empty_dates
            update_empty_dates_after_fetch(
                'CBArchiver', 'repo_daily', trade_date, 
                df.empty, recent_trade_dates
            )
            
            if df.empty:
                continue
            df = convert_dates(df, [f for f in ['trade_date'] if f in use_fields])
            if not df.empty:
                df[use_fields] = safe_db_ready(df[use_fields], use_fields)
                insert_sql = f"""
                REPLACE INTO {self.table_name} ({', '.join(use_fields)}) VALUES ({', '.join(['%s']*len(use_fields))})
                """
                with self.conn.cursor() as cursor:
                    cursor.executemany(insert_sql, df[use_fields].values.tolist())
                self.conn.commit()

class BondBlkUpdater(CBBaseUpdater):
    """
    债券大宗交易数据更新器，支持分批按交易日拉取，字段与Tushare官方文档一致。
    https://tushare.pro/document/2?doc_id=271
    """
    def __init__(self):
        super().__init__()
        self.table_name = 'bond_blk'
        self.columns = [
            'trade_date', 'ts_code', 'name', 'price', 'vol', 'amount'
        ]
        self.create_table(self._get_create_sql())

    def _get_create_sql(self):
        return f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            trade_date DATE,
            ts_code VARCHAR(20),
            name VARCHAR(100),
            price FLOAT,
            vol FLOAT,
            amount FLOAT,
            PRIMARY KEY(trade_date, ts_code, name, price, vol, amount)
        ) DEFAULT CHARSET=utf8mb4;
        """

    def update(self, mode='full', fields=None, trade_dates=None):
        use_fields = fields if fields else self.columns
        if trade_dates is None:
            raise ValueError('BondBlkUpdater.update: trade_dates参数不能为空，必须分批拉取！')
        
        # 获取empty_dates和最近交易日
        empty_dates = get_empty_dates_for_updater('CBArchiver', 'bond_blk')
        recent_trade_dates = get_recent_trade_dates(trade_dates, 5)
        
        if mode == 'full':
            self.truncate_table(self.table_name)
            dates_to_update = trade_dates
        else:
            # 获取已存在的日期
            with self.conn.cursor() as cursor:
                cursor.execute(f"SELECT DISTINCT trade_date FROM {self.table_name}")
                exist_dates = normalize_dates([row[0] for row in cursor.fetchall()])
            
            # 过滤出需要更新的日期（排除已存在和empty的日期）
            dates_to_update = filter_dates_for_update(
                trade_dates, exist_dates, empty_dates, 
                'CBArchiver', 'bond_blk', recent_trade_dates
            )
        
        for trade_date in track(dates_to_update, description=f"bond_blk {mode} updating"):
            df = self.pro.bond_blk(trade_date=trade_date, fields=','.join(use_fields))
            
            # 更新empty_dates
            update_empty_dates_after_fetch(
                'CBArchiver', 'bond_blk', trade_date, 
                df.empty, recent_trade_dates
            )
            
            if df.empty:
                continue
                
            df = convert_dates(df, [f for f in ['trade_date'] if f in use_fields])
            if not df.empty:
                df[use_fields] = safe_db_ready(df[use_fields], use_fields)
                insert_sql = f"""
                REPLACE INTO {self.table_name} ({', '.join(use_fields)}) VALUES ({', '.join(['%s']*len(use_fields))})
                """
                with self.conn.cursor() as cursor:
                    cursor.executemany(insert_sql, df[use_fields].values.tolist())
                self.conn.commit()

def main():
    """
    主函数：更新所有可转债相关数据（目前cb_basic、cb_issue、cb_call、cb_daily、cb_share、repo_daily、bond_blk，未来可扩展）
    """
    # cb_basic
    try:
        mode_basic = CB_ARCHIVER_UPDATE_MODE.get('cb_basic', 'full')
        logger.info(f"开始更新可转债基本信息，模式: {mode_basic} ...")
        cb_basic_updater = CB_BasicUpdater()
        cb_basic_updater.update(mode=mode_basic)
        cb_basic_updater.close()
        logger.info("可转债基本信息更新完成！")
    except Exception as e:
        logger.error(f"可转债基本信息更新失败: {e}")

    # 获取交易日（上交所，2023-01-01至今）
    try:
        conn = pymysql.connect(
            host=Config.MYSQL_HOST,
            port=Config.MYSQL_PORT,
            user=Config.MYSQL_USER,
            password=Config.MYSQL_PASSWORD,
            database=Config.MYSQL_DATABASE,
            charset='utf8mb4'
        )
        trade_dates = get_trade_dates(conn, '2023-01-01', pd.Timestamp.today().strftime('%Y-%m-%d'))
        conn.close()
    except Exception as e:
        logger.error(f"获取交易日失败: {e}")
        trade_dates = []

    # cb_issue
    try:
        mode_issue = CB_ARCHIVER_UPDATE_MODE.get('cb_issue', 'full')
        logger.info(f"开始更新可转债发行数据，模式: {mode_issue} ...")
        cb_issue_updater = CB_IssueUpdater()
        cb_issue_updater.update(mode=mode_issue, trade_dates=trade_dates)
        cb_issue_updater.close()
        logger.info("可转债发行数据更新完成！")
    except Exception as e:
        logger.error(f"可转债发行数据更新失败: {e}")

    # cb_call
    try:
        mode_call = CB_ARCHIVER_UPDATE_MODE.get('cb_call', 'full')
        logger.info(f"开始更新可转债赎回信息，模式: {mode_call} ...")
        cb_call_updater = CB_CallUpdater()
        cb_call_updater.update(mode=mode_call, trade_dates=trade_dates)
        cb_call_updater.close()
        logger.info("可转债赎回信息更新完成！")
    except Exception as e:
        logger.error(f"可转债赎回信息更新失败: {e}")

    # cb_daily
    try:
        mode_daily = CB_ARCHIVER_UPDATE_MODE.get('cb_daily', 'full')
        logger.info(f"开始更新可转债行情数据，模式: {mode_daily} ...")
        cb_daily_updater = CB_DailyUpdater()
        cb_daily_updater.update(mode=mode_daily, trade_dates=trade_dates)
        cb_daily_updater.close()
        logger.info("可转债行情数据更新完成！")
    except Exception as e:
        logger.error(f"可转债行情数据更新失败: {e}")

    # cb_share
    try:
        mode_share = CB_ARCHIVER_UPDATE_MODE.get('cb_share', 'full')
        logger.info(f"开始更新可转债转股结果数据，模式: {mode_share} ...")
        cb_share_updater = CB_ShareUpdater()
        cb_share_updater.update(mode=mode_share, trade_dates=trade_dates)
        cb_share_updater.close()
        logger.info("可转债转股结果数据更新完成！")
    except Exception as e:
        logger.error(f"可转债转股结果数据更新失败: {e}")

    # repo_daily
    try:
        mode_repo = CB_ARCHIVER_UPDATE_MODE.get('repo_daily', 'full')
        logger.info(f"开始更新债券回购日行情数据，模式: {mode_repo} ...")
        repo_daily_updater = RepoDailyUpdater()
        repo_daily_updater.update(mode=mode_repo, trade_dates=trade_dates)
        repo_daily_updater.close()
        logger.info("债券回购日行情数据更新完成！")
    except Exception as e:
        logger.error(f"债券回购日行情数据更新失败: {e}")

    # bond_blk
    try:
        mode_blk = CB_ARCHIVER_UPDATE_MODE.get('bond_blk', 'full')
        logger.info(f"开始更新债券大宗交易数据，模式: {mode_blk} ...")
        bond_blk_updater = BondBlkUpdater()
        bond_blk_updater.update(mode=mode_blk, trade_dates=trade_dates)
        bond_blk_updater.close()
        logger.info("债券大宗交易数据更新完成！")
    except Exception as e:
        logger.error(f"债券大宗交易数据更新失败: {e}")

if __name__ == "__main__":
    main()

