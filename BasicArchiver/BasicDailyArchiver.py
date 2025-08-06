import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import tushare as ts
import pandas as pd
import numpy as np
import pymysql
from config import Config
from loguru import logger
from update_mode import BASIC_ARCHIVER_UPDATE_MODE
from utils import (
    update_empty_dates_after_fetch,
    convert_dates,
    safe_db_ready,
)

class BasicBaseUpdater:
    """
    基础信息相关数据更新基类，负责数据库连接、Tushare初始化、建表等通用操作。
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

class TradeCalUpdater(BasicBaseUpdater):
    """
    交易日历数据更新器，支持全量/增量更新，字段与Tushare官方文档保持一致。
    https://tushare.pro/document/2?doc_id=26
    """
    def __init__(self):
        super().__init__()
        self.table_name = 'trade_cal'
        self.columns = [
            'exchange', 'cal_date', 'is_open', 'pretrade_date'
        ]
        self.create_table(self._get_create_sql())

    def _get_create_sql(self):
        return f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            exchange VARCHAR(10),
            cal_date DATE,
            is_open BOOLEAN,
            pretrade_date DATE,
            PRIMARY KEY(exchange, cal_date)
        ) DEFAULT CHARSET=utf8mb4;
        """

    def update(self, mode='full', fields=None, exchange='', start_date=None, end_date=None):
        if mode != 'full':
            raise ValueError('TradeCalUpdater 只支持全量更新（full）模式！')
        use_fields = fields if fields else self.columns
        df = self.pro.trade_cal(exchange=exchange, start_date=start_date, end_date=end_date, fields=','.join(use_fields))
        
        # 更新empty_dates（虽然只支持full模式，但仍记录empty状态）
        # 对于trade_cal，我们记录整个请求的empty状态
        update_empty_dates_after_fetch(
            'BasicArchiver', 'trade_cal', f"{start_date}_{end_date}_{exchange}", 
            df.empty, None  # trade_cal不需要recent_trade_dates检查
        )
        
        # 统一日期字段
        date_fields = ['cal_date', 'pretrade_date']
        df = convert_dates(df, [f for f in date_fields if f in use_fields])
        # is_open转为布尔类型
        if 'is_open' in use_fields and 'is_open' in df.columns:
            df['is_open'] = df['is_open'].astype(int).astype(bool)
        self.truncate_table(self.table_name)
        if not df.empty:
            df[use_fields] = safe_db_ready(df[use_fields], use_fields)
            insert_sql = f"""
            REPLACE INTO {self.table_name} ({', '.join(use_fields)}) VALUES ({', '.join(['%s']*len(use_fields))})
            """
            with self.conn.cursor() as cursor:
                cursor.executemany(insert_sql, df[use_fields].values.tolist())
            self.conn.commit()

# 示例用法
if __name__ == "__main__":
    mode = BASIC_ARCHIVER_UPDATE_MODE.get('trade_cal', 'full')
    logger.info(f"开始更新交易日历，模式: {mode} ...")
    updater = TradeCalUpdater()
    updater.update(mode=mode)
    updater.close()
    logger.info("交易日历更新完成！") 