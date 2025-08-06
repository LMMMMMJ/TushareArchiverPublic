import json
import os
from datetime import datetime, timedelta
from typing import Set, List, Optional
from loguru import logger
import pandas as pd
import numpy as np
import pymysql
from config import Config

# 统一的empty_dates文件路径
EMPTY_DATES_FILE = "empty_dates.json"

def load_empty_dates() -> dict:
    """
    加载所有Archiver的empty_dates
    返回格式: {
        "CBArchiver": {
            "cb_issue": ["20230101", "20230102", ...],
            "cb_call": ["20230101", ...],
            ...
        },
        "BasicArchiver": {
            "trade_cal": ["20230101", ...],
            ...
        }
    }
    """
    if os.path.exists(EMPTY_DATES_FILE):
        try:
            with open(EMPTY_DATES_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                # 确保返回的是字典格式，兼容旧版本
                if isinstance(data, dict):
                    return data
                else:
                    return {}
        except Exception as e:
            logger.warning(f"加载empty_dates.json失败: {e}")
            return {}
    return {}

def save_empty_dates(empty_dates: dict):
    """
    保存所有Archiver的empty_dates到JSON文件
    """
    try:
        with open(EMPTY_DATES_FILE, 'w', encoding='utf-8') as f:
            json.dump(empty_dates, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error(f"保存empty_dates.json失败: {e}")

def get_empty_dates_for_updater(archiver_name: str, updater_name: str) -> Set[str]:
    """
    获取指定Archiver和Updater的empty_dates
    """
    empty_dates = load_empty_dates()
    return set(empty_dates.get(archiver_name, {}).get(updater_name, []))

def add_empty_date(archiver_name: str, updater_name: str, date: str):
    """
    为指定的Archiver和Updater添加一个empty_date
    """
    empty_dates = load_empty_dates()
    
    # 确保archiver存在
    if archiver_name not in empty_dates:
        empty_dates[archiver_name] = {}
    
    # 确保updater存在
    if updater_name not in empty_dates[archiver_name]:
        empty_dates[archiver_name][updater_name] = []
    
    # 添加日期（如果不存在）
    if date not in empty_dates[archiver_name][updater_name]:
        empty_dates[archiver_name][updater_name].append(date)
        # 保持日期排序
        empty_dates[archiver_name][updater_name].sort()
        save_empty_dates(empty_dates)
        logger.debug(f"添加empty_date: {archiver_name}.{updater_name} - {date}")

def is_recent_trading_day(date: str, trade_dates: List[str], recent_days: int = 5) -> bool:
    """
    判断是否为最近N个交易日
    """
    if not trade_dates:
        return False
    
    # 将trade_dates转换为datetime对象并排序
    try:
        date_objects = []
        for d in trade_dates:
            if isinstance(d, str) and len(d) == 8:
                date_objects.append(datetime.strptime(d, '%Y%m%d'))
        
        if not date_objects:
            return False
        
        # 排序并获取最近的N个交易日
        date_objects.sort(reverse=True)
        recent_dates = date_objects[:recent_days]
        
        # 检查目标日期是否在最近N个交易日中
        target_date = datetime.strptime(date, '%Y%m%d')
        return target_date in recent_dates
        
    except Exception as e:
        logger.warning(f"判断最近交易日失败: {e}")
        return False

def filter_dates_for_update(trade_dates: List[str], 
                          exist_dates: Set[str], 
                          empty_dates: Set[str],
                          archiver_name: str,
                          updater_name: str,
                          recent_trade_dates: List[str] = None) -> List[str]:
    """
    过滤出需要更新的日期
    排除：已存在的日期 + empty的日期（除了最近5个交易日）
    """
    if not trade_dates:
        return []
    
    # 标准化日期格式
    normalized_trade_dates = set()
    for d in trade_dates:
        if d is not None:
            if hasattr(d, 'strftime'):
                normalized_trade_dates.add(d.strftime('%Y%m%d'))
            else:
                s = str(d)
                if '-' in s:
                    s = s.replace('-', '')
                if len(s) >= 8:
                    normalized_trade_dates.add(s[:8])
    
    # 排除已存在的日期
    dates_to_update = normalized_trade_dates - exist_dates
    
    # 排除empty的日期（除了最近5个交易日）
    if recent_trade_dates:
        for date in list(dates_to_update):
            if date in empty_dates and not is_recent_trading_day(date, recent_trade_dates, 5):
                dates_to_update.remove(date)
    
    return sorted(list(dates_to_update))

def update_empty_dates_after_fetch(archiver_name: str, 
                                 updater_name: str, 
                                 date: str, 
                                 data_empty: bool,
                                 recent_trade_dates: List[str] = None):
    """
    在数据获取后更新empty_dates
    如果数据为空且不是最近5个交易日，则添加到empty_dates
    """
    if data_empty:
        # 检查是否为最近5个交易日
        if recent_trade_dates and is_recent_trading_day(date, recent_trade_dates, 5):
            return
        
        # 添加到empty_dates
        add_empty_date(archiver_name, updater_name, date)
        logger.info(f"添加empty_date: {archiver_name}.{updater_name} - {date}")
    else:
        # 如果数据不为空，从empty_dates中移除（如果存在）
        empty_dates = load_empty_dates()
        if (archiver_name in empty_dates and 
            updater_name in empty_dates[archiver_name] and 
            date in empty_dates[archiver_name][updater_name]):
            
            empty_dates[archiver_name][updater_name].remove(date)
            save_empty_dates(empty_dates)
            logger.info(f"移除empty_date（数据已恢复）: {archiver_name}.{updater_name} - {date}")

def get_recent_trade_dates(trade_dates: List[str], days: int = 5) -> List[str]:
    """
    获取最近N个交易日
    """
    if not trade_dates:
        return []
    
    try:
        # 转换为datetime对象并排序
        date_objects = []
        for d in trade_dates:
            if isinstance(d, str) and len(d) == 8:
                date_objects.append(datetime.strptime(d, '%Y%m%d'))
        
        if not date_objects:
            return []
        
        # 排序并获取最近的N个交易日
        date_objects.sort(reverse=True)
        recent_dates = date_objects[:days]
        
        # 转换回字符串格式
        return [d.strftime('%Y%m%d') for d in recent_dates]
        
    except Exception as e:
        logger.warning(f"获取最近交易日失败: {e}")
        return [] 

def generate_date_range(start_date: str, end_date: str, include_next_day: bool = False) -> List[str]:
    """
    生成指定范围内的所有日历日期（包括非交易日）
    
    Args:
        start_date: 开始日期，支持 'YYYY-MM-DD' 或 'YYYYMMDD' 格式
        end_date: 结束日期，支持 'YYYY-MM-DD' 或 'YYYYMMDD' 格式
        include_next_day: 是否包含结束日期的下一天，用于捕获延迟发布的数据
        
    Returns:
        日期字符串列表，格式为 YYYYMMDD
    """
    try:
        # 标准化日期格式
        def normalize_date_input(date_str):
            if len(date_str) == 8:  # YYYYMMDD格式
                return datetime.strptime(date_str, '%Y%m%d')
            elif len(date_str) == 10:  # YYYY-MM-DD格式
                return datetime.strptime(date_str, '%Y-%m-%d')
            else:
                raise ValueError(f"不支持的日期格式: {date_str}")
        
        start = normalize_date_input(start_date)
        end = normalize_date_input(end_date)
        
        # 如果需要包含下一天，扩展结束日期
        if include_next_day:
            end = end + timedelta(days=1)
        
        if start > end:
            raise ValueError(f"开始日期 {start_date} 不能晚于结束日期 {end_date}")
        
        dates = []
        current = start
        while current <= end:
            dates.append(current.strftime('%Y%m%d'))
            current += timedelta(days=1)
        
        next_day_info = " (包含下一天)" if include_next_day else ""
        logger.info(f"生成日期范围: {start_date} 到 {end_date}{next_day_info}，共 {len(dates)} 个日期")
        return dates
        
    except Exception as e:
        logger.error(f"生成日期范围失败: {e}")
        return []

def get_all_stock_codes(list_status: str = None) -> List[str]:
    """
    从stock_basic表获取所有股票代码
    
    Args:
        list_status: 上市状态，L上市 D退市 P暂停上市，默认为None（获取所有状态股票）
    
    Returns:
        股票代码列表，如 ['000001.SZ', '000002.SZ', ...]
    """
    try:
        conn = pymysql.connect(
            host=Config.MYSQL_HOST,
            port=Config.MYSQL_PORT,
            user=Config.MYSQL_USER,
            password=Config.MYSQL_PASSWORD,
            database=Config.MYSQL_DATABASE,
            charset='utf8mb4'
        )
        
        if list_status is None:
            # 获取所有状态的股票（量化交易回测需要）
            sql = "SELECT ts_code FROM stock_basic ORDER BY ts_code"
            with conn.cursor() as cursor:
                cursor.execute(sql)
                rows = cursor.fetchall()
        else:
            # 按指定状态筛选
            sql = "SELECT ts_code FROM stock_basic WHERE list_status = %s ORDER BY ts_code"
            with conn.cursor() as cursor:
                cursor.execute(sql, (list_status,))
                rows = cursor.fetchall()
        
        conn.close()
        
        # 返回股票代码列表
        stock_codes = [row[0] for row in rows]
        status_desc = "所有状态" if list_status is None else f"状态: {list_status}"
        logger.info(f"获取到 {len(stock_codes)} 个股票代码（{status_desc}）")
        return stock_codes
        
    except Exception as e:
        logger.error(f"获取股票代码失败: {e}")
        return []

def convert_dates(df, date_fields):
    for field in date_fields:
        if field in df.columns:
            df[field] = pd.to_datetime(df[field], errors='coerce').astype('datetime64[ns]')
    return df

def safe_db_ready(df, use_fields):
    # 将所有datetime64的NaT转为None，所有NaN也转为None
    for col in use_fields:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].replace({pd.NaT: None})
        df[col] = df[col].replace({np.nan: None})
    return df.where(pd.notnull(df), None)

def get_trade_dates(conn, start_date, end_date):
    sql = f"""
    SELECT cal_date FROM trade_cal
    WHERE exchange='SSE' AND is_open=1 AND cal_date >= %s AND cal_date <= %s
    ORDER BY cal_date
    """
    with conn.cursor() as cursor:
        cursor.execute(sql, (start_date, end_date))
        rows = cursor.fetchall()
    # 返回YYYYMMDD字符串列表
    return [row[0].strftime('%Y%m%d') if hasattr(row[0], 'strftime') else row[0] for row in rows]

def normalize_dates(date_list):
    """
    将日期列表中的每个元素转为YYYYMMDD字符串，支持datetime/date/str等多种类型。
    """
    normed = set()
    for d in date_list:
        if d is None:
            continue
        if hasattr(d, 'strftime'):
            normed.add(d.strftime('%Y%m%d'))
        else:
            s = str(d)
            if '-' in s:
                s = s.replace('-', '')[:8]
            normed.add(s)
    return normed