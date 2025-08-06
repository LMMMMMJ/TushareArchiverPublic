import sys
import os
from types import NoneType
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import tushare as ts
import pandas as pd
import numpy as np
import pymysql
from config import Config
from loguru import logger
from update_mode import STOCK_INFO_ARCHIVER_UPDATE_MODE
from rich.progress import track
from utils import (
    get_empty_dates_for_updater, 
    filter_dates_for_update, 
    update_empty_dates_after_fetch,
    get_recent_trade_dates,
    convert_dates,
    safe_db_ready,
    normalize_dates,
    get_trade_dates,
    get_all_stock_codes,
    is_recent_trading_day,
    generate_date_range
)


class StockInfoBaseUpdater:
    """
    股票信息相关数据更新基类，负责数据库连接、Tushare初始化、建表等通用操作。
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

class Stock_BasicUpdater(StockInfoBaseUpdater):
    """
    股票基本信息表的更新器，支持全量/增量更新，字段与Tushare官方文档保持一致。
    https://tushare.pro/document/2?doc_id=25
    """
    def __init__(self):
        super().__init__()
        self.table_name = 'stock_basic'
        # 官方文档所有字段
        self.columns = [
            'ts_code', 'symbol', 'name', 'area', 'industry', 'fullname', 'enname', 'cnspell',
            'market', 'exchange', 'curr_type', 'list_status', 'list_date', 'delist_date',
            'is_hs', 'act_name', 'act_ent_type'
        ]
        self.create_table(self._get_create_sql())

    def _get_create_sql(self):
        # 字段类型与接口文档保持一致
        return f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            ts_code VARCHAR(20) PRIMARY KEY,
            symbol VARCHAR(20),
            name VARCHAR(100),
            area VARCHAR(50),
            industry VARCHAR(100),
            fullname VARCHAR(200),
            enname VARCHAR(200),
            cnspell VARCHAR(50),
            market VARCHAR(50),
            exchange VARCHAR(20),
            curr_type VARCHAR(20),
            list_status VARCHAR(20),
            list_date VARCHAR(20),
            delist_date VARCHAR(20),
            is_hs VARCHAR(20),
            act_name VARCHAR(200),
            act_ent_type VARCHAR(100)
        ) DEFAULT CHARSET=utf8mb4;
        """

    def update(self, mode='full', fields=None):
        """
        更新股票基本信息表
        mode: 'full' 全量更新（覆写），'increment' 增量更新
        fields: 指定字段列表，默认全字段
        """
        use_fields = fields if fields else self.columns
        # tushare接口支持fields参数，提升效率
        df = self.pro.stock_basic(fields=','.join(use_fields))
        # 统一日期字段
        date_fields = ['list_date', 'delist_date']
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

class Stock_NameChangeUpdater(StockInfoBaseUpdater):
    """
    股票曾用名数据更新器，支持全量/增量更新，字段与Tushare官方文档保持一致。
    https://tushare.pro/document/2?doc_id=100
    """
    def __init__(self):
        super().__init__()
        self.table_name = 'stock_namechange'
        # 官方文档所有字段
        self.columns = [
            'ts_code', 'name', 'start_date', 'end_date', 'ann_date', 'change_reason'
        ]
        self.create_table(self._get_create_sql())

    def _get_create_sql(self):
        # 字段类型与接口文档保持一致
        return f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            ts_code VARCHAR(20),
            name VARCHAR(100),
            start_date VARCHAR(20),
            end_date VARCHAR(20),
            ann_date VARCHAR(20),
            change_reason VARCHAR(200),
            PRIMARY KEY(ts_code, start_date, name)
        ) DEFAULT CHARSET=utf8mb4;
        """

    def update(self, mode='full', fields=None, dates=None):
        """
        更新股票曾用名数据
        mode: 'full' 全量更新（覆写），'increment' 增量更新
        fields: 指定字段列表，默认全字段
        dates: 日期列表，用于按公告日期分批拉取
        """
        use_fields = fields if fields else self.columns
        assert dates is not None, "dates参数不能为空"
        
        # 获取empty_dates和最近日历日期
        empty_dates = get_empty_dates_for_updater('StockInfoArchiver', 'stock_namechange')
        recent_calendar_dates = self._get_recent_calendar_dates(dates, 5)
        
        if mode == 'full':
            self.truncate_table(self.table_name)
            dates_to_update = dates
        else:
            # 获取已存在的公告日期
            with self.conn.cursor() as cursor:
                cursor.execute(f"SELECT DISTINCT ann_date FROM {self.table_name}")
                exist_dates = normalize_dates([row[0] for row in cursor.fetchall()])
            
            # 删除最近3个日历日的数据，确保数据及时性
            recent_3_calendar_dates = self._get_recent_calendar_dates(dates, 3)
            if recent_3_calendar_dates:
                recent_3_dates_str = "', '".join(recent_3_calendar_dates)
                delete_sql = f"DELETE FROM {self.table_name} WHERE ann_date IN ('{recent_3_dates_str}')"
                with self.conn.cursor() as cursor:
                    cursor.execute(delete_sql)
                self.conn.commit()
                logger.info(f"删除最近3个日历日的数据: {recent_3_calendar_dates}")
                
                # 从已存在日期中移除最近3个日历日
                exist_dates = exist_dates - set(recent_3_calendar_dates)
            
            # 过滤出需要更新的日期（排除已存在和empty的日期，但最近5个日历日不进入empty_dates）
            dates_to_update = []
            for date in dates:
                if date in exist_dates:
                    continue
                if date in empty_dates and date not in recent_calendar_dates:
                    continue
                dates_to_update.append(date)
        
        for ann_date in track(dates_to_update, description=f"stock_namechange {mode} updating"):
            # 按公告日期分批拉取
            df = self.pro.namechange(start_date=ann_date, end_date=ann_date, fields=','.join(use_fields))
            
            # 更新empty_dates（最近5个日历日不进入empty_dates）
            if ann_date not in recent_calendar_dates:
                update_empty_dates_after_fetch(
                    'StockInfoArchiver', 'stock_namechange', ann_date, 
                    df.empty, recent_calendar_dates
                )
            
            if df.empty:
                continue
                
            # 转换日期字段
            df = convert_dates(df, [f for f in ['start_date', 'end_date', 'ann_date'] if f in use_fields])
            if not df.empty:
                df[use_fields] = safe_db_ready(df[use_fields], use_fields)
                insert_sql = f"""
                REPLACE INTO {self.table_name} ({', '.join(use_fields)}) VALUES ({', '.join(['%s']*len(use_fields))})
                """
                with self.conn.cursor() as cursor:
                    cursor.executemany(insert_sql, df[use_fields].values.tolist())
                self.conn.commit()

    def _get_recent_calendar_dates(self, reference_dates, days=5):
        """
        基于参考日期，获取最近N个日历日期
        
        Args:
            reference_dates: 参考日期列表
            days: 需要获取的日历日期数量
            
        Returns:
            最近N个日历日期列表
        """
        if not reference_dates:
            return []
        
        from datetime import datetime, timedelta
        
        try:
            # 获取最新的参考日期
            latest_date_str = max(reference_dates)
            if isinstance(latest_date_str, str) and len(latest_date_str) == 8:
                latest_date = datetime.strptime(latest_date_str, '%Y%m%d')
            else:
                return []
            
            # 生成最近N个日历日期
            calendar_dates = []
            for i in range(days):
                date = latest_date - timedelta(days=i)
                calendar_dates.append(date.strftime('%Y%m%d'))
            
            return sorted(calendar_dates, reverse=True)
            
        except Exception as e:
            logger.warning(f"获取最近日历日期失败: {e}")
            return []

class Stock_DailyUpdater(StockInfoBaseUpdater):
    """
    A股日线行情数据更新器，支持分批按交易日拉取，字段与Tushare官方文档一致。
    https://tushare.pro/document/2?doc_id=27
    """
    def __init__(self):
        super().__init__()
        self.table_name = 'stock_daily'
        self.columns = [
            'ts_code', 'trade_date', 'open', 'high', 'low', 'close', 'pre_close', 
            'change', 'pct_chg', 'vol', 'amount'
        ]
        self.create_table(self._get_create_sql())

    def _get_create_sql(self):
        return f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            ts_code VARCHAR(20),
            trade_date DATE,
            open FLOAT,
            high FLOAT,
            low FLOAT,
            close FLOAT,
            pre_close FLOAT,
            `change` FLOAT,
            pct_chg FLOAT,
            vol FLOAT,
            amount FLOAT,
            PRIMARY KEY(ts_code, trade_date)
        ) DEFAULT CHARSET=utf8mb4;
        """

    def update(self, mode='full', fields=None, trade_dates=None):
        use_fields = [f'`change`' if f == 'change' else f for f in (fields if fields else self.columns)]
        raw_fields = [f.replace('`', '') for f in use_fields]
        if trade_dates is None:
            raise ValueError('Stock_DailyUpdater.update: trade_dates参数不能为空，必须分批拉取！')
        
        # 获取empty_dates和最近交易日
        empty_dates = get_empty_dates_for_updater('StockInfoArchiver', 'stock_daily')
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
                'StockInfoArchiver', 'stock_daily', recent_trade_dates
            )
        
        for trade_date in track(dates_to_update, description=f"stock_daily {mode} updating"):
            df = self.pro.daily(trade_date=trade_date, fields=','.join(raw_fields))
            
            # 更新empty_dates
            update_empty_dates_after_fetch(
                'StockInfoArchiver', 'stock_daily', trade_date, 
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

class Stock_IncomeUpdater(StockInfoBaseUpdater):
    """
    股票利润表数据更新器，使用income_vip接口按公告日期更新，支持全量/增量更新，字段与Tushare官方文档一致。
    https://tushare.pro/document/2?doc_id=33
    """
    def __init__(self):
        super().__init__()
        self.table_name = 'stock_income'
        # 官方文档所有字段
        self.columns = [
            'ts_code', 'ann_date', 'f_ann_date', 'end_date', 'report_type', 'comp_type', 'end_type',
            'basic_eps', 'diluted_eps', 'total_revenue', 'revenue', 'int_income', 'prem_earned',
            'comm_income', 'n_commis_income', 'n_oth_income', 'n_oth_b_income', 'prem_income',
            'out_prem', 'une_prem_reser', 'reins_income', 'n_sec_tb_income', 'n_sec_uw_income',
            'n_asset_mg_income', 'oth_b_income', 'fv_value_chg_gain', 'invest_income', 'ass_invest_income',
            'forex_gain', 'total_cogs', 'oper_cost', 'int_exp', 'comm_exp', 'biz_tax_surchg',
            'sell_exp', 'admin_exp', 'fin_exp', 'assets_impair_loss', 'prem_refund', 'compens_payout',
            'reser_insur_liab', 'div_payt', 'reins_exp', 'oper_exp', 'compens_payout_refu',
            'insur_reser_refu', 'reins_cost_refund', 'other_bus_cost', 'operate_profit', 'non_oper_income',
            'non_oper_exp', 'nca_disploss', 'total_profit', 'income_tax', 'n_income', 'n_income_attr_p',
            'minority_gain', 'oth_compr_income', 't_compr_income', 'compr_inc_attr_p', 'compr_inc_attr_m_s',
            'ebit', 'ebitda', 'insurance_exp', 'undist_profit', 'distable_profit', 'rd_exp',
            'fin_exp_int_exp', 'fin_exp_int_inc', 'transfer_surplus_rese', 'transfer_housing_imprest',
            'transfer_oth', 'adj_lossgain', 'withdra_legal_surplus', 'withdra_legal_pubfund',
            'withdra_biz_devfund', 'withdra_rese_fund', 'withdra_oth_ersu', 'workers_welfare',
            'distr_profit_shrhder', 'prfshare_payable_dvd', 'comshare_payable_dvd', 'capit_comstock_div',
            'net_after_nr_lp_correct', 'credit_impa_loss', 'net_expo_hedging_benefits', 'oth_impair_loss_assets',
            'total_opcost', 'amodcost_fin_assets', 'oth_income', 'asset_disp_income', 'continued_net_profit',
            'end_net_profit', 'update_flag'
        ]
        self.create_table(self._get_create_sql())

    def _get_create_sql(self):
        # 字段类型与接口文档保持一致，包含所有字段
        return f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            ts_code VARCHAR(20),
            ann_date VARCHAR(20),
            f_ann_date VARCHAR(20),
            end_date VARCHAR(20),
            report_type VARCHAR(20),
            comp_type VARCHAR(20),
            end_type VARCHAR(20),
            basic_eps FLOAT,
            diluted_eps FLOAT,
            total_revenue FLOAT,
            revenue FLOAT,
            int_income FLOAT,
            prem_earned FLOAT,
            comm_income FLOAT,
            n_commis_income FLOAT,
            n_oth_income FLOAT,
            n_oth_b_income FLOAT,
            prem_income FLOAT,
            out_prem FLOAT,
            une_prem_reser FLOAT,
            reins_income FLOAT,
            n_sec_tb_income FLOAT,
            n_sec_uw_income FLOAT,
            n_asset_mg_income FLOAT,
            oth_b_income FLOAT,
            fv_value_chg_gain FLOAT,
            invest_income FLOAT,
            ass_invest_income FLOAT,
            forex_gain FLOAT,
            total_cogs FLOAT,
            oper_cost FLOAT,
            int_exp FLOAT,
            comm_exp FLOAT,
            biz_tax_surchg FLOAT,
            sell_exp FLOAT,
            admin_exp FLOAT,
            fin_exp FLOAT,
            assets_impair_loss FLOAT,
            prem_refund FLOAT,
            compens_payout FLOAT,
            reser_insur_liab FLOAT,
            div_payt FLOAT,
            reins_exp FLOAT,
            oper_exp FLOAT,
            compens_payout_refu FLOAT,
            insur_reser_refu FLOAT,
            reins_cost_refund FLOAT,
            other_bus_cost FLOAT,
            operate_profit FLOAT,
            non_oper_income FLOAT,
            non_oper_exp FLOAT,
            nca_disploss FLOAT,
            total_profit FLOAT,
            income_tax FLOAT,
            n_income FLOAT,
            n_income_attr_p FLOAT,
            minority_gain FLOAT,
            oth_compr_income FLOAT,
            t_compr_income FLOAT,
            compr_inc_attr_p FLOAT,
            compr_inc_attr_m_s FLOAT,
            ebit FLOAT,
            ebitda FLOAT,
            insurance_exp FLOAT,
            undist_profit FLOAT,
            distable_profit FLOAT,
            rd_exp FLOAT,
            fin_exp_int_exp FLOAT,
            fin_exp_int_inc FLOAT,
            transfer_surplus_rese FLOAT,
            transfer_housing_imprest FLOAT,
            transfer_oth FLOAT,
            adj_lossgain FLOAT,
            withdra_legal_surplus FLOAT,
            withdra_legal_pubfund FLOAT,
            withdra_biz_devfund FLOAT,
            withdra_rese_fund FLOAT,
            withdra_oth_ersu FLOAT,
            workers_welfare FLOAT,
            distr_profit_shrhder FLOAT,
            prfshare_payable_dvd FLOAT,
            comshare_payable_dvd FLOAT,
            capit_comstock_div FLOAT,
            net_after_nr_lp_correct FLOAT,
            credit_impa_loss FLOAT,
            net_expo_hedging_benefits FLOAT,
            oth_impair_loss_assets FLOAT,
            total_opcost FLOAT,
            amodcost_fin_assets FLOAT,
            oth_income FLOAT,
            asset_disp_income FLOAT,
            continued_net_profit FLOAT,
            end_net_profit FLOAT,
            update_flag VARCHAR(20),
            PRIMARY KEY(ts_code, ann_date, end_date, report_type)
        ) DEFAULT CHARSET=utf8mb4;
        """

    def _get_recent_calendar_dates(self, reference_dates, days=5):
        """
        基于参考日期，获取最近N个日历日期
        
        Args:
            reference_dates: 参考日期列表
            days: 需要获取的日历日期数量
            
        Returns:
            最近N个日历日期列表
        """
        if not reference_dates:
            return []
        
        from datetime import datetime, timedelta
        
        try:
            # 获取最新的参考日期
            latest_date_str = max(reference_dates)
            if isinstance(latest_date_str, str) and len(latest_date_str) == 8:
                latest_date = datetime.strptime(latest_date_str, '%Y%m%d')
            else:
                return []
            
            # 生成最近N个日历日期
            calendar_dates = []
            for i in range(days):
                date = latest_date - timedelta(days=i)
                calendar_dates.append(date.strftime('%Y%m%d'))
            
            return sorted(calendar_dates, reverse=True)
            
        except Exception as e:
            logger.warning(f"获取最近日历日期失败: {e}")
            return []

    def update(self, mode='full', fields=None, dates=None):
        """
        更新股票利润表数据，使用income_vip接口按公告日期更新
        mode: 'full' 全量更新（覆写），'increment' 增量更新
        fields: 指定字段列表，默认使用全部字段
        dates: 日期列表，优先使用
        start_date: 开始日期，支持 'YYYY-MM-DD' 或 'YYYYMMDD' 格式
        end_date: 结束日期，支持 'YYYY-MM-DD' 或 'YYYYMMDD' 格式
        """
        use_fields = fields if fields else self.columns
        
        # 确定要更新的日期范围
        assert dates is not None,"dates参数不能为空"
        calendar_dates = dates

        if not calendar_dates:
            logger.warning("没有可更新的日期")
            return
        
        # 获取empty_dates和最近日历日期
        empty_dates = get_empty_dates_for_updater('StockInfoArchiver', 'stock_income')
        recent_calendar_dates = self._get_recent_calendar_dates(calendar_dates, 5)
        
        if mode == 'full':
            self.truncate_table(self.table_name)
            dates_to_update = calendar_dates
        else:
            # 获取已存在的公告日期
            with self.conn.cursor() as cursor:
                cursor.execute(f"SELECT DISTINCT ann_date FROM {self.table_name}")
                exist_dates = normalize_dates([row[0] for row in cursor.fetchall()])
            
            # 删除最近3个日历日的数据，确保数据及时性
            recent_3_calendar_dates = self._get_recent_calendar_dates(calendar_dates, 3)
            if recent_3_calendar_dates:
                recent_3_dates_str = "', '".join(recent_3_calendar_dates)
                delete_sql = f"DELETE FROM {self.table_name} WHERE ann_date IN ('{recent_3_dates_str}')"
                with self.conn.cursor() as cursor:
                    cursor.execute(delete_sql)
                self.conn.commit()
                logger.info(f"删除最近3个日历日的数据: {recent_3_calendar_dates}")
                
                # 从已存在日期中移除最近3个日历日
                exist_dates = exist_dates - set(recent_3_calendar_dates)
            
            # 过滤出需要更新的日期（排除已存在和empty的日期，但最近5个日历日不进入empty_dates）
            dates_to_update = []
            for date in calendar_dates:
                if date in exist_dates:
                    continue
                if date in empty_dates and date not in recent_calendar_dates:
                    continue
                dates_to_update.append(date)
        
        for ann_date in track(dates_to_update, description=f"stock_income {mode} updating"):
            try:
                # 使用income_vip接口按公告日期拉取数据
                df = self.pro.income_vip(ann_date=ann_date, fields=','.join(use_fields))
                
                # 更新empty_dates（最近5个日历日不进入empty_dates）
                if ann_date not in recent_calendar_dates:
                    update_empty_dates_after_fetch(
                        'StockInfoArchiver', 'stock_income', ann_date, 
                        df.empty, recent_calendar_dates
                    )
                
                if df.empty:
                    continue
                    
                # 转换日期字段
                df = convert_dates(df, [f for f in ['ann_date', 'f_ann_date', 'end_date'] if f in use_fields])
                if not df.empty:
                    df[use_fields] = safe_db_ready(df[use_fields], use_fields)
                    insert_sql = f"""
                    REPLACE INTO {self.table_name} ({', '.join(use_fields)}) VALUES ({', '.join(['%s']*len(use_fields))})
                    """
                    with self.conn.cursor() as cursor:
                        cursor.executemany(insert_sql, df[use_fields].values.tolist())
                    self.conn.commit()
                
            except Exception as e:
                logger.error(f"获取 {ann_date} 日期的利润表数据失败: {e}")
                continue

class Stock_CashflowUpdater(StockInfoBaseUpdater):
    """
    股票现金流量表数据更新器，使用cashflow_vip接口按公告日期更新，支持全量/增量更新，字段与Tushare官方文档一致。
    https://tushare.pro/document/2?doc_id=44
    """
    def __init__(self):
        super().__init__()
        self.table_name = 'stock_cashflow'
        # 官方文档所有字段
        self.columns = [
            'ts_code', 'ann_date', 'f_ann_date', 'end_date', 'comp_type', 'report_type',
            'net_profit', 'finan_exp', 'c_fr_sale_sg', 'recp_tax_rends', 'n_depos_incr_fi',
            'n_incr_loans_cb', 'n_inc_borr_oth_fi', 'prem_fr_orig_contr', 'n_incr_insured_dep',
            'n_reinsur_prem', 'n_incr_disp_tfa', 'ifc_cash_incr', 'n_incr_disp_faas',
            'n_incr_loans_oth_bank', 'n_cap_incr_repur', 'c_fr_oth_operate_a', 'c_inf_fr_operate_a',
            'c_paid_goods_s', 'c_paid_to_for_empl', 'c_paid_for_taxes', 'n_incr_clt_loan_adv',
            'n_incr_dep_cbob', 'c_pay_claims_orig_inco', 'pay_handling_chrg', 'pay_comm_insur_plcy',
            'oth_cash_pay_oper_act', 'st_cash_out_act', 'n_cashflow_act', 'oth_recp_ral_inv_act',
            'c_disp_withdrwl_invest', 'c_recp_return_invest', 'n_recp_disp_fiolta', 'n_recp_disp_sobu',
            'stot_inflows_inv_act', 'c_pay_acq_const_fiolta', 'c_paid_invest', 'n_disp_subs_oth_biz',
            'oth_pay_ral_inv_act', 'n_incr_pledge_loan', 'stot_out_inv_act', 'n_cashflow_inv_act',
            'c_recp_borrow', 'proc_issue_bonds', 'oth_cash_recp_ral_fnc_act', 'stot_cash_in_fnc_act',
            'free_cashflow', 'c_prepay_amt_borr', 'c_pay_dist_dpcp_int_exp', 'incl_dvd_profit_paid_sc_ms',
            'oth_cashpay_ral_fnc_act', 'stot_cashout_fnc_act', 'n_cash_flows_fnc_act', 'eff_fx_flu_cash',
            'n_incr_cash_cash_equ', 'c_cash_equ_beg_period', 'c_cash_equ_end_period', 'c_recp_cap_contrib',
            'incl_cash_rec_saims', 'uncon_invest_loss', 'prov_depr_assets', 'depr_fa_coga_dpba',
            'amort_intang_assets', 'lt_amort_deferred_exp', 'decr_deferred_exp', 'incr_acc_exp',
            'loss_disp_fiolta', 'loss_scr_fa', 'loss_fv_chg', 'invest_loss', 'decr_def_inc_tax_assets',
            'incr_def_inc_tax_liab', 'decr_inventories', 'decr_oper_payable', 'incr_oper_payable',
            'others', 'im_net_cashflow_oper_act', 'conv_debt_into_cap', 'conv_copbonds_due_within_1y',
            'fa_fnc_leases', 'im_n_incr_cash_equ', 'net_dism_capital_add', 'net_cash_rece_sec',
            'credit_impa_loss', 'use_right_asset_dep', 'oth_loss_asset', 'end_bal_cash',
            'beg_bal_cash', 'end_bal_cash_equ', 'beg_bal_cash_equ', 'update_flag'
        ]
        self.create_table(self._get_create_sql())

    def _get_create_sql(self):
        # 字段类型与接口文档保持一致，包含所有字段
        return f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            ts_code VARCHAR(20),
            ann_date VARCHAR(20),
            f_ann_date VARCHAR(20),
            end_date VARCHAR(20),
            comp_type VARCHAR(20),
            report_type VARCHAR(20),
            net_profit FLOAT,
            finan_exp FLOAT,
            c_fr_sale_sg FLOAT,
            recp_tax_rends FLOAT,
            n_depos_incr_fi FLOAT,
            n_incr_loans_cb FLOAT,
            n_inc_borr_oth_fi FLOAT,
            prem_fr_orig_contr FLOAT,
            n_incr_insured_dep FLOAT,
            n_reinsur_prem FLOAT,
            n_incr_disp_tfa FLOAT,
            ifc_cash_incr FLOAT,
            n_incr_disp_faas FLOAT,
            n_incr_loans_oth_bank FLOAT,
            n_cap_incr_repur FLOAT,
            c_fr_oth_operate_a FLOAT,
            c_inf_fr_operate_a FLOAT,
            c_paid_goods_s FLOAT,
            c_paid_to_for_empl FLOAT,
            c_paid_for_taxes FLOAT,
            n_incr_clt_loan_adv FLOAT,
            n_incr_dep_cbob FLOAT,
            c_pay_claims_orig_inco FLOAT,
            pay_handling_chrg FLOAT,
            pay_comm_insur_plcy FLOAT,
            oth_cash_pay_oper_act FLOAT,
            st_cash_out_act FLOAT,
            n_cashflow_act FLOAT,
            oth_recp_ral_inv_act FLOAT,
            c_disp_withdrwl_invest FLOAT,
            c_recp_return_invest FLOAT,
            n_recp_disp_fiolta FLOAT,
            n_recp_disp_sobu FLOAT,
            stot_inflows_inv_act FLOAT,
            c_pay_acq_const_fiolta FLOAT,
            c_paid_invest FLOAT,
            n_disp_subs_oth_biz FLOAT,
            oth_pay_ral_inv_act FLOAT,
            n_incr_pledge_loan FLOAT,
            stot_out_inv_act FLOAT,
            n_cashflow_inv_act FLOAT,
            c_recp_borrow FLOAT,
            proc_issue_bonds FLOAT,
            oth_cash_recp_ral_fnc_act FLOAT,
            stot_cash_in_fnc_act FLOAT,
            free_cashflow FLOAT,
            c_prepay_amt_borr FLOAT,
            c_pay_dist_dpcp_int_exp FLOAT,
            incl_dvd_profit_paid_sc_ms FLOAT,
            oth_cashpay_ral_fnc_act FLOAT,
            stot_cashout_fnc_act FLOAT,
            n_cash_flows_fnc_act FLOAT,
            eff_fx_flu_cash FLOAT,
            n_incr_cash_cash_equ FLOAT,
            c_cash_equ_beg_period FLOAT,
            c_cash_equ_end_period FLOAT,
            c_recp_cap_contrib FLOAT,
            incl_cash_rec_saims FLOAT,
            uncon_invest_loss FLOAT,
            prov_depr_assets FLOAT,
            depr_fa_coga_dpba FLOAT,
            amort_intang_assets FLOAT,
            lt_amort_deferred_exp FLOAT,
            decr_deferred_exp FLOAT,
            incr_acc_exp FLOAT,
            loss_disp_fiolta FLOAT,
            loss_scr_fa FLOAT,
            loss_fv_chg FLOAT,
            invest_loss FLOAT,
            decr_def_inc_tax_assets FLOAT,
            incr_def_inc_tax_liab FLOAT,
            decr_inventories FLOAT,
            decr_oper_payable FLOAT,
            incr_oper_payable FLOAT,
            others FLOAT,
            im_net_cashflow_oper_act FLOAT,
            conv_debt_into_cap FLOAT,
            conv_copbonds_due_within_1y FLOAT,
            fa_fnc_leases FLOAT,
            im_n_incr_cash_equ FLOAT,
            net_dism_capital_add FLOAT,
            net_cash_rece_sec FLOAT,
            credit_impa_loss FLOAT,
            use_right_asset_dep FLOAT,
            oth_loss_asset FLOAT,
            end_bal_cash FLOAT,
            beg_bal_cash FLOAT,
            end_bal_cash_equ FLOAT,
            beg_bal_cash_equ FLOAT,
            update_flag VARCHAR(20),
            PRIMARY KEY(ts_code, ann_date, end_date, report_type)
        ) DEFAULT CHARSET=utf8mb4;
        """

    def _get_recent_calendar_dates(self, reference_dates, days=5):
        """
        基于参考日期，获取最近N个日历日期
        
        Args:
            reference_dates: 参考日期列表
            days: 需要获取的日历日期数量
            
        Returns:
            最近N个日历日期列表
        """
        if not reference_dates:
            return []
        
        from datetime import datetime, timedelta
        
        try:
            # 获取最新的参考日期
            latest_date_str = max(reference_dates)
            if isinstance(latest_date_str, str) and len(latest_date_str) == 8:
                latest_date = datetime.strptime(latest_date_str, '%Y%m%d')
            else:
                return []
            
            # 生成最近N个日历日期
            calendar_dates = []
            for i in range(days):
                date = latest_date - timedelta(days=i)
                calendar_dates.append(date.strftime('%Y%m%d'))
            
            return sorted(calendar_dates, reverse=True)
            
        except Exception as e:
            logger.warning(f"获取最近日历日期失败: {e}")
            return []

    def update(self, mode='full', fields=None, dates=None):
        """
        更新股票现金流量表数据，使用cashflow_vip接口按公告日期更新
        mode: 'full' 全量更新（覆写），'increment' 增量更新
        fields: 指定字段列表，默认使用全部字段
        dates: 日期列表，用于按公告日期分批拉取
        """
        use_fields = fields if fields else self.columns
        assert dates is not None, "dates参数不能为空"
        
        if not dates:
            logger.warning("没有可更新的日期")
            return
        
        # 获取empty_dates和最近日历日期
        empty_dates = get_empty_dates_for_updater('StockInfoArchiver', 'stock_cashflow')
        recent_calendar_dates = self._get_recent_calendar_dates(dates, 5)
        
        if mode == 'full':
            self.truncate_table(self.table_name)
            dates_to_update = dates
        else:
            # 获取已存在的公告日期
            with self.conn.cursor() as cursor:
                cursor.execute(f"SELECT DISTINCT ann_date FROM {self.table_name}")
                exist_dates = normalize_dates([row[0] for row in cursor.fetchall()])
            
            # 删除最近3个日历日的数据，确保数据及时性
            recent_3_calendar_dates = self._get_recent_calendar_dates(dates, 3)
            if recent_3_calendar_dates:
                recent_3_dates_str = "', '".join(recent_3_calendar_dates)
                delete_sql = f"DELETE FROM {self.table_name} WHERE ann_date IN ('{recent_3_dates_str}')"
                with self.conn.cursor() as cursor:
                    cursor.execute(delete_sql)
                self.conn.commit()
                logger.info(f"删除最近3个日历日的数据: {recent_3_calendar_dates}")
                
                # 从已存在日期中移除最近3个日历日
                exist_dates = exist_dates - set(recent_3_calendar_dates)
            
            # 过滤出需要更新的日期（排除已存在和empty的日期，但最近5个日历日不进入empty_dates）
            dates_to_update = []
            for date in dates:
                if date in exist_dates:
                    continue
                if date in empty_dates and date not in recent_calendar_dates:
                    continue
                dates_to_update.append(date)
        
        for ann_date in track(dates_to_update, description=f"stock_cashflow {mode} updating"):
            try:
                # 使用cashflow_vip接口按公告日期拉取数据
                df = self.pro.cashflow_vip(ann_date=ann_date, fields=','.join(use_fields))
                
                # 更新empty_dates（最近5个日历日不进入empty_dates）
                if ann_date not in recent_calendar_dates:
                    update_empty_dates_after_fetch(
                        'StockInfoArchiver', 'stock_cashflow', ann_date, 
                        df.empty, recent_calendar_dates
                    )
                
                if df.empty:
                    continue
                    
                # 转换日期字段
                df = convert_dates(df, [f for f in ['ann_date', 'f_ann_date', 'end_date'] if f in use_fields])
                if not df.empty:
                    df[use_fields] = safe_db_ready(df[use_fields], use_fields)
                    insert_sql = f"""
                    REPLACE INTO {self.table_name} ({', '.join(use_fields)}) VALUES ({', '.join(['%s']*len(use_fields))})
                    """
                    with self.conn.cursor() as cursor:
                        cursor.executemany(insert_sql, df[use_fields].values.tolist())
                    self.conn.commit()
                
            except Exception as e:
                logger.error(f"获取 {ann_date} 日期的现金流量表数据失败: {e}")
                continue

class Stock_BalancesheetUpdater(StockInfoBaseUpdater):
    """
    股票资产负债表数据更新器，使用balancesheet_vip接口按公告日期更新，支持全量/增量更新，字段与Tushare官方文档一致。
    https://tushare.pro/document/2?doc_id=36
    """
    def __init__(self):
        super().__init__()
        self.table_name = 'stock_balancesheet'
        # 官方文档所有字段（按照文档顺序）
        self.columns = [
            'ts_code', 'ann_date', 'f_ann_date', 'end_date', 'report_type', 'comp_type',
            'total_share', 'cap_rese', 'undistr_porfit', 'surplus_rese', 'special_rese', 'money_cap',
            'trad_asset', 'notes_receiv', 'accounts_receiv', 'oth_receiv', 'prepayment', 'div_receiv',
            'int_receiv', 'inventories', 'amor_exp', 'nca_within_1y', 'sett_rsrv', 'loanto_oth_bank_fi',
            'premium_receiv', 'reinsur_receiv', 'reinsur_res_receiv', 'pur_resale_fa', 'oth_cur_assets',
            'total_cur_assets', 'fa_avail_for_sale', 'htm_invest', 'lt_eqt_invest', 'invest_real_estate',
            'time_deposits', 'oth_assets', 'lt_rec', 'fix_assets', 'cip', 'const_materials', 'fixed_assets_disp',
            'produc_bio_assets', 'oil_and_gas_assets', 'intan_assets', 'r_and_d', 'goodwill', 'lt_amor_exp',
            'defer_tax_assets', 'decr_in_disbur', 'oth_nca', 'total_nca', 'cash_reser_cb', 'depos_in_oth_bfi',
            'prec_metals', 'deriv_assets', 'rr_reins_une_prem', 'rr_reins_outstd_cla', 'rr_reins_lins_liab',
            'rr_reins_lthins_liab', 'refund_depos', 'ph_pledge_loans', 'refund_cap_depos', 'indep_acct_assets',
            'client_depos', 'client_prov', 'transac_seat_fee', 'invest_as_receiv', 'total_assets',
            'lt_borr', 'st_borr', 'cb_borr', 'depos_ib_deposits', 'loan_oth_bank', 'trading_fl',
            'notes_payable', 'acct_payable', 'adv_receipts', 'sold_for_repur_fa', 'comm_payable',
            'payroll_payable', 'taxes_payable', 'int_payable', 'div_payable', 'oth_payable',
            'acc_exp', 'deferred_inc', 'st_bonds_payable', 'payable_to_reinsurer', 'rsrv_insur_cont',
            'acting_trading_sec', 'acting_uw_sec', 'non_cur_liab_due_1y', 'oth_cur_liab', 'total_cur_liab',
            'bond_payable', 'lt_payable', 'specific_payables', 'estimated_liab', 'defer_tax_liab',
            'defer_inc_non_cur_liab', 'oth_ncl', 'total_ncl', 'depos_oth_bfi', 'deriv_liab',
            'depos', 'agency_bus_liab', 'oth_liab', 'prem_receiv_adva', 'depos_received',
            'ph_invest', 'reser_une_prem', 'reser_outstd_claims', 'reser_lins_liab', 'reser_lthins_liab',
            'indept_acc_liab', 'pledge_borr', 'indem_payable', 'policy_div_payable', 'total_liab',
            'treasury_share', 'ordin_risk_reser', 'forex_differ', 'invest_loss_unconf', 'minority_int',
            'total_hldr_eqy_exc_min_int', 'total_hldr_eqy_inc_min_int', 'total_liab_hldr_eqy', 'lt_payroll_payable',
            'oth_comp_income', 'oth_eqt_tools', 'oth_eqt_tools_p_shr', 'lending_funds', 'acc_receivable',
            'st_fin_payable', 'payables', 'hfs_assets', 'hfs_sales', 'cost_fin_assets', 'fair_value_fin_assets',
            'cip_total', 'oth_pay_total', 'long_pay_total', 'debt_invest', 'oth_debt_invest', 'oth_eq_invest',
            'oth_illiq_fin_assets', 'oth_eq_ppbond', 'receiv_financing', 'use_right_assets', 'lease_liab',
            'contract_assets', 'contract_liab', 'accounts_receiv_bill', 'accounts_pay', 'oth_rcv_total',
            'fix_assets_total', 'update_flag'
        ]
        self.create_table(self._get_create_sql())

    def _get_create_sql(self):
        # 字段类型与接口文档保持一致，包含所有字段
        return f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            ts_code VARCHAR(20),
            ann_date VARCHAR(20),
            f_ann_date VARCHAR(20),
            end_date VARCHAR(20),
            report_type VARCHAR(20),
            comp_type VARCHAR(20),
            total_share FLOAT,
            cap_rese FLOAT,
            undistr_porfit FLOAT,
            surplus_rese FLOAT,
            special_rese FLOAT,
            money_cap FLOAT,
            trad_asset FLOAT,
            notes_receiv FLOAT,
            accounts_receiv FLOAT,
            oth_receiv FLOAT,
            prepayment FLOAT,
            div_receiv FLOAT,
            int_receiv FLOAT,
            inventories FLOAT,
            amor_exp FLOAT,
            nca_within_1y FLOAT,
            sett_rsrv FLOAT,
            loanto_oth_bank_fi FLOAT,
            premium_receiv FLOAT,
            reinsur_receiv FLOAT,
            reinsur_res_receiv FLOAT,
            pur_resale_fa FLOAT,
            oth_cur_assets FLOAT,
            total_cur_assets FLOAT,
            fa_avail_for_sale FLOAT,
            htm_invest FLOAT,
            lt_eqt_invest FLOAT,
            invest_real_estate FLOAT,
            time_deposits FLOAT,
            oth_assets FLOAT,
            lt_rec FLOAT,
            fix_assets FLOAT,
            cip FLOAT,
            const_materials FLOAT,
            fixed_assets_disp FLOAT,
            produc_bio_assets FLOAT,
            oil_and_gas_assets FLOAT,
            intan_assets FLOAT,
            r_and_d FLOAT,
            goodwill FLOAT,
            lt_amor_exp FLOAT,
            defer_tax_assets FLOAT,
            decr_in_disbur FLOAT,
            oth_nca FLOAT,
            total_nca FLOAT,
            cash_reser_cb FLOAT,
            depos_in_oth_bfi FLOAT,
            prec_metals FLOAT,
            deriv_assets FLOAT,
            rr_reins_une_prem FLOAT,
            rr_reins_outstd_cla FLOAT,
            rr_reins_lins_liab FLOAT,
            rr_reins_lthins_liab FLOAT,
            refund_depos FLOAT,
            ph_pledge_loans FLOAT,
            refund_cap_depos FLOAT,
            indep_acct_assets FLOAT,
            client_depos FLOAT,
            client_prov FLOAT,
            transac_seat_fee FLOAT,
            invest_as_receiv FLOAT,
            total_assets FLOAT,
            lt_borr FLOAT,
            st_borr FLOAT,
            cb_borr FLOAT,
            depos_ib_deposits FLOAT,
            loan_oth_bank FLOAT,
            trading_fl FLOAT,
            notes_payable FLOAT,
            acct_payable FLOAT,
            adv_receipts FLOAT,
            sold_for_repur_fa FLOAT,
            comm_payable FLOAT,
            payroll_payable FLOAT,
            taxes_payable FLOAT,
            int_payable FLOAT,
            div_payable FLOAT,
            oth_payable FLOAT,
            acc_exp FLOAT,
            deferred_inc FLOAT,
            st_bonds_payable FLOAT,
            payable_to_reinsurer FLOAT,
            rsrv_insur_cont FLOAT,
            acting_trading_sec FLOAT,
            acting_uw_sec FLOAT,
            non_cur_liab_due_1y FLOAT,
            oth_cur_liab FLOAT,
            total_cur_liab FLOAT,
            bond_payable FLOAT,
            lt_payable FLOAT,
            specific_payables FLOAT,
            estimated_liab FLOAT,
            defer_tax_liab FLOAT,
            defer_inc_non_cur_liab FLOAT,
            oth_ncl FLOAT,
            total_ncl FLOAT,
            depos_oth_bfi FLOAT,
            deriv_liab FLOAT,
            depos FLOAT,
            agency_bus_liab FLOAT,
            oth_liab FLOAT,
            prem_receiv_adva FLOAT,
            depos_received FLOAT,
            ph_invest FLOAT,
            reser_une_prem FLOAT,
            reser_outstd_claims FLOAT,
            reser_lins_liab FLOAT,
            reser_lthins_liab FLOAT,
            indept_acc_liab FLOAT,
            pledge_borr FLOAT,
            indem_payable FLOAT,
            policy_div_payable FLOAT,
            total_liab FLOAT,
            treasury_share FLOAT,
            ordin_risk_reser FLOAT,
            forex_differ FLOAT,
            invest_loss_unconf FLOAT,
            minority_int FLOAT,
            total_hldr_eqy_exc_min_int FLOAT,
            total_hldr_eqy_inc_min_int FLOAT,
            total_liab_hldr_eqy FLOAT,
            lt_payroll_payable FLOAT,
            oth_comp_income FLOAT,
            oth_eqt_tools FLOAT,
            oth_eqt_tools_p_shr FLOAT,
            lending_funds FLOAT,
            acc_receivable FLOAT,
            st_fin_payable FLOAT,
            payables FLOAT,
            hfs_assets FLOAT,
            hfs_sales FLOAT,
            cost_fin_assets FLOAT,
            fair_value_fin_assets FLOAT,
            cip_total FLOAT,
            oth_pay_total FLOAT,
            long_pay_total FLOAT,
            debt_invest FLOAT,
            oth_debt_invest FLOAT,
            oth_eq_invest FLOAT,
            oth_illiq_fin_assets FLOAT,
            oth_eq_ppbond FLOAT,
            receiv_financing FLOAT,
            use_right_assets FLOAT,
            lease_liab FLOAT,
            contract_assets FLOAT,
            contract_liab FLOAT,
            accounts_receiv_bill FLOAT,
            accounts_pay FLOAT,
            oth_rcv_total FLOAT,
            fix_assets_total FLOAT,
            update_flag VARCHAR(20),
            PRIMARY KEY(ts_code, ann_date, end_date, report_type)
        ) DEFAULT CHARSET=utf8mb4;
        """

    def _get_recent_calendar_dates(self, reference_dates, days=5):
        """
        基于参考日期，获取最近N个日历日期
        
        Args:
            reference_dates: 参考日期列表
            days: 需要获取的日历日期数量
            
        Returns:
            最近N个日历日期列表
        """
        if not reference_dates:
            return []
        
        from datetime import datetime, timedelta
        
        try:
            # 获取最新的参考日期
            latest_date_str = max(reference_dates)
            if isinstance(latest_date_str, str) and len(latest_date_str) == 8:
                latest_date = datetime.strptime(latest_date_str, '%Y%m%d')
            else:
                return []
            
            # 生成最近N个日历日期
            calendar_dates = []
            for i in range(days):
                date = latest_date - timedelta(days=i)
                calendar_dates.append(date.strftime('%Y%m%d'))
            
            return sorted(calendar_dates, reverse=True)
            
        except Exception as e:
            logger.warning(f"获取最近日历日期失败: {e}")
            return []

    def update(self, mode='full', fields=None, dates=None):
        """
        更新股票资产负债表数据，使用balancesheet_vip接口按公告日期更新
        mode: 'full' 全量更新（覆写），'increment' 增量更新
        fields: 指定字段列表，默认使用全部字段
        dates: 日期列表，用于按公告日期分批拉取
        """
        use_fields = fields if fields else self.columns
        assert dates is not None, "dates参数不能为空"
        
        if not dates:
            logger.warning("没有可更新的日期")
            return
        
        # 获取empty_dates和最近日历日期
        empty_dates = get_empty_dates_for_updater('StockInfoArchiver', 'stock_balancesheet')
        recent_calendar_dates = self._get_recent_calendar_dates(dates, 5)
        
        if mode == 'full':
            self.truncate_table(self.table_name)
            dates_to_update = dates
        else:
            # 获取已存在的公告日期
            with self.conn.cursor() as cursor:
                cursor.execute(f"SELECT DISTINCT ann_date FROM {self.table_name}")
                exist_dates = normalize_dates([row[0] for row in cursor.fetchall()])
            
            # 删除最近3个日历日的数据，确保数据及时性
            recent_3_calendar_dates = self._get_recent_calendar_dates(dates, 3)
            if recent_3_calendar_dates:
                recent_3_dates_str = "', '".join(recent_3_calendar_dates)
                delete_sql = f"DELETE FROM {self.table_name} WHERE ann_date IN ('{recent_3_dates_str}')"
                with self.conn.cursor() as cursor:
                    cursor.execute(delete_sql)
                self.conn.commit()
                logger.info(f"删除最近3个日历日的数据: {recent_3_calendar_dates}")
                
                # 从已存在日期中移除最近3个日历日
                exist_dates = exist_dates - set(recent_3_calendar_dates)
            
            # 过滤出需要更新的日期（排除已存在和empty的日期，但最近5个日历日不进入empty_dates）
            dates_to_update = []
            for date in dates:
                if date in exist_dates:
                    continue
                if date in empty_dates and date not in recent_calendar_dates:
                    continue
                dates_to_update.append(date)
        
        for ann_date in track(dates_to_update, description=f"stock_balancesheet {mode} updating"):
            try:
                # 使用balancesheet_vip接口按公告日期拉取数据
                df = self.pro.balancesheet_vip(ann_date=ann_date, fields=','.join(use_fields))
                
                # 更新empty_dates（最近5个日历日不进入empty_dates）
                if ann_date not in recent_calendar_dates:
                    update_empty_dates_after_fetch(
                        'StockInfoArchiver', 'stock_balancesheet', ann_date, 
                        df.empty, recent_calendar_dates
                    )
                
                if df.empty:
                    continue
                    
                # 转换日期字段
                df = convert_dates(df, [f for f in ['ann_date', 'f_ann_date', 'end_date'] if f in use_fields])
                if not df.empty:
                    df[use_fields] = safe_db_ready(df[use_fields], use_fields)
                    insert_sql = f"""
                    REPLACE INTO {self.table_name} ({', '.join(use_fields)}) VALUES ({', '.join(['%s']*len(use_fields))})
                    """
                    with self.conn.cursor() as cursor:
                        cursor.executemany(insert_sql, df[use_fields].values.tolist())
                    self.conn.commit()
                
            except Exception as e:
                logger.error(f"获取 {ann_date} 日期的资产负债表数据失败: {e}")
                continue

class Stock_ForecastUpdater(StockInfoBaseUpdater):
    """
    股票业绩预告数据更新器，使用forecast_vip接口按公告日期更新，支持全量/增量更新，字段与Tushare官方文档一致。
    https://tushare.pro/document/2?doc_id=45
    """
    def __init__(self):
        super().__init__()
        self.table_name = 'stock_forecast'
        # 官方文档所有字段
        self.columns = [
            'ts_code', 'ann_date', 'end_date', 'type', 'p_change_min', 'p_change_max',
            'net_profit_min', 'net_profit_max', 'last_parent_net', 'first_ann_date',
            'summary', 'change_reason'
        ]
        self.create_table(self._get_create_sql())

    def _get_create_sql(self):
        # 字段类型与接口文档保持一致，包含所有字段
        return f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            ts_code VARCHAR(20),
            ann_date VARCHAR(20),
            end_date VARCHAR(20),
            type VARCHAR(20),
            p_change_min FLOAT,
            p_change_max FLOAT,
            net_profit_min FLOAT,
            net_profit_max FLOAT,
            last_parent_net FLOAT,
            first_ann_date VARCHAR(20),
            summary TEXT,
            change_reason TEXT,
            PRIMARY KEY(ts_code, ann_date, end_date)
        ) DEFAULT CHARSET=utf8mb4;
        """

    def _get_recent_calendar_dates(self, reference_dates, days=5):
        """
        基于参考日期，获取最近N个日历日期
        
        Args:
            reference_dates: 参考日期列表
            days: 需要获取的日历日期数量
            
        Returns:
            最近N个日历日期列表
        """
        if not reference_dates:
            return []
        
        from datetime import datetime, timedelta
        
        try:
            # 获取最新的参考日期
            latest_date_str = max(reference_dates)
            if isinstance(latest_date_str, str) and len(latest_date_str) == 8:
                latest_date = datetime.strptime(latest_date_str, '%Y%m%d')
            else:
                return []
            
            # 生成最近N个日历日期
            calendar_dates = []
            for i in range(days):
                date = latest_date - timedelta(days=i)
                calendar_dates.append(date.strftime('%Y%m%d'))
            
            return sorted(calendar_dates, reverse=True)
            
        except Exception as e:
            logger.warning(f"获取最近日历日期失败: {e}")
            return []

    def update(self, mode='full', fields=None, dates=None):
        """
        更新股票业绩预告数据，使用forecast_vip接口按公告日期更新
        mode: 'full' 全量更新（覆写），'increment' 增量更新
        fields: 指定字段列表，默认使用全部字段
        dates: 日期列表，用于按公告日期分批拉取
        """
        use_fields = fields if fields else self.columns
        assert dates is not None, "dates参数不能为空"
        
        if not dates:
            logger.warning("没有可更新的日期")
            return
        
        # 获取empty_dates和最近日历日期
        empty_dates = get_empty_dates_for_updater('StockInfoArchiver', 'stock_forecast')
        recent_calendar_dates = self._get_recent_calendar_dates(dates, 5)
        
        if mode == 'full':
            self.truncate_table(self.table_name)
            dates_to_update = dates
        else:
            # 获取已存在的公告日期
            with self.conn.cursor() as cursor:
                cursor.execute(f"SELECT DISTINCT ann_date FROM {self.table_name}")
                exist_dates = normalize_dates([row[0] for row in cursor.fetchall()])
            
            # 删除最近3个日历日的数据，确保数据及时性
            recent_3_calendar_dates = self._get_recent_calendar_dates(dates, 3)
            if recent_3_calendar_dates:
                recent_3_dates_str = "', '".join(recent_3_calendar_dates)
                delete_sql = f"DELETE FROM {self.table_name} WHERE ann_date IN ('{recent_3_dates_str}')"
                with self.conn.cursor() as cursor:
                    cursor.execute(delete_sql)
                self.conn.commit()
                logger.info(f"删除最近3个日历日的数据: {recent_3_calendar_dates}")
                
                # 从已存在日期中移除最近3个日历日
                exist_dates = exist_dates - set(recent_3_calendar_dates)
            
            # 过滤出需要更新的日期（排除已存在和empty的日期，但最近5个日历日不进入empty_dates）
            dates_to_update = []
            for date in dates:
                if date in exist_dates:
                    continue
                if date in empty_dates and date not in recent_calendar_dates:
                    continue
                dates_to_update.append(date)
        
        for ann_date in track(dates_to_update, description=f"stock_forecast {mode} updating"):
            try:
                # 使用forecast_vip接口按公告日期拉取数据
                df = self.pro.forecast_vip(ann_date=ann_date, fields=','.join(use_fields))
                
                # 更新empty_dates（最近5个日历日不进入empty_dates）
                if ann_date not in recent_calendar_dates:
                    update_empty_dates_after_fetch(
                        'StockInfoArchiver', 'stock_forecast', ann_date, 
                        df.empty, recent_calendar_dates
                    )
                
                if df.empty:
                    continue
                    
                # 转换日期字段
                df = convert_dates(df, [f for f in ['ann_date', 'first_ann_date', 'end_date'] if f in use_fields])
                if not df.empty:
                    df[use_fields] = safe_db_ready(df[use_fields], use_fields)
                    insert_sql = f"""
                    REPLACE INTO {self.table_name} ({', '.join(use_fields)}) VALUES ({', '.join(['%s']*len(use_fields))})
                    """
                    with self.conn.cursor() as cursor:
                        cursor.executemany(insert_sql, df[use_fields].values.tolist())
                    self.conn.commit()
                
            except Exception as e:
                logger.error(f"获取 {ann_date} 日期的业绩预告数据失败: {e}")
                continue

class Stock_ExpressUpdater(StockInfoBaseUpdater):
    """
    股票业绩快报数据更新器，使用express_vip接口按公告日期更新，支持全量/增量更新，字段与Tushare官方文档一致。
    https://tushare.pro/document/2?doc_id=46
    """
    def __init__(self):
        super().__init__()
        self.table_name = 'stock_express'
        # 官方文档所有字段
        self.columns = [
            'ts_code', 'ann_date', 'end_date', 'revenue', 'operate_profit', 'total_profit', 'n_income',
            'total_assets', 'total_hldr_eqy_exc_min_int', 'diluted_eps', 'diluted_roe', 'yoy_net_profit',
            'bps', 'yoy_sales', 'yoy_op', 'yoy_tp', 'yoy_dedu_np', 'yoy_eps', 'yoy_roe', 'growth_assets',
            'yoy_equity', 'growth_bps', 'or_last_year', 'op_last_year', 'tp_last_year', 'np_last_year',
            'eps_last_year', 'open_net_assets', 'open_bps', 'perf_summary', 'is_audit', 'remark'
        ]
        self.create_table(self._get_create_sql())

    def _get_create_sql(self):
        # 字段类型与接口文档保持一致，包含所有字段
        return f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            ts_code VARCHAR(20),
            ann_date VARCHAR(20),
            end_date VARCHAR(20),
            revenue FLOAT,
            operate_profit FLOAT,
            total_profit FLOAT,
            n_income FLOAT,
            total_assets FLOAT,
            total_hldr_eqy_exc_min_int FLOAT,
            diluted_eps FLOAT,
            diluted_roe FLOAT,
            yoy_net_profit FLOAT,
            bps FLOAT,
            yoy_sales FLOAT,
            yoy_op FLOAT,
            yoy_tp FLOAT,
            yoy_dedu_np FLOAT,
            yoy_eps FLOAT,
            yoy_roe FLOAT,
            growth_assets FLOAT,
            yoy_equity FLOAT,
            growth_bps FLOAT,
            or_last_year FLOAT,
            op_last_year FLOAT,
            tp_last_year FLOAT,
            np_last_year FLOAT,
            eps_last_year FLOAT,
            open_net_assets FLOAT,
            open_bps FLOAT,
            perf_summary TEXT,
            is_audit INT,
            remark TEXT,
            PRIMARY KEY(ts_code, ann_date, end_date)
        ) DEFAULT CHARSET=utf8mb4;
        """

    def _get_recent_calendar_dates(self, reference_dates, days=5):
        """
        基于参考日期，获取最近N个日历日期
        
        Args:
            reference_dates: 参考日期列表
            days: 需要获取的日历日期数量
            
        Returns:
            最近N个日历日期列表
        """
        if not reference_dates:
            return []
        
        from datetime import datetime, timedelta
        
        try:
            # 获取最新的参考日期
            latest_date_str = max(reference_dates)
            if isinstance(latest_date_str, str) and len(latest_date_str) == 8:
                latest_date = datetime.strptime(latest_date_str, '%Y%m%d')
            else:
                return []
            
            # 生成最近N个日历日期
            calendar_dates = []
            for i in range(days):
                date = latest_date - timedelta(days=i)
                calendar_dates.append(date.strftime('%Y%m%d'))
            
            return sorted(calendar_dates, reverse=True)
            
        except Exception as e:
            logger.warning(f"获取最近日历日期失败: {e}")
            return []

    def update(self, mode='full', fields=None, dates=None):
        """
        更新股票业绩快报数据，使用express_vip接口按公告日期更新
        mode: 'full' 全量更新（覆写），'increment' 增量更新
        fields: 指定字段列表，默认使用全部字段
        dates: 日期列表，用于按公告日期分批拉取
        """
        use_fields = fields if fields else self.columns
        assert dates is not None, "dates参数不能为空"
        
        if not dates:
            logger.warning("没有可更新的日期")
            return
        
        # 获取empty_dates和最近日历日期
        empty_dates = get_empty_dates_for_updater('StockInfoArchiver', 'stock_express')
        recent_calendar_dates = self._get_recent_calendar_dates(dates, 5)
        
        if mode == 'full':
            self.truncate_table(self.table_name)
            dates_to_update = dates
        else:
            # 获取已存在的公告日期
            with self.conn.cursor() as cursor:
                cursor.execute(f"SELECT DISTINCT ann_date FROM {self.table_name}")
                exist_dates = normalize_dates([row[0] for row in cursor.fetchall()])
            
            # 删除最近3个日历日的数据，确保数据及时性
            recent_3_calendar_dates = self._get_recent_calendar_dates(dates, 3)
            if recent_3_calendar_dates:
                recent_3_dates_str = "', '".join(recent_3_calendar_dates)
                delete_sql = f"DELETE FROM {self.table_name} WHERE ann_date IN ('{recent_3_dates_str}')"
                with self.conn.cursor() as cursor:
                    cursor.execute(delete_sql)
                self.conn.commit()
                logger.info(f"删除最近3个日历日的数据: {recent_3_calendar_dates}")
                
                # 从已存在日期中移除最近3个日历日
                exist_dates = exist_dates - set(recent_3_calendar_dates)
            
            # 过滤出需要更新的日期（排除已存在和empty的日期，但最近5个日历日不进入empty_dates）
            dates_to_update = []
            for date in dates:
                if date in exist_dates:
                    continue
                if date in empty_dates and date not in recent_calendar_dates:
                    continue
                dates_to_update.append(date)
        
        for ann_date in track(dates_to_update, description=f"stock_express {mode} updating"):
            try:
                # 使用express_vip接口按公告日期拉取数据
                df = self.pro.express_vip(ann_date=ann_date, fields=','.join(use_fields))
                
                # 更新empty_dates（最近5个日历日不进入empty_dates）
                if ann_date not in recent_calendar_dates:
                    update_empty_dates_after_fetch(
                        'StockInfoArchiver', 'stock_express', ann_date, 
                        df.empty, recent_calendar_dates
                    )
                
                if df.empty:
                    continue
                    
                # 转换日期字段
                df = convert_dates(df, [f for f in ['ann_date', 'end_date'] if f in use_fields])
                if not df.empty:
                    df[use_fields] = safe_db_ready(df[use_fields], use_fields)
                    insert_sql = f"""
                    REPLACE INTO {self.table_name} ({', '.join(use_fields)}) VALUES ({', '.join(['%s']*len(use_fields))})
                    """
                    with self.conn.cursor() as cursor:
                        cursor.executemany(insert_sql, df[use_fields].values.tolist())
                    self.conn.commit()
                
            except Exception as e:
                logger.error(f"获取 {ann_date} 日期的业绩快报数据失败: {e}")
                continue

class Stock_FinaIndicatorUpdater(StockInfoBaseUpdater):
    """
    股票财务指标数据更新器，使用fina_indicator_vip接口按公告日期更新，支持全量/增量更新，字段与Tushare官方文档一致。
    https://tushare.pro/document/2?doc_id=79
    """
    def __init__(self):
        super().__init__()
        self.table_name = 'stock_fina_indicator'
        # 官方文档所有字段（按照文档顺序）
        self.columns = [
            'ts_code', 'ann_date', 'end_date', 'eps', 'dt_eps', 'total_revenue_ps', 'revenue_ps',
            'capital_rese_ps', 'surplus_rese_ps', 'undist_profit_ps', 'extra_item', 'profit_dedt',
            'gross_margin', 'current_ratio', 'quick_ratio', 'cash_ratio', 'invturn_days',
            'arturn_days', 'inv_turn', 'ar_turn', 'ca_turn', 'fa_turn', 'assets_turn',
            'op_income', 'valuechange_income', 'interst_income', 'daa', 'ebit', 'ebitda',
            'fcff', 'fcfe', 'current_exint', 'noncurrent_exint', 'interestdebt', 'netdebt',
            'tangible_asset', 'working_capital', 'networking_capital', 'invest_capital',
            'retained_earnings', 'diluted2_eps', 'bps', 'ocfps', 'retainedps', 'cfps',
            'ebit_ps', 'fcff_ps', 'fcfe_ps', 'netprofit_margin', 'grossprofit_margin',
            'cogs_of_sales', 'expense_of_sales', 'profit_to_gr', 'saleexp_to_gr',
            'adminexp_of_gr', 'finaexp_of_gr', 'impai_ttm', 'gc_of_gr', 'op_of_gr',
            'ebit_of_gr', 'roe', 'roe_waa', 'roe_dt', 'roa', 'npta', 'roic', 'roe_yearly',
            'roa2_yearly', 'roe_avg', 'opincome_of_ebt', 'investincome_of_ebt',
            'n_op_profit_of_ebt', 'tax_to_ebt', 'dtprofit_to_profit', 'salescash_to_or',
            'ocf_to_or', 'ocf_to_opincome', 'capitalized_to_da', 'debt_to_assets',
            'assets_to_eqt', 'dp_assets_to_eqt', 'ca_to_assets', 'nca_to_assets',
            'tbassets_to_totalassets', 'int_to_talcap', 'eqt_to_talcapital', 'currentdebt_to_debt',
            'longdeb_to_debt', 'ocf_to_shortdebt', 'debt_to_eqt', 'eqt_to_debt',
            'eqt_to_interestdebt', 'tangibleasset_to_debt', 'tangasset_to_intdebt',
            'tangibleasset_to_netdebt', 'ocf_to_debt', 'ocf_to_interestdebt', 'ocf_to_netdebt',
            'ebit_to_interest', 'longdebt_to_workingcapital', 'ebitda_to_debt',
            'turn_days', 'roa_yearly', 'roa_dp', 'fixed_assets', 'profit_prefin_exp',
            'non_op_profit', 'op_to_ebt', 'nop_to_ebt', 'ocf_to_profit', 'cash_to_liqdebt',
            'cash_to_liqdebt_withinterest', 'op_to_liqdebt', 'op_to_debt', 'roic_yearly',
            'total_fa_trun', 'profit_to_op', 'q_opincome', 'q_investincome', 'q_dtprofit',
            'q_eps', 'q_netprofit_margin', 'q_gsprofit_margin', 'q_exp_to_sales',
            'q_profit_to_gr', 'q_saleexp_to_gr', 'q_adminexp_to_gr', 'q_finaexp_to_gr',
            'q_impair_to_gr_ttm', 'q_gc_to_gr', 'q_op_to_gr', 'q_roe', 'q_dt_roe',
            'q_npta', 'q_opincome_to_ebt', 'q_investincome_to_ebt', 'q_dtprofit_to_profit',
            'q_salescash_to_or', 'q_ocf_to_sales', 'q_ocf_to_or', 'basic_eps_yoy',
            'dt_eps_yoy', 'cfps_yoy', 'op_yoy', 'ebt_yoy', 'netprofit_yoy', 'dt_netprofit_yoy',
            'ocf_yoy', 'roe_yoy', 'bps_yoy', 'assets_yoy', 'eqt_yoy', 'tr_yoy', 'or_yoy',
            'q_gr_yoy', 'q_gr_qoq', 'q_sales_yoy', 'q_sales_qoq', 'q_op_yoy', 'q_op_qoq',
            'q_profit_yoy', 'q_profit_qoq', 'q_netprofit_yoy', 'q_netprofit_qoq',
            'equity_yoy', 'rd_exp', 'update_flag'
        ]
        self.create_table(self._get_create_sql())

    def _get_create_sql(self):
        # 字段类型与接口文档保持一致，包含所有字段
        return f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            ts_code VARCHAR(20),
            ann_date VARCHAR(20),
            end_date VARCHAR(20),
            eps FLOAT,
            dt_eps FLOAT,
            total_revenue_ps FLOAT,
            revenue_ps FLOAT,
            capital_rese_ps FLOAT,
            surplus_rese_ps FLOAT,
            undist_profit_ps FLOAT,
            extra_item FLOAT,
            profit_dedt FLOAT,
            gross_margin FLOAT,
            current_ratio FLOAT,
            quick_ratio FLOAT,
            cash_ratio FLOAT,
            invturn_days FLOAT,
            arturn_days FLOAT,
            inv_turn FLOAT,
            ar_turn FLOAT,
            ca_turn FLOAT,
            fa_turn FLOAT,
            assets_turn FLOAT,
            op_income FLOAT,
            valuechange_income FLOAT,
            interst_income FLOAT,
            daa FLOAT,
            ebit FLOAT,
            ebitda FLOAT,
            fcff FLOAT,
            fcfe FLOAT,
            current_exint FLOAT,
            noncurrent_exint FLOAT,
            interestdebt FLOAT,
            netdebt FLOAT,
            tangible_asset FLOAT,
            working_capital FLOAT,
            networking_capital FLOAT,
            invest_capital FLOAT,
            retained_earnings FLOAT,
            diluted2_eps FLOAT,
            bps FLOAT,
            ocfps FLOAT,
            retainedps FLOAT,
            cfps FLOAT,
            ebit_ps FLOAT,
            fcff_ps FLOAT,
            fcfe_ps FLOAT,
            netprofit_margin FLOAT,
            grossprofit_margin FLOAT,
            cogs_of_sales FLOAT,
            expense_of_sales FLOAT,
            profit_to_gr FLOAT,
            saleexp_to_gr FLOAT,
            adminexp_of_gr FLOAT,
            finaexp_of_gr FLOAT,
            impai_ttm FLOAT,
            gc_of_gr FLOAT,
            op_of_gr FLOAT,
            ebit_of_gr FLOAT,
            roe FLOAT,
            roe_waa FLOAT,
            roe_dt FLOAT,
            roa FLOAT,
            npta FLOAT,
            roic FLOAT,
            roe_yearly FLOAT,
            roa_yearly FLOAT,
            roe_avg FLOAT,
            opincome_of_ebt FLOAT,
            investincome_of_ebt FLOAT,
            n_op_profit_of_ebt FLOAT,
            tax_to_ebt FLOAT,
            dtprofit_to_profit FLOAT,
            salescash_to_or FLOAT,
            ocf_to_or FLOAT,
            ocf_to_opincome FLOAT,
            capitalized_to_da FLOAT,
            debt_to_assets FLOAT,
            assets_to_eqt FLOAT,
            dp_assets_to_eqt FLOAT,
            ca_to_assets FLOAT,
            nca_to_assets FLOAT,
            tbassets_to_totalassets FLOAT,
            int_to_talcap FLOAT,
            eqt_to_talcapital FLOAT,
            currentdebt_to_debt FLOAT,
            longdeb_to_debt FLOAT,
            ocf_to_shortdebt FLOAT,
            debt_to_eqt FLOAT,
            eqt_to_debt FLOAT,
            eqt_to_interestdebt FLOAT,
            tangibleasset_to_debt FLOAT,
            tangasset_to_intdebt FLOAT,
            tangibleasset_to_netdebt FLOAT,
            ocf_to_debt FLOAT,
            ocf_to_interestdebt FLOAT,
            ocf_to_netdebt FLOAT,
            ebit_to_interest FLOAT,
            longdebt_to_workingcapital FLOAT,
            ebitda_to_debt FLOAT,
            turn_days FLOAT,
            roa2_yearly FLOAT,
            roa_dp FLOAT,
            fixed_assets FLOAT,
            profit_prefin_exp FLOAT,
            non_op_profit FLOAT,
            op_to_ebt FLOAT,
            nop_to_ebt FLOAT,
            ocf_to_profit FLOAT,
            cash_to_liqdebt FLOAT,
            cash_to_liqdebt_withinterest FLOAT,
            op_to_liqdebt FLOAT,
            op_to_debt FLOAT,
            roic_yearly FLOAT,
            total_fa_trun FLOAT,
            profit_to_op FLOAT,
            q_opincome FLOAT,
            q_investincome FLOAT,
            q_dtprofit FLOAT,
            q_eps FLOAT,
            q_netprofit_margin FLOAT,
            q_gsprofit_margin FLOAT,
            q_exp_to_sales FLOAT,
            q_profit_to_gr FLOAT,
            q_saleexp_to_gr FLOAT,
            q_adminexp_to_gr FLOAT,
            q_finaexp_to_gr FLOAT,
            q_impair_to_gr_ttm FLOAT,
            q_gc_to_gr FLOAT,
            q_op_to_gr FLOAT,
            q_roe FLOAT,
            q_dt_roe FLOAT,
            q_npta FLOAT,
            q_opincome_to_ebt FLOAT,
            q_investincome_to_ebt FLOAT,
            q_dtprofit_to_profit FLOAT,
            q_salescash_to_or FLOAT,
            q_ocf_to_sales FLOAT,
            q_ocf_to_or FLOAT,
            basic_eps_yoy FLOAT,
            dt_eps_yoy FLOAT,
            cfps_yoy FLOAT,
            op_yoy FLOAT,
            ebt_yoy FLOAT,
            netprofit_yoy FLOAT,
            dt_netprofit_yoy FLOAT,
            ocf_yoy FLOAT,
            roe_yoy FLOAT,
            bps_yoy FLOAT,
            assets_yoy FLOAT,
            eqt_yoy FLOAT,
            tr_yoy FLOAT,
            or_yoy FLOAT,
            q_gr_yoy FLOAT,
            q_gr_qoq FLOAT,
            q_sales_yoy FLOAT,
            q_sales_qoq FLOAT,
            q_op_yoy FLOAT,
            q_op_qoq FLOAT,
            q_profit_yoy FLOAT,
            q_profit_qoq FLOAT,
            q_netprofit_yoy FLOAT,
            q_netprofit_qoq FLOAT,
            equity_yoy FLOAT,
            rd_exp FLOAT,
            update_flag VARCHAR(20),
            PRIMARY KEY(ts_code, ann_date, end_date)
        ) DEFAULT CHARSET=utf8mb4;
        """

    def _get_recent_calendar_dates(self, reference_dates, days=5):
        """
        基于参考日期，获取最近N个日历日期
        
        Args:
            reference_dates: 参考日期列表
            days: 需要获取的日历日期数量
            
        Returns:
            最近N个日历日期列表
        """
        if not reference_dates:
            return []
        
        from datetime import datetime, timedelta
        
        try:
            # 获取最新的参考日期
            latest_date_str = max(reference_dates)
            if isinstance(latest_date_str, str) and len(latest_date_str) == 8:
                latest_date = datetime.strptime(latest_date_str, '%Y%m%d')
            else:
                return []
            
            # 生成最近N个日历日期
            calendar_dates = []
            for i in range(days):
                date = latest_date - timedelta(days=i)
                calendar_dates.append(date.strftime('%Y%m%d'))
            
            return sorted(calendar_dates, reverse=True)
            
        except Exception as e:
            logger.warning(f"获取最近日历日期失败: {e}")
            return []

    def update(self, mode='full', fields=None, dates=None):
        """
        更新股票财务指标数据，使用fina_indicator_vip接口按公告日期更新
        mode: 'full' 全量更新（覆写），'increment' 增量更新
        fields: 指定字段列表，默认使用全部字段
        dates: 日期列表，用于按公告日期分批拉取
        """
        use_fields = fields if fields else self.columns
        assert dates is not None, "dates参数不能为空"
        
        if not dates:
            logger.warning("没有可更新的日期")
            return
        
        # 获取empty_dates和最近日历日期
        empty_dates = get_empty_dates_for_updater('StockInfoArchiver', 'stock_fina_indicator')
        recent_calendar_dates = self._get_recent_calendar_dates(dates, 5)
        
        if mode == 'full':
            self.truncate_table(self.table_name)
            dates_to_update = dates
        else:
            # 获取已存在的公告日期
            with self.conn.cursor() as cursor:
                cursor.execute(f"SELECT DISTINCT ann_date FROM {self.table_name}")
                exist_dates = normalize_dates([row[0] for row in cursor.fetchall()])
            
            # 删除最近3个日历日的数据，确保数据及时性
            recent_3_calendar_dates = self._get_recent_calendar_dates(dates, 3)
            if recent_3_calendar_dates:
                recent_3_dates_str = "', '".join(recent_3_calendar_dates)
                delete_sql = f"DELETE FROM {self.table_name} WHERE ann_date IN ('{recent_3_dates_str}')"
                with self.conn.cursor() as cursor:
                    cursor.execute(delete_sql)
                self.conn.commit()
                logger.info(f"删除最近3个日历日的数据: {recent_3_calendar_dates}")
                
                # 从已存在日期中移除最近3个日历日
                exist_dates = exist_dates - set(recent_3_calendar_dates)
            
            # 过滤出需要更新的日期（排除已存在和empty的日期，但最近5个日历日不进入empty_dates）
            dates_to_update = []
            for date in dates:
                if date in exist_dates:
                    continue
                if date in empty_dates and date not in recent_calendar_dates:
                    continue
                dates_to_update.append(date)
        
        for ann_date in track(dates_to_update, description=f"stock_fina_indicator {mode} updating"):
            try:
                # 使用fina_indicator_vip接口按公告日期拉取数据
                df = self.pro.fina_indicator_vip(ann_date=ann_date, fields=','.join(use_fields))
                
                # 更新empty_dates（最近5个日历日不进入empty_dates）
                if ann_date not in recent_calendar_dates:
                    update_empty_dates_after_fetch(
                        'StockInfoArchiver', 'stock_fina_indicator', ann_date, 
                        df.empty, recent_calendar_dates
                    )
                
                if df.empty:
                    continue
                    
                # 转换日期字段
                df = convert_dates(df, [f for f in ['ann_date', 'end_date'] if f in use_fields])
                if not df.empty:
                    df[use_fields] = safe_db_ready(df[use_fields], use_fields)
                    insert_sql = f"""
                    REPLACE INTO {self.table_name} ({', '.join(use_fields)}) VALUES ({', '.join(['%s']*len(use_fields))})
                    """
                    with self.conn.cursor() as cursor:
                        cursor.executemany(insert_sql, df[use_fields].values.tolist())
                    self.conn.commit()
                
            except Exception as e:
                logger.error(f"获取 {ann_date} 日期的财务指标数据失败: {e}")
                continue

class Stock_FinaMainbzUpdater(StockInfoBaseUpdater):
    """
    股票主营业务构成数据更新器，使用fina_mainbz_vip接口按报告期更新，支持全量/增量更新，字段与Tushare官方文档一致。
    https://tushare.pro/document/2?doc_id=81
    """
    def __init__(self):
        super().__init__()
        self.table_name = 'stock_fina_mainbz'
        # 官方文档所有字段
        self.columns = [
            'ts_code', 'end_date', 'bz_item', 'bz_sales', 'bz_profit', 'bz_cost', 'curr_type', 'update_flag'
        ]
        self.create_table(self._get_create_sql())

    def _get_create_sql(self):
        # 字段类型与接口文档保持一致，包含所有字段
        return f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            ts_code VARCHAR(20),
            end_date VARCHAR(20),
            bz_item VARCHAR(200),
            bz_sales FLOAT,
            bz_profit FLOAT,
            bz_cost FLOAT,
            curr_type VARCHAR(10),
            update_flag VARCHAR(20),
            PRIMARY KEY(ts_code, end_date, bz_item)
        ) DEFAULT CHARSET=utf8mb4;
        """

    def _get_recent_periods(self, reference_periods, count=2):
        """
        基于参考报告期，获取最近N个报告期
        
        Args:
            reference_periods: 参考报告期列表 (格式: YYYYMMDD)
            count: 需要获取的报告期数量
            
        Returns:
            最近N个报告期列表
        """
        if not reference_periods:
            return []
        
        try:
            # 筛选出标准报告期（季度末日期）
            standard_periods = []
            for period in reference_periods:
                if isinstance(period, str) and len(period) == 8:
                    month_day = period[4:]
                    if month_day in ['0331', '0630', '0930', '1231']:
                        standard_periods.append(period)
            
            # 排序并取最近N个
            standard_periods.sort(reverse=True)
            return standard_periods[:count]
            
        except Exception as e:
            logger.warning(f"获取最近报告期失败: {e}")
            return []

    def update(self, mode='full', fields=None, periods=None):
        """
        更新股票主营业务构成数据，使用fina_mainbz_vip接口按报告期更新
        mode: 'full' 全量更新（覆写），'increment' 增量更新
        fields: 指定字段列表，默认使用全部字段
        periods: 报告期列表，用于按报告期分批拉取
        """
        use_fields = fields if fields else self.columns
        assert periods is not None, "periods参数不能为空"
        
        if not periods:
            logger.warning("没有可更新的报告期")
            return
        
        # 筛选出标准报告期（季度末）
        standard_periods = []
        for period in periods:
            if isinstance(period, str) and len(period) == 8:
                month_day = period[4:]
                if month_day in ['0331', '0630', '0930', '1231']:
                    standard_periods.append(period)
        
        if not standard_periods:
            logger.warning("没有有效的报告期数据")
            return
        
        # 获取empty_periods和最近报告期
        empty_periods = get_empty_dates_for_updater('StockInfoArchiver', 'stock_fina_mainbz')
        recent_periods = self._get_recent_periods(standard_periods, 2)
        
        if mode == 'full':
            self.truncate_table(self.table_name)
            periods_to_update = standard_periods
        else:
            # 获取已存在的报告期
            with self.conn.cursor() as cursor:
                cursor.execute(f"SELECT DISTINCT end_date FROM {self.table_name}")
                exist_periods = normalize_dates([row[0] for row in cursor.fetchall()])
            
            # 删除最近2个报告期的数据，确保数据及时性
            if recent_periods:
                recent_periods_str = "', '".join(recent_periods)
                delete_sql = f"DELETE FROM {self.table_name} WHERE end_date IN ('{recent_periods_str}')"
                with self.conn.cursor() as cursor:
                    cursor.execute(delete_sql)
                self.conn.commit()
                logger.info(f"删除最近2个报告期的数据: {recent_periods}")
                
                # 从已存在报告期中移除最近2个报告期
                exist_periods = exist_periods - set(recent_periods)
            
            # 过滤出需要更新的报告期（排除已存在和empty的报告期，但最近2个报告期不进入empty_periods）
            periods_to_update = []
            for period in standard_periods:
                if period in exist_periods:
                    continue
                if period in empty_periods and period not in recent_periods:
                    continue
                periods_to_update.append(period)
        
        for period in track(periods_to_update, description=f"stock_fina_mainbz {mode} updating"):
            try:
                # 按产品和地区两种类型分别获取数据
                all_data = []
                
                for bz_type in ['P', 'D']:
                    try:
                        df = self.pro.fina_mainbz_vip(period=period, type=bz_type, fields=','.join(use_fields))
                        if not df.empty:
                            all_data.append(df)
                    except Exception as e:
                        logger.warning(f"获取 {period} 报告期 {bz_type} 类型的主营业务构成数据失败: {e}")
                        continue
                
                # 更新empty_periods（最近2个报告期不进入empty_periods）
                data_found = len(all_data) > 0
                if period not in recent_periods:
                    update_empty_dates_after_fetch(
                        'StockInfoArchiver', 'stock_fina_mainbz', period, 
                        not data_found, recent_periods
                    )
                
                if not data_found:
                    continue
                
                # 合并所有数据
                combined_df = pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()
                
                if not combined_df.empty:
                    # 转换日期字段
                    combined_df = convert_dates(combined_df, [f for f in ['end_date'] if f in use_fields])
                    combined_df[use_fields] = safe_db_ready(combined_df[use_fields], use_fields)
                    insert_sql = f"""
                    REPLACE INTO {self.table_name} ({', '.join(use_fields)}) VALUES ({', '.join(['%s']*len(use_fields))})
                    """
                    with self.conn.cursor() as cursor:
                        cursor.executemany(insert_sql, combined_df[use_fields].values.tolist())
                    self.conn.commit()
                
            except Exception as e:
                logger.error(f"获取 {period} 报告期的主营业务构成数据失败: {e}")
                continue

class Stock_DividendUpdater(StockInfoBaseUpdater):
    """
    股票分红送股数据更新器，使用dividend接口按公告日期更新，支持全量/增量更新，字段与Tushare官方文档一致。
    https://tushare.pro/document/2?doc_id=103
    """
    def __init__(self):
        super().__init__()
        self.table_name = 'stock_dividend'
        # 官方文档所有字段
        self.columns = [
            'ts_code', 'end_date', 'ann_date', 'div_proc', 'stk_div', 'stk_bo_rate', 'stk_co_rate',
            'cash_div', 'cash_div_tax', 'record_date', 'ex_date', 'pay_date', 'div_listdate',
            'imp_ann_date', 'base_date', 'base_share'
        ]
        self.create_table(self._get_create_sql())

    def _get_create_sql(self):
        # 字段类型与接口文档保持一致，包含所有字段
        return f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            ts_code VARCHAR(20),
            end_date VARCHAR(20),
            ann_date VARCHAR(20),
            div_proc VARCHAR(20),
            stk_div FLOAT,
            stk_bo_rate FLOAT,
            stk_co_rate FLOAT,
            cash_div FLOAT,
            cash_div_tax FLOAT,
            record_date VARCHAR(20),
            ex_date VARCHAR(20),
            pay_date VARCHAR(20),
            div_listdate VARCHAR(20),
            imp_ann_date VARCHAR(20),
            base_date VARCHAR(20),
            base_share FLOAT,
            PRIMARY KEY(ts_code, end_date, ann_date)
        ) DEFAULT CHARSET=utf8mb4;
        """

    def _get_recent_calendar_dates(self, reference_dates, days=5):
        """
        基于参考日期，获取最近N个日历日期
        
        Args:
            reference_dates: 参考日期列表
            days: 需要获取的日历日期数量
            
        Returns:
            最近N个日历日期列表
        """
        if not reference_dates:
            return []
        
        from datetime import datetime, timedelta
        
        try:
            # 获取最新的参考日期
            latest_date_str = max(reference_dates)
            if isinstance(latest_date_str, str) and len(latest_date_str) == 8:
                latest_date = datetime.strptime(latest_date_str, '%Y%m%d')
            else:
                return []
            
            # 生成最近N个日历日期
            calendar_dates = []
            for i in range(days):
                date = latest_date - timedelta(days=i)
                calendar_dates.append(date.strftime('%Y%m%d'))
            
            return sorted(calendar_dates, reverse=True)
            
        except Exception as e:
            logger.warning(f"获取最近日历日期失败: {e}")
            return []

    def update(self, mode='full', fields=None, dates=None):
        """
        更新股票分红送股数据，使用dividend接口按公告日期更新
        mode: 'full' 全量更新（覆写），'increment' 增量更新
        fields: 指定字段列表，默认使用全部字段
        dates: 日期列表，用于按公告日期分批拉取
        """
        use_fields = fields if fields else self.columns
        assert dates is not None, "dates参数不能为空"
        
        if not dates:
            logger.warning("没有可更新的日期")
            return
        
        # 获取empty_dates和最近日历日期
        empty_dates = get_empty_dates_for_updater('StockInfoArchiver', 'stock_dividend')
        recent_calendar_dates = self._get_recent_calendar_dates(dates, 5)
        
        if mode == 'full':
            self.truncate_table(self.table_name)
            dates_to_update = dates
        else:
            # 获取已存在的公告日期
            with self.conn.cursor() as cursor:
                cursor.execute(f"SELECT DISTINCT ann_date FROM {self.table_name}")
                exist_dates = normalize_dates([row[0] for row in cursor.fetchall()])
            
            # 删除最近3个日历日的数据，确保数据及时性
            recent_3_calendar_dates = self._get_recent_calendar_dates(dates, 3)
            if recent_3_calendar_dates:
                recent_3_dates_str = "', '".join(recent_3_calendar_dates)
                delete_sql = f"DELETE FROM {self.table_name} WHERE ann_date IN ('{recent_3_dates_str}')"
                with self.conn.cursor() as cursor:
                    cursor.execute(delete_sql)
                self.conn.commit()
                logger.info(f"删除最近3个日历日的数据: {recent_3_calendar_dates}")
                
                # 从已存在日期中移除最近3个日历日
                exist_dates = exist_dates - set(recent_3_calendar_dates)
            
            # 过滤出需要更新的日期（排除已存在和empty的日期，但最近5个日历日不进入empty_dates）
            dates_to_update = []
            for date in dates:
                if date in exist_dates:
                    continue
                if date in empty_dates and date not in recent_calendar_dates:
                    continue
                dates_to_update.append(date)
        
        for ann_date in track(dates_to_update, description=f"stock_dividend {mode} updating"):
            try:
                # 使用dividend接口按公告日期拉取数据
                df = self.pro.dividend(ann_date=ann_date, fields=','.join(use_fields))
                
                # 更新empty_dates（最近5个日历日不进入empty_dates）
                if ann_date not in recent_calendar_dates:
                    update_empty_dates_after_fetch(
                        'StockInfoArchiver', 'stock_dividend', ann_date, 
                        df.empty, recent_calendar_dates
                    )
                
                if df.empty:
                    continue
                    
                # 转换日期字段
                date_fields = [f for f in ['ann_date', 'end_date', 'record_date', 'ex_date', 'pay_date', 'div_listdate', 'imp_ann_date', 'base_date'] if f in use_fields]
                df = convert_dates(df, date_fields)
                if not df.empty:
                    df[use_fields] = safe_db_ready(df[use_fields], use_fields)
                    insert_sql = f"""
                    REPLACE INTO {self.table_name} ({', '.join(use_fields)}) VALUES ({', '.join(['%s']*len(use_fields))})
                    """
                    with self.conn.cursor() as cursor:
                        cursor.executemany(insert_sql, df[use_fields].values.tolist())
                    self.conn.commit()
                
            except Exception as e:
                logger.error(f"获取 {ann_date} 日期的分红送股数据失败: {e}")
                continue

class Stock_BlockTradeUpdater(StockInfoBaseUpdater):
    """
    股票大宗交易数据更新器，使用block_trade接口按交易日期更新，支持全量/增量更新，字段与Tushare官方文档一致。
    https://tushare.pro/document/2?doc_id=161
    """
    def __init__(self):
        super().__init__()
        self.table_name = 'stock_block_trade'
        # 官方文档所有字段
        self.columns = [
            'ts_code', 'trade_date', 'price', 'vol', 'amount', 'buyer', 'seller'
        ]
        self.create_table(self._get_create_sql())

    def _get_create_sql(self):
        # 字段类型与接口文档保持一致，包含所有字段
        return f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            ts_code VARCHAR(20),
            trade_date VARCHAR(20),
            price FLOAT,
            vol FLOAT,
            amount FLOAT,
            buyer VARCHAR(500),
            seller VARCHAR(500),
            PRIMARY KEY(ts_code, trade_date, price, vol)
        ) DEFAULT CHARSET=utf8mb4;
        """

    def update(self, mode='full', fields=None, trade_dates=None):
        """
        更新股票大宗交易数据，使用block_trade接口按交易日期更新
        mode: 'full' 全量更新（覆写），'increment' 增量更新
        fields: 指定字段列表，默认使用全部字段
        trade_dates: 交易日期列表，用于按交易日期分批拉取
        """
        use_fields = fields if fields else self.columns
        if trade_dates is None:
            raise ValueError('Stock_BlockTradeUpdater.update: trade_dates参数不能为空，必须分批拉取！')
        
        # 获取empty_dates和最近交易日
        empty_dates = get_empty_dates_for_updater('StockInfoArchiver', 'stock_block_trade')
        recent_trade_dates = get_recent_trade_dates(trade_dates, 5)
        
        if mode == 'full':
            self.truncate_table(self.table_name)
            dates_to_update = trade_dates
        else:
            # 获取已存在的交易日期
            with self.conn.cursor() as cursor:
                cursor.execute(f"SELECT DISTINCT trade_date FROM {self.table_name}")
                exist_dates = normalize_dates([row[0] for row in cursor.fetchall()])
            
            # 过滤出需要更新的日期（排除已存在和empty的日期）
            dates_to_update = filter_dates_for_update(
                trade_dates, exist_dates, empty_dates, 
                'StockInfoArchiver', 'stock_block_trade', recent_trade_dates
            )
        
        for trade_date in track(dates_to_update, description=f"stock_block_trade {mode} updating"):
            try:
                # 使用block_trade接口按交易日期拉取数据
                df = self.pro.block_trade(trade_date=trade_date, fields=','.join(use_fields))
                
                # 更新empty_dates
                update_empty_dates_after_fetch(
                    'StockInfoArchiver', 'stock_block_trade', trade_date, 
                    df.empty, recent_trade_dates
                )
                
                if df.empty:
                    continue
                    
                # 转换日期字段
                df = convert_dates(df, [f for f in ['trade_date'] if f in use_fields])
                if not df.empty:
                    df[use_fields] = safe_db_ready(df[use_fields], use_fields)
                    insert_sql = f"""
                    REPLACE INTO {self.table_name} ({', '.join(use_fields)}) VALUES ({', '.join(['%s']*len(use_fields))})
                    """
                    with self.conn.cursor() as cursor:
                        cursor.executemany(insert_sql, df[use_fields].values.tolist())
                    self.conn.commit()
                
            except Exception as e:
                logger.error(f"获取 {trade_date} 日期的大宗交易数据失败: {e}")
                continue

class Stock_MarginUpdater(StockInfoBaseUpdater):
    """
    融资融券交易汇总数据更新器，使用margin接口按交易日期更新，支持全量/增量更新，字段与Tushare官方文档一致。
    https://tushare.pro/document/2?doc_id=58
    """
    def __init__(self):
        super().__init__()
        self.table_name = 'stock_margin'
        # 官方文档所有字段
        self.columns = [
            'trade_date', 'exchange_id', 'rzye', 'rzmre', 'rzche', 'rqye', 'rqmcl', 'rzrqye', 'rqyl'
        ]
        self.create_table(self._get_create_sql())

    def _get_create_sql(self):
        # 字段类型与接口文档保持一致，包含所有字段
        return f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            trade_date VARCHAR(20),
            exchange_id VARCHAR(10),
            rzye FLOAT,
            rzmre FLOAT,
            rzche FLOAT,
            rqye FLOAT,
            rqmcl FLOAT,
            rzrqye FLOAT,
            rqyl FLOAT,
            PRIMARY KEY(trade_date, exchange_id)
        ) DEFAULT CHARSET=utf8mb4;
        """

    def update(self, mode='full', fields=None, trade_dates=None):
        """
        更新融资融券交易汇总数据，使用margin接口按交易日期更新
        mode: 'full' 全量更新（覆写），'increment' 增量更新
        fields: 指定字段列表，默认使用全部字段
        trade_dates: 交易日期列表，用于按交易日期分批拉取
        """
        use_fields = fields if fields else self.columns
        if trade_dates is None:
            raise ValueError('Stock_MarginUpdater.update: trade_dates参数不能为空，必须分批拉取！')
        
        # 获取empty_dates和最近交易日
        empty_dates = get_empty_dates_for_updater('StockInfoArchiver', 'stock_margin')
        recent_trade_dates = get_recent_trade_dates(trade_dates, 5)
        
        if mode == 'full':
            self.truncate_table(self.table_name)
            dates_to_update = trade_dates
        else:
            # 获取已存在的交易日期
            with self.conn.cursor() as cursor:
                cursor.execute(f"SELECT DISTINCT trade_date FROM {self.table_name}")
                exist_dates = normalize_dates([row[0] for row in cursor.fetchall()])
            
            # 过滤出需要更新的日期（排除已存在和empty的日期）
            dates_to_update = filter_dates_for_update(
                trade_dates, exist_dates, empty_dates, 
                'StockInfoArchiver', 'stock_margin', recent_trade_dates
            )
        
        for trade_date in track(dates_to_update, description=f"stock_margin {mode} updating"):
            try:
                # 使用margin接口按交易日期拉取数据
                df = self.pro.margin(trade_date=trade_date, fields=','.join(use_fields))
                
                # 更新empty_dates
                update_empty_dates_after_fetch(
                    'StockInfoArchiver', 'stock_margin', trade_date, 
                    df.empty, recent_trade_dates
                )
                
                if df.empty:
                    continue
                    
                # 转换日期字段
                df = convert_dates(df, [f for f in ['trade_date'] if f in use_fields])
                if not df.empty:
                    df[use_fields] = safe_db_ready(df[use_fields], use_fields)
                    insert_sql = f"""
                    REPLACE INTO {self.table_name} ({', '.join(use_fields)}) VALUES ({', '.join(['%s']*len(use_fields))})
                    """
                    with self.conn.cursor() as cursor:
                        cursor.executemany(insert_sql, df[use_fields].values.tolist())
                    self.conn.commit()
                
            except Exception as e:
                logger.error(f"获取 {trade_date} 日期的融资融券数据失败: {e}")
                continue

class Stock_KplConceptUpdater(StockInfoBaseUpdater):
    """
    开盘啦题材库数据更新器，使用kpl_concept接口按交易日期更新，支持全量/增量更新，字段与Tushare官方文档一致。
    https://tushare.pro/document/2?doc_id=350
    """
    def __init__(self):
        super().__init__()
        self.table_name = 'stock_kpl_concept'
        # 官方文档所有字段
        self.columns = [
            'trade_date', 'ts_code', 'name', 'z_t_num', 'up_num'
        ]
        self.create_table(self._get_create_sql())

    def _get_create_sql(self):
        # 字段类型与接口文档保持一致，包含所有字段
        return f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            trade_date VARCHAR(20),
            ts_code VARCHAR(20),
            name VARCHAR(100),
            z_t_num INT,
            up_num INT,
            PRIMARY KEY(trade_date, ts_code)
        ) DEFAULT CHARSET=utf8mb4;
        """

    def update(self, mode='full', fields=None, trade_dates=None):
        """
        更新开盘啦题材库数据，使用kpl_concept接口按交易日期更新
        mode: 'full' 全量更新（覆写），'increment' 增量更新
        fields: 指定字段列表，默认使用全部字段
        trade_dates: 交易日期列表，用于按交易日期分批拉取
        """
        use_fields = fields if fields else self.columns
        if trade_dates is None:
            raise ValueError('Stock_KplConceptUpdater.update: trade_dates参数不能为空，必须分批拉取！')
        
        # 获取empty_dates和最近交易日
        empty_dates = get_empty_dates_for_updater('StockInfoArchiver', 'stock_kpl_concept')
        recent_trade_dates = get_recent_trade_dates(trade_dates, 5)
        
        if mode == 'full':
            self.truncate_table(self.table_name)
            dates_to_update = trade_dates
        else:
            # 获取已存在的交易日期
            with self.conn.cursor() as cursor:
                cursor.execute(f"SELECT DISTINCT trade_date FROM {self.table_name}")
                exist_dates = normalize_dates([row[0] for row in cursor.fetchall()])
            
            # 过滤出需要更新的日期（排除已存在和empty的日期）
            dates_to_update = filter_dates_for_update(
                trade_dates, exist_dates, empty_dates, 
                'StockInfoArchiver', 'stock_kpl_concept', recent_trade_dates
            )
        
        for trade_date in track(dates_to_update, description=f"stock_kpl_concept {mode} updating"):
            try:
                # 使用kpl_concept接口按交易日期拉取数据
                df = self.pro.kpl_concept(trade_date=trade_date, fields=','.join(use_fields))
                
                # 更新empty_dates
                update_empty_dates_after_fetch(
                    'StockInfoArchiver', 'stock_kpl_concept', trade_date, 
                    df.empty, recent_trade_dates
                )
                
                if df.empty:
                    continue
                    
                # 转换日期字段
                df = convert_dates(df, [f for f in ['trade_date'] if f in use_fields])
                if not df.empty:
                    df[use_fields] = safe_db_ready(df[use_fields], use_fields)
                    insert_sql = f"""
                    REPLACE INTO {self.table_name} ({', '.join(use_fields)}) VALUES ({', '.join(['%s']*len(use_fields))})
                    """
                    with self.conn.cursor() as cursor:
                        cursor.executemany(insert_sql, df[use_fields].values.tolist())
                    self.conn.commit()
                
            except Exception as e:
                logger.error(f"获取 {trade_date} 日期的开盘啦题材库数据失败: {e}")
                continue

class Stock_KplConceptConsUpdater(StockInfoBaseUpdater):
    """
    开盘啦题材成分数据更新器，使用kpl_concept_cons接口按交易日期更新，支持全量/增量更新，字段与Tushare官方文档一致。
    https://tushare.pro/document/2?doc_id=351
    """
    def __init__(self):
        super().__init__()
        self.table_name = 'stock_kpl_concept_cons'
        # 核心字段（去除desc和hot_num）
        self.columns = [
            'ts_code', 'con_code', 'name', 'trade_date'
        ]
        self.create_table(self._get_create_sql())

    def _get_create_sql(self):
        # 字段类型与接口文档保持一致，核心字段
        return f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            ts_code VARCHAR(20),
            con_code VARCHAR(20),
            name VARCHAR(100),
            trade_date VARCHAR(20),
            PRIMARY KEY(trade_date, ts_code, con_code)
        ) DEFAULT CHARSET=utf8mb4;
        """

    def update(self, mode='full', fields=None, trade_dates=None):
        """
        更新开盘啦题材成分数据，使用kpl_concept_cons接口按交易日期更新
        mode: 'full' 全量更新（覆写），'increment' 增量更新
        fields: 指定字段列表，默认使用全部字段
        trade_dates: 交易日期列表，用于按交易日期分批拉取
        """
        use_fields = fields if fields else self.columns
        if trade_dates is None:
            raise ValueError('Stock_KplConceptConsUpdater.update: trade_dates参数不能为空，必须分批拉取！')
        
        # 获取empty_dates和最近交易日
        empty_dates = get_empty_dates_for_updater('StockInfoArchiver', 'stock_kpl_concept_cons')
        recent_trade_dates = get_recent_trade_dates(trade_dates, 5)
        
        if mode == 'full':
            self.truncate_table(self.table_name)
            dates_to_update = trade_dates
        else:
            # 获取已存在的交易日期
            with self.conn.cursor() as cursor:
                cursor.execute(f"SELECT DISTINCT trade_date FROM {self.table_name}")
                exist_dates = normalize_dates([row[0] for row in cursor.fetchall()])
            
            # 过滤出需要更新的日期（排除已存在和empty的日期）
            dates_to_update = filter_dates_for_update(
                trade_dates, exist_dates, empty_dates, 
                'StockInfoArchiver', 'stock_kpl_concept_cons', recent_trade_dates
            )
        
        for trade_date in track(dates_to_update, description=f"stock_kpl_concept_cons {mode} updating"):
            try:
                # 使用kpl_concept_cons接口按交易日期拉取数据
                df = self.pro.kpl_concept_cons(trade_date=trade_date, fields=','.join(use_fields))
                
                # 更新empty_dates
                update_empty_dates_after_fetch(
                    'StockInfoArchiver', 'stock_kpl_concept_cons', trade_date, 
                    df.empty, recent_trade_dates
                )
                
                if df.empty:
                    continue
                    
                # 转换日期字段
                df = convert_dates(df, [f for f in ['trade_date'] if f in use_fields])
                if not df.empty:
                    df[use_fields] = safe_db_ready(df[use_fields], use_fields)
                    insert_sql = f"""
                    REPLACE INTO {self.table_name} ({', '.join(use_fields)}) VALUES ({', '.join(['%s']*len(use_fields))})
                    """
                    with self.conn.cursor() as cursor:
                        cursor.executemany(insert_sql, df[use_fields].values.tolist())
                    self.conn.commit()
                
            except Exception as e:
                logger.error(f"获取 {trade_date} 日期的开盘啦题材成分数据失败: {e}")
                continue

class Stock_KplListUpdater(StockInfoBaseUpdater):
    """
    开盘啦榜单数据更新器，使用kpl_list接口按交易日期更新，支持全量/增量更新，字段与Tushare官方文档一致。
    https://tushare.pro/document/2?doc_id=347
    """
    def __init__(self):
        super().__init__()
        self.table_name = 'stock_kpl_list'
        # 官方文档所有字段
        self.columns = [
            'ts_code', 'name', 'trade_date', 'lu_time', 'ld_time', 'open_time', 'last_time',
            'lu_desc', 'tag', 'theme', 'net_change', 'bid_amount', 'status', 'bid_change',
            'bid_turnover', 'lu_bid_vol', 'pct_chg', 'bid_pct_chg', 'rt_pct_chg', 'limit_order',
            'amount', 'turnover_rate', 'free_float', 'lu_limit_order'
        ]
        self.create_table(self._get_create_sql())

    def _get_create_sql(self):
        # 字段类型与接口文档保持一致，包含所有字段
        return f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            ts_code VARCHAR(20),
            name VARCHAR(100),
            trade_date VARCHAR(20),
            lu_time VARCHAR(20),
            ld_time VARCHAR(20),
            open_time VARCHAR(20),
            last_time VARCHAR(20),
            lu_desc TEXT,
            tag VARCHAR(50),
            theme VARCHAR(200),
            net_change FLOAT,
            bid_amount FLOAT,
            status VARCHAR(50),
            bid_change FLOAT,
            bid_turnover FLOAT,
            lu_bid_vol FLOAT,
            pct_chg FLOAT,
            bid_pct_chg FLOAT,
            rt_pct_chg FLOAT,
            limit_order FLOAT,
            amount FLOAT,
            turnover_rate FLOAT,
            free_float FLOAT,
            lu_limit_order FLOAT,
            PRIMARY KEY(trade_date, ts_code, tag)
        ) DEFAULT CHARSET=utf8mb4;
        """

    def update(self, mode='full', fields=None, trade_dates=None):
        """
        更新开盘啦榜单数据，使用kpl_list接口按交易日期更新
        mode: 'full' 全量更新（覆写），'increment' 增量更新
        fields: 指定字段列表，默认使用全部字段
        trade_dates: 交易日期列表，用于按交易日期分批拉取
        """
        use_fields = fields if fields else self.columns
        if trade_dates is None:
            raise ValueError('Stock_KplListUpdater.update: trade_dates参数不能为空，必须分批拉取！')
        
        # 获取empty_dates和最近交易日
        empty_dates = get_empty_dates_for_updater('StockInfoArchiver', 'stock_kpl_list')
        recent_trade_dates = get_recent_trade_dates(trade_dates, 5)
        
        if mode == 'full':
            self.truncate_table(self.table_name)
            dates_to_update = trade_dates
        else:
            # 获取已存在的交易日期
            with self.conn.cursor() as cursor:
                cursor.execute(f"SELECT DISTINCT trade_date FROM {self.table_name}")
                exist_dates = normalize_dates([row[0] for row in cursor.fetchall()])
            
            # 过滤出需要更新的日期（排除已存在和empty的日期）
            dates_to_update = filter_dates_for_update(
                trade_dates, exist_dates, empty_dates, 
                'StockInfoArchiver', 'stock_kpl_list', recent_trade_dates
            )
        
        for trade_date in track(dates_to_update, description=f"stock_kpl_list {mode} updating"):
            try:
                # 使用kpl_list接口按交易日期拉取数据
                df = self.pro.kpl_list(trade_date=trade_date, fields=','.join(use_fields))
                
                # 更新empty_dates
                update_empty_dates_after_fetch(
                    'StockInfoArchiver', 'stock_kpl_list', trade_date, 
                    df.empty, recent_trade_dates
                )
                
                if df.empty:
                    continue
                    
                # 转换日期字段
                df = convert_dates(df, [f for f in ['trade_date'] if f in use_fields])
                if not df.empty:
                    df[use_fields] = safe_db_ready(df[use_fields], use_fields)
                    insert_sql = f"""
                    REPLACE INTO {self.table_name} ({', '.join(use_fields)}) VALUES ({', '.join(['%s']*len(use_fields))})
                    """
                    with self.conn.cursor() as cursor:
                        cursor.executemany(insert_sql, df[use_fields].values.tolist())
                    self.conn.commit()
                
            except Exception as e:
                logger.error(f"获取 {trade_date} 日期的开盘啦榜单数据失败: {e}")
                continue

class Stock_HolderTradeUpdater(StockInfoBaseUpdater):
    """
    股东增减持数据更新器，使用stk_holdertrade接口按公告日期更新，支持全量/增量更新，字段与Tushare官方文档一致。
    https://tushare.pro/document/2?doc_id=175
    """
    def __init__(self):
        super().__init__()
        self.table_name = 'stock_holder_trade'
        # 官方文档所有字段
        self.columns = [
            'ts_code', 'ann_date', 'holder_name', 'holder_type', 'in_de', 'change_vol',
            'change_ratio', 'after_share', 'after_ratio', 'avg_price', 'total_share',
            'begin_date', 'close_date'
        ]
        self.create_table(self._get_create_sql())

    def _get_create_sql(self):
        # 字段类型与接口文档保持一致，包含所有字段
        return f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            ts_code VARCHAR(20),
            ann_date VARCHAR(20),
            holder_name VARCHAR(200),
            holder_type VARCHAR(10),
            in_de VARCHAR(10),
            change_vol FLOAT,
            change_ratio FLOAT,
            after_share FLOAT,
            after_ratio FLOAT,
            avg_price FLOAT,
            total_share FLOAT,
            begin_date VARCHAR(20),
            close_date VARCHAR(20),
            PRIMARY KEY(ts_code, ann_date, holder_name)
        ) DEFAULT CHARSET=utf8mb4;
        """

    def _get_recent_calendar_dates(self, reference_dates, days=5):
        """
        基于参考日期，获取最近N个日历日期
        
        Args:
            reference_dates: 参考日期列表
            days: 需要获取的日历日期数量
            
        Returns:
            最近N个日历日期列表
        """
        if not reference_dates:
            return []
        
        from datetime import datetime, timedelta
        
        try:
            # 获取最新的参考日期
            latest_date_str = max(reference_dates)
            if isinstance(latest_date_str, str) and len(latest_date_str) == 8:
                latest_date = datetime.strptime(latest_date_str, '%Y%m%d')
            else:
                return []
            
            # 生成最近N个日历日期
            calendar_dates = []
            for i in range(days):
                date = latest_date - timedelta(days=i)
                calendar_dates.append(date.strftime('%Y%m%d'))
            
            return sorted(calendar_dates, reverse=True)
            
        except Exception as e:
            logger.warning(f"获取最近日历日期失败: {e}")
            return []

    def update(self, mode='full', fields=None, dates=None):
        """
        更新股东增减持数据，使用stk_holdertrade接口按公告日期更新
        mode: 'full' 全量更新（覆写），'increment' 增量更新
        fields: 指定字段列表，默认使用全部字段
        dates: 日期列表，用于按公告日期分批拉取
        """
        use_fields = fields if fields else self.columns
        assert dates is not None, "dates参数不能为空"
        
        if not dates:
            logger.warning("没有可更新的日期")
            return
        
        # 获取empty_dates和最近日历日期
        empty_dates = get_empty_dates_for_updater('StockInfoArchiver', 'stock_holder_trade')
        recent_calendar_dates = self._get_recent_calendar_dates(dates, 5)
        
        if mode == 'full':
            self.truncate_table(self.table_name)
            dates_to_update = dates
        else:
            # 获取已存在的公告日期
            with self.conn.cursor() as cursor:
                cursor.execute(f"SELECT DISTINCT ann_date FROM {self.table_name}")
                exist_dates = normalize_dates([row[0] for row in cursor.fetchall()])
            
            # 删除最近3个日历日的数据，确保数据及时性
            recent_3_calendar_dates = self._get_recent_calendar_dates(dates, 3)
            if recent_3_calendar_dates:
                recent_3_dates_str = "', '".join(recent_3_calendar_dates)
                delete_sql = f"DELETE FROM {self.table_name} WHERE ann_date IN ('{recent_3_dates_str}')"
                with self.conn.cursor() as cursor:
                    cursor.execute(delete_sql)
                self.conn.commit()
                logger.info(f"删除最近3个日历日的数据: {recent_3_calendar_dates}")
                
                # 从已存在日期中移除最近3个日历日
                exist_dates = exist_dates - set(recent_3_calendar_dates)
            
            # 过滤出需要更新的日期（排除已存在和empty的日期，但最近5个日历日不进入empty_dates）
            dates_to_update = []
            for date in dates:
                if date in exist_dates:
                    continue
                if date in empty_dates and date not in recent_calendar_dates:
                    continue
                dates_to_update.append(date)
        
        for ann_date in track(dates_to_update, description=f"stock_holder_trade {mode} updating"):
            try:
                # 使用stk_holdertrade接口按公告日期拉取数据
                df = self.pro.stk_holdertrade(ann_date=ann_date, fields=','.join(use_fields))
                
                # 更新empty_dates（最近5个日历日不进入empty_dates）
                if ann_date not in recent_calendar_dates:
                    update_empty_dates_after_fetch(
                        'StockInfoArchiver', 'stock_holder_trade', ann_date, 
                        df.empty, recent_calendar_dates
                    )
                
                if df.empty:
                    continue
                    
                # 转换日期字段
                df = convert_dates(df, [f for f in ['ann_date', 'begin_date', 'close_date'] if f in use_fields])
                if not df.empty:
                    df[use_fields] = safe_db_ready(df[use_fields], use_fields)
                    insert_sql = f"""
                    REPLACE INTO {self.table_name} ({', '.join(use_fields)}) VALUES ({', '.join(['%s']*len(use_fields))})
                    """
                    with self.conn.cursor() as cursor:
                        cursor.executemany(insert_sql, df[use_fields].values.tolist())
                    self.conn.commit()
                
            except Exception as e:
                logger.error(f"获取 {ann_date} 日期的股东增减持数据失败: {e}")
                continue

def main():
    """
    主函数：更新股票信息相关数据
    """
    # stock_basic
    try:
        mode_basic = STOCK_INFO_ARCHIVER_UPDATE_MODE.get('stock_basic', 'full')
        logger.info(f"开始更新股票基本信息，模式: {mode_basic} ...")
        stock_basic_updater = Stock_BasicUpdater()
        stock_basic_updater.update(mode=mode_basic)
        stock_basic_updater.close()
        logger.info("股票基本信息更新完成！")
    except Exception as e:
        logger.error(f"股票基本信息更新失败: {e}")

    # 获取交易日（上交所，2019-01-01至今）和 日历日期
    try:
        conn = pymysql.connect(
            host=Config.MYSQL_HOST,
            port=Config.MYSQL_PORT,
            user=Config.MYSQL_USER,
            password=Config.MYSQL_PASSWORD,
            database=Config.MYSQL_DATABASE,
            charset='utf8mb4'
        )
        trade_dates = get_trade_dates(conn, '2019-01-01', pd.Timestamp.today().strftime('%Y-%m-%d'))
        calendar_dates = generate_date_range('2019-01-01', pd.Timestamp.today().strftime('%Y-%m-%d'), include_next_day=True)
        
        # 生成报告期列表（季度末日期）
        report_periods = []
        start_year = 2019
        current_year = pd.Timestamp.today().year
        for year in range(start_year, current_year + 1):
            for quarter_end in ['0331', '0630', '0930', '1231']:
                period = f"{year}{quarter_end}"
                report_periods.append(period)
        
        conn.close()
    except Exception as e:
        logger.error(f"获取交易日失败: {e}")
        trade_dates = []

    # stock_namechange
    try:
        mode_namechange = STOCK_INFO_ARCHIVER_UPDATE_MODE.get('stock_namechange', 'full')
        logger.info(f"开始更新股票曾用名数据，模式: {mode_namechange} ...")
        stock_namechange_updater = Stock_NameChangeUpdater()
        stock_namechange_updater.update(mode=mode_namechange, dates=calendar_dates)
        stock_namechange_updater.close()
        logger.info("股票曾用名数据更新完成！")
    except Exception as e:
        logger.error(f"股票曾用名数据更新失败: {e}")

    # stock_daily
    try:
        mode_daily = STOCK_INFO_ARCHIVER_UPDATE_MODE.get('stock_daily', 'full')
        logger.info(f"开始更新A股日线行情数据，模式: {mode_daily} ...")
        stock_daily_updater = Stock_DailyUpdater()
        stock_daily_updater.update(mode=mode_daily, trade_dates=trade_dates)
        stock_daily_updater.close()
        logger.info("A股日线行情数据更新完成！")
    except Exception as e:
        logger.error(f"A股日线行情数据更新失败: {e}")

    # stock_income
    try:
        mode_income = STOCK_INFO_ARCHIVER_UPDATE_MODE.get('stock_income', 'full')
        logger.info(f"开始更新股票利润表数据，模式: {mode_income} ...")
        stock_income_updater = Stock_IncomeUpdater()
        stock_income_updater.update(mode=mode_income, dates=calendar_dates)
        stock_income_updater.close()
        logger.info("股票利润表数据更新完成！")
    except Exception as e:
        logger.error(f"股票利润表数据更新失败: {e}")

    # stock_cashflow
    try:
        mode_cashflow = STOCK_INFO_ARCHIVER_UPDATE_MODE.get('stock_cashflow', 'full')
        logger.info(f"开始更新股票现金流量表数据，模式: {mode_cashflow} ...")
        stock_cashflow_updater = Stock_CashflowUpdater()
        stock_cashflow_updater.update(mode=mode_cashflow, dates=calendar_dates)
        stock_cashflow_updater.close()
        logger.info("股票现金流量表数据更新完成！")
    except Exception as e:
        logger.error(f"股票现金流量表数据更新失败: {e}")

    # stock_balancesheet
    try:
        mode_balancesheet = STOCK_INFO_ARCHIVER_UPDATE_MODE.get('stock_balancesheet', 'full')
        logger.info(f"开始更新股票资产负债表数据，模式: {mode_balancesheet} ...")
        stock_balancesheet_updater = Stock_BalancesheetUpdater()
        stock_balancesheet_updater.update(mode=mode_balancesheet, dates=calendar_dates)
        stock_balancesheet_updater.close()
        logger.info("股票资产负债表数据更新完成！")
    except Exception as e:
        logger.error(f"股票资产负债表数据更新失败: {e}")

    # stock_forecast
    try:
        mode_forecast = STOCK_INFO_ARCHIVER_UPDATE_MODE.get('stock_forecast', 'full')
        logger.info(f"开始更新股票业绩预告数据，模式: {mode_forecast} ...")
        stock_forecast_updater = Stock_ForecastUpdater()
        stock_forecast_updater.update(mode=mode_forecast, dates=calendar_dates)
        stock_forecast_updater.close()
        logger.info("股票业绩预告数据更新完成！")
    except Exception as e:
        logger.error(f"股票业绩预告数据更新失败: {e}")

    # stock_express
    try:
        mode_express = STOCK_INFO_ARCHIVER_UPDATE_MODE.get('stock_express', 'full')
        logger.info(f"开始更新股票业绩快报数据，模式: {mode_express} ...")
        stock_express_updater = Stock_ExpressUpdater()
        stock_express_updater.update(mode=mode_express, dates=calendar_dates)
        stock_express_updater.close()
        logger.info("股票业绩快报数据更新完成！")
    except Exception as e:
        logger.error(f"股票业绩快报数据更新失败: {e}")

    # stock_fina_indicator
    try:
        mode_fina_indicator = STOCK_INFO_ARCHIVER_UPDATE_MODE.get('stock_fina_indicator', 'full')
        logger.info(f"开始更新股票财务指标数据，模式: {mode_fina_indicator} ...")
        stock_fina_indicator_updater = Stock_FinaIndicatorUpdater()
        stock_fina_indicator_updater.update(mode=mode_fina_indicator, dates=calendar_dates)
        stock_fina_indicator_updater.close()
        logger.info("股票财务指标数据更新完成！")
    except Exception as e:
        logger.error(f"股票财务指标数据更新失败: {e}")

    # stock_fina_mainbz
    try:
        mode_fina_mainbz = STOCK_INFO_ARCHIVER_UPDATE_MODE.get('stock_fina_mainbz', 'full')
        logger.info(f"开始更新股票主营业务构成数据，模式: {mode_fina_mainbz} ...")
        stock_fina_mainbz_updater = Stock_FinaMainbzUpdater()
        stock_fina_mainbz_updater.update(mode=mode_fina_mainbz, periods=report_periods)
        stock_fina_mainbz_updater.close()
        logger.info("股票主营业务构成数据更新完成！")
    except Exception as e:
        logger.error(f"股票主营业务构成数据更新失败: {e}")

    # stock_dividend
    try:
        mode_dividend = STOCK_INFO_ARCHIVER_UPDATE_MODE.get('stock_dividend', 'full')
        logger.info(f"开始更新股票分红送股数据，模式: {mode_dividend} ...")
        stock_dividend_updater = Stock_DividendUpdater()
        stock_dividend_updater.update(mode=mode_dividend, dates=calendar_dates)
        stock_dividend_updater.close()
        logger.info("股票分红送股数据更新完成！")
    except Exception as e:
        logger.error(f"股票分红送股数据更新失败: {e}")

    # stock_block_trade
    try:
        mode_block_trade = STOCK_INFO_ARCHIVER_UPDATE_MODE.get('stock_block_trade', 'full')
        logger.info(f"开始更新股票大宗交易数据，模式: {mode_block_trade} ...")
        stock_block_trade_updater = Stock_BlockTradeUpdater()
        stock_block_trade_updater.update(mode=mode_block_trade, trade_dates=trade_dates)
        stock_block_trade_updater.close()
        logger.info("股票大宗交易数据更新完成！")
    except Exception as e:
        logger.error(f"股票大宗交易数据更新失败: {e}")

    # stock_margin
    try:
        mode_margin = STOCK_INFO_ARCHIVER_UPDATE_MODE.get('stock_margin', 'full')
        logger.info(f"开始更新融资融券交易汇总数据，模式: {mode_margin} ...")
        stock_margin_updater = Stock_MarginUpdater()
        stock_margin_updater.update(mode=mode_margin, trade_dates=trade_dates)
        stock_margin_updater.close()
        logger.info("融资融券交易汇总数据更新完成！")
    except Exception as e:
        logger.error(f"融资融券交易汇总数据更新失败: {e}")

    # stock_kpl_concept
    try:
        mode_kpl_concept = STOCK_INFO_ARCHIVER_UPDATE_MODE.get('stock_kpl_concept', 'full')
        logger.info(f"开始更新开盘啦题材库数据，模式: {mode_kpl_concept} ...")
        stock_kpl_concept_updater = Stock_KplConceptUpdater()
        stock_kpl_concept_updater.update(mode=mode_kpl_concept, trade_dates=trade_dates)
        stock_kpl_concept_updater.close()
        logger.info("开盘啦题材库数据更新完成！")
    except Exception as e:
        logger.error(f"开盘啦题材库数据更新失败: {e}")

    # stock_kpl_concept_cons
    try:
        mode_kpl_concept_cons = STOCK_INFO_ARCHIVER_UPDATE_MODE.get('stock_kpl_concept_cons', 'full')
        logger.info(f"开始更新开盘啦题材成分数据，模式: {mode_kpl_concept_cons} ...")
        stock_kpl_concept_cons_updater = Stock_KplConceptConsUpdater()
        stock_kpl_concept_cons_updater.update(mode=mode_kpl_concept_cons, trade_dates=trade_dates)
        stock_kpl_concept_cons_updater.close()
        logger.info("开盘啦题材成分数据更新完成！")
    except Exception as e:
        logger.error(f"开盘啦题材成分数据更新失败: {e}")

    # stock_kpl_list
    try:
        mode_kpl_list = STOCK_INFO_ARCHIVER_UPDATE_MODE.get('stock_kpl_list', 'full')
        logger.info(f"开始更新开盘啦榜单数据，模式: {mode_kpl_list} ...")
        stock_kpl_list_updater = Stock_KplListUpdater()
        stock_kpl_list_updater.update(mode=mode_kpl_list, trade_dates=trade_dates)
        stock_kpl_list_updater.close()
        logger.info("开盘啦榜单数据更新完成！")
    except Exception as e:
        logger.error(f"开盘啦榜单数据更新失败: {e}")

    # stock_holder_trade
    try:
        mode_holder_trade = STOCK_INFO_ARCHIVER_UPDATE_MODE.get('stock_holder_trade', 'full')
        logger.info(f"开始更新股东增减持数据，模式: {mode_holder_trade} ...")
        stock_holder_trade_updater = Stock_HolderTradeUpdater()
        stock_holder_trade_updater.update(mode=mode_holder_trade, dates=calendar_dates)
        stock_holder_trade_updater.close()
        logger.info("股东增减持数据更新完成！")
    except Exception as e:
        logger.error(f"股东增减持数据更新失败: {e}")

    # 更新东方财富概念板块数据
    try:
        mode_dc_index = STOCK_INFO_ARCHIVER_UPDATE_MODE.get('stock_dc_index', 'full')
        logger.info(f"开始更新东方财富概念板块数据，模式: {mode_dc_index} ...")
        stock_dc_index_updater = Stock_DcIndexUpdater()
        stock_dc_index_updater.update(mode=mode_dc_index, trade_dates=trade_dates)
        stock_dc_index_updater.close()
        logger.info("东方财富概念板块数据更新完成！")
    except Exception as e:
        logger.error(f"东方财富概念板块数据更新失败: {e}")

    # 更新东方财富板块成分数据
    try:
        mode_dc_member = STOCK_INFO_ARCHIVER_UPDATE_MODE.get('stock_dc_member', 'full')
        logger.info(f"开始更新东方财富板块成分数据，模式: {mode_dc_member} ...")
        stock_dc_member_updater = Stock_DcMemberUpdater()
        stock_dc_member_updater.update(mode=mode_dc_member, trade_dates=trade_dates)
        stock_dc_member_updater.close()
        logger.info("东方财富板块成分数据更新完成！")
    except Exception as e:
        logger.error(f"东方财富板块成分数据更新失败: {e}")

class Stock_DcIndexUpdater(StockInfoBaseUpdater):
    """
    东方财富概念板块数据更新器，使用dc_index接口按交易日期更新，支持全量/增量更新，字段与Tushare官方文档一致。
    https://tushare.pro/document/2?doc_id=362
    """
    def __init__(self):
        super().__init__()
        self.table_name = 'stock_dc_index'
        # 官方文档所有字段（leading改名为leader避免SQL关键字冲突）
        self.columns = [
            'ts_code', 'trade_date', 'name', 'leader', 'leading_code', 'pct_change',
            'leading_pct', 'total_mv', 'turnover_rate', 'up_num', 'down_num'
        ]
        self.create_table(self._get_create_sql())

    def _get_create_sql(self):
        # 字段类型与接口文档保持一致，包含所有字段
        return f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            ts_code VARCHAR(20),
            trade_date VARCHAR(20),
            name VARCHAR(100),
            leader VARCHAR(100),
            leading_code VARCHAR(20),
            pct_change FLOAT,
            leading_pct FLOAT,
            total_mv FLOAT,
            turnover_rate FLOAT,
            up_num INT,
            down_num INT,
            PRIMARY KEY(ts_code, trade_date)
        ) DEFAULT CHARSET=utf8mb4;
        """

    def update(self, mode='full', fields=None, trade_dates=None):
        """
        更新东方财富概念板块数据，使用dc_index接口按交易日期更新
        mode: 'full' 全量更新（覆写），'increment' 增量更新
        fields: 指定字段列表，默认使用全部字段
        trade_dates: 交易日期列表，用于按交易日期分批拉取
        """
        use_fields = fields if fields else self.columns
        if trade_dates is None:
            raise ValueError('Stock_DcIndexUpdater.update: trade_dates参数不能为空，必须分批拉取！')
        
        # 获取empty_dates和最近交易日
        empty_dates = get_empty_dates_for_updater('StockInfoArchiver', 'stock_dc_index')
        recent_trade_dates = get_recent_trade_dates(trade_dates, 5)
        
        if mode == 'full':
            self.truncate_table(self.table_name)
            dates_to_update = trade_dates
        else:
            # 获取已存在的交易日期
            with self.conn.cursor() as cursor:
                cursor.execute(f"SELECT DISTINCT trade_date FROM {self.table_name}")
                exist_dates = normalize_dates([row[0] for row in cursor.fetchall()])
            
            # 过滤出需要更新的日期（排除已存在和empty的日期）
            dates_to_update = filter_dates_for_update(
                trade_dates, exist_dates, empty_dates, 
                'StockInfoArchiver', 'stock_dc_index', recent_trade_dates
            )
        
        for trade_date in track(dates_to_update, description=f"stock_dc_index {mode} updating"):
            try:
                # 准备API字段列表（将leader映射回leading）
                api_fields = [f if f != 'leader' else 'leading' for f in use_fields]
                
                # 使用dc_index接口按交易日期拉取数据
                df = self.pro.dc_index(trade_date=trade_date, fields=','.join(api_fields))
                
                # 更新empty_dates
                update_empty_dates_after_fetch(
                    'StockInfoArchiver', 'stock_dc_index', trade_date, 
                    df.empty, recent_trade_dates
                )
                
                if df.empty:
                    continue
                
                # 重命名字段：将API返回的leading重命名为leader
                if 'leading' in df.columns and 'leader' in use_fields:
                    df = df.rename(columns={'leading': 'leader'})
                    
                # 转换日期字段
                df = convert_dates(df, [f for f in ['trade_date'] if f in use_fields])
                if not df.empty:
                    df[use_fields] = safe_db_ready(df[use_fields], use_fields)
                    insert_sql = f"""
                    REPLACE INTO {self.table_name} ({', '.join(use_fields)}) VALUES ({', '.join(['%s']*len(use_fields))})
                    """
                    with self.conn.cursor() as cursor:
                        cursor.executemany(insert_sql, df[use_fields].values.tolist())
                    self.conn.commit()
                
            except Exception as e:
                logger.error(f"获取 {trade_date} 日期的东方财富概念板块数据失败: {e}")
                continue

class Stock_DcMemberUpdater(StockInfoBaseUpdater):
    """
    东方财富板块成分数据更新器，使用dc_member接口按交易日期更新，支持全量/增量更新，字段与Tushare官方文档一致。
    https://tushare.pro/document/2?doc_id=363
    """
    def __init__(self):
        super().__init__()
        self.table_name = 'stock_dc_member'
        # 官方文档所有字段
        self.columns = [
            'trade_date', 'ts_code', 'con_code', 'name'
        ]
        self.create_table(self._get_create_sql())

    def _get_create_sql(self):
        # 字段类型与接口文档保持一致，包含所有字段
        return f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            trade_date VARCHAR(20),
            ts_code VARCHAR(20),
            con_code VARCHAR(20),
            name VARCHAR(100),
            PRIMARY KEY(trade_date, ts_code, con_code)
        ) DEFAULT CHARSET=utf8mb4;
        """

    def update(self, mode='full', fields=None, trade_dates=None):
        """
        更新东方财富板块成分数据，使用dc_member接口按交易日期更新
        mode: 'full' 全量更新（覆写），'increment' 增量更新
        fields: 指定字段列表，默认使用全部字段
        trade_dates: 交易日期列表，用于按交易日期分批拉取
        """
        use_fields = fields if fields else self.columns
        if trade_dates is None:
            raise ValueError('Stock_DcMemberUpdater.update: trade_dates参数不能为空，必须分批拉取！')
        
        # 获取empty_dates和最近交易日
        empty_dates = get_empty_dates_for_updater('StockInfoArchiver', 'stock_dc_member')
        recent_trade_dates = get_recent_trade_dates(trade_dates, 5)
        
        if mode == 'full':
            self.truncate_table(self.table_name)
            dates_to_update = trade_dates
        else:
            # 获取已存在的交易日期
            with self.conn.cursor() as cursor:
                cursor.execute(f"SELECT DISTINCT trade_date FROM {self.table_name}")
                exist_dates = normalize_dates([row[0] for row in cursor.fetchall()])
            
            # 过滤出需要更新的日期（排除已存在和empty的日期）
            dates_to_update = filter_dates_for_update(
                trade_dates, exist_dates, empty_dates, 
                'StockInfoArchiver', 'stock_dc_member', recent_trade_dates
            )
        
        for trade_date in track(dates_to_update, description=f"stock_dc_member {mode} updating"):
            try:
                # 使用dc_member接口按交易日期拉取数据
                df = self.pro.dc_member(trade_date=trade_date, fields=','.join(use_fields))
                
                # 更新empty_dates
                update_empty_dates_after_fetch(
                    'StockInfoArchiver', 'stock_dc_member', trade_date, 
                    df.empty, recent_trade_dates
                )
                
                if df.empty:
                    continue
                    
                # 转换日期字段
                df = convert_dates(df, [f for f in ['trade_date'] if f in use_fields])
                if not df.empty:
                    df[use_fields] = safe_db_ready(df[use_fields], use_fields)
                    insert_sql = f"""
                    REPLACE INTO {self.table_name} ({', '.join(use_fields)}) VALUES ({', '.join(['%s']*len(use_fields))})
                    """
                    with self.conn.cursor() as cursor:
                        cursor.executemany(insert_sql, df[use_fields].values.tolist())
                    self.conn.commit()
                
            except Exception as e:
                logger.error(f"获取 {trade_date} 日期的东方财富板块成分数据失败: {e}")
                continue

if __name__ == "__main__":
    main()
