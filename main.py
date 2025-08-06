#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
TushareArchiverPublic 主入口程序
一次性更新所有数据模块：可转债数据、股票数据
"""

import sys
import os
from datetime import datetime, timedelta
from loguru import logger

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from utils import generate_date_range
from config import Config
import pymysql

def setup_logger():
    """配置日志"""
    # 移除默认handler
    logger.remove()
    
    # 添加控制台输出
    logger.add(
        sys.stdout,
        format="<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        level="INFO"
    )
    
    # 添加文件输出
    log_file = f"logs/tushare_archiver_{datetime.now().strftime('%Y%m%d')}.log"
    os.makedirs("logs", exist_ok=True)
    logger.add(
        log_file,
        format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {name}:{function}:{line} - {message}",
        level="DEBUG",
        rotation="500 MB",
        retention="30 days"
    )

def update_convertible_bond_data():
    """更新可转债数据"""
    logger.info("=" * 60)
    logger.info("开始更新可转债数据模块")
    logger.info("=" * 60)
    
    try:
        from CBArchiver.CBDailyArchiver import main as cb_main
        cb_main()
        logger.success("可转债数据更新完成！")
    except Exception as e:
        logger.error(f"可转债数据更新失败: {e}")
        return False
    
    return True

def update_stock_info_data():
    """更新股票信息数据"""
    logger.info("=" * 60)
    logger.info("开始更新股票信息数据模块")
    logger.info("=" * 60)
    
    try:
        from StockInfoArchiver.StockInfoDailyArchiver import main as stock_main
        stock_main()
        logger.success("股票信息数据更新完成！")
    except Exception as e:
        logger.error(f"股票信息数据更新失败: {e}")
        return False
    
    return True

def update_basic_data():
    """更新基础数据（如交易日历等）"""
    logger.info("=" * 60)
    logger.info("开始更新基础数据")
    logger.info("=" * 60)
    
    try:
        from BasicArchiver.BasicDailyArchiver import main as basic_main
        basic_main()
        return True
    except Exception as e:
        logger.error(f"基础数据更新失败: {e}")
        return False

def print_summary(results):
    """打印更新结果摘要"""
    logger.info("=" * 60)
    logger.info("数据更新结果摘要")
    logger.info("=" * 60)
    
    total_modules = len(results)
    success_modules = sum(1 for success in results.values() if success)
    failed_modules = total_modules - success_modules
    
    for module_name, success in results.items():
        status = "✅ 成功" if success else "❌ 失败"
        logger.info(f"{module_name:<20} : {status}")
    
    logger.info("-" * 60)
    logger.info(f"总模块数: {total_modules}")
    logger.info(f"成功模块: {success_modules}")
    logger.info(f"失败模块: {failed_modules}")
    
    if failed_modules == 0:
        logger.success("🎉 所有数据模块更新成功！")
    else:
        logger.warning(f"⚠️  有 {failed_modules} 个模块更新失败，请检查日志")

def test_database_connection(config):
    """测试数据库连接"""
    logger.info("🔗 测试数据库连接...")
    
    try:
        # 尝试连接数据库
        conn = pymysql.connect(
            host=config.MYSQL_HOST,
            port=config.MYSQL_PORT,
            user=config.MYSQL_USER,
            password=config.MYSQL_PASSWORD,
            database=config.MYSQL_DATABASE,
            charset='utf8mb4',
            connect_timeout=10  # 10秒连接超时
        )
        
        # 测试数据库操作
        with conn.cursor() as cursor:
            cursor.execute("SELECT VERSION()")
            db_version = cursor.fetchone()[0]
            logger.info(f"✅ 数据库连接成功")
            logger.info(f"数据库版本: {db_version}")
            logger.info(f"连接信息: {config.MYSQL_USER}@{config.MYSQL_HOST}:{config.MYSQL_PORT}/{config.MYSQL_DATABASE}")
        
        conn.close()
        return True
        
    except pymysql.err.OperationalError as e:
        error_code, error_msg = e.args
        if error_code == 1045:
            logger.error("❌ 数据库连接失败: 用户名或密码错误")
            logger.error("请检查config.py中的MYSQL_USER和MYSQL_PASSWORD配置")
        elif error_code == 2003:
            logger.error("❌ 数据库连接失败: 无法连接到MySQL服务器")
            logger.error(f"请检查MySQL服务是否启动，以及MYSQL_HOST({config.MYSQL_HOST})和MYSQL_PORT({config.MYSQL_PORT})配置")
        elif error_code == 1049:
            logger.error(f"❌ 数据库连接失败: 数据库'{config.MYSQL_DATABASE}'不存在")
            logger.error("请先创建数据库或检查MYSQL_DATABASE配置")
        else:
            logger.error(f"❌ 数据库连接失败: [{error_code}] {error_msg}")
        return False
        
    except Exception as e:
        logger.error(f"❌ 数据库连接失败: {e}")
        logger.error("请检查数据库配置和网络连接")
        return False

def test_tushare_api(config):
    """测试Tushare API连接"""
    logger.info("🔌 测试Tushare API连接...")
    
    try:
        import tushare as ts
        
        # 初始化Tushare API
        if not hasattr(config, 'TUSHARE_TOKEN') or not config.TUSHARE_TOKEN:
            logger.error("❌ Tushare API测试失败: 未配置TUSHARE_TOKEN")
            logger.error("请在config.py中设置正确的TUSHARE_TOKEN")
            return False
        
        pro = ts.pro_api(config.TUSHARE_TOKEN)
        
        # 测试API调用 - 获取交易日历
        df = pro.trade_cal(exchange='', start_date='20250101', end_date='20250110')
        
        if df is not None and not df.empty:
            logger.info("✅ Tushare API连接成功")
            logger.info(f"API权限测试通过，获取到 {len(df)} 条交易日历数据")
            return True
        else:
            logger.error("❌ Tushare API测试失败: 返回数据为空")
            return False
            
    except Exception as e:
        error_msg = str(e)
        if "您每分钟最多访问该接口" in error_msg:
            logger.warning("⚠️  Tushare API调用频率限制，但连接正常")
            return True
        elif "权限不足" in error_msg or "没有访问权限" in error_msg:
            logger.error("❌ Tushare API测试失败: 权限不足")
            logger.error("请检查您的Tushare账户权限和积分")
            return False
        elif "无效的token" in error_msg or "token错误" in error_msg:
            logger.error("❌ Tushare API测试失败: Token无效")
            logger.error("请检查config.py中的TUSHARE_TOKEN是否正确")
            return False
        else:
            logger.error(f"❌ Tushare API测试失败: {e}")
            logger.error("请检查网络连接和Tushare配置")
            return False

def main():
    """主函数"""
    setup_logger()
    
    start_time = datetime.now()
    logger.info("🚀 TushareArchiverPublic 数据更新开始")
    logger.info(f"开始时间: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 检查配置
    try:
        config = Config()
        logger.info("✅ 配置文件加载成功")
    except Exception as e:
        logger.error(f"❌ 配置文件加载失败: {e}")
        logger.error("请确保config.py文件存在且配置正确")
        return
    
    # 测试数据库连接
    if not test_database_connection(config):
        logger.error("💥 数据库连接测试失败，程序退出")
        logger.error("请解决数据库连接问题后重新运行程序")
        return
    
    # 测试Tushare API连接
    if not test_tushare_api(config):
        logger.error("💥 Tushare API连接测试失败，程序退出")
        logger.error("请解决Tushare配置问题后重新运行程序")
        return
    
    logger.info("🎉 所有连接测试通过，开始数据更新流程")
    logger.info("=" * 60)
    
    # 存储更新结果
    results = {}
    
    # 1. 更新基础数据
    logger.info("🔄 步骤 1/3: 更新基础数据")
    results["基础数据"] = update_basic_data()
    
    # 2. 更新可转债数据
    logger.info("🔄 步骤 2/3: 更新可转债数据")
    results["可转债数据"] = update_convertible_bond_data()
    
    # 3. 更新股票信息数据
    logger.info("🔄 步骤 3/3: 更新股票信息数据")
    results["股票信息数据"] = update_stock_info_data()
    
    # 打印结果摘要
    print_summary(results)
    
    # 计算耗时
    end_time = datetime.now()
    duration = end_time - start_time
    
    logger.info("=" * 60)
    logger.info(f"⏰ 更新完成时间: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"⏱️  总耗时: {duration}")
    logger.info("🏁 TushareArchiverPublic 数据更新结束")

if __name__ == "__main__":
    main() 