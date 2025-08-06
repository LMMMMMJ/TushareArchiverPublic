# TushareArchiverPublic

## 项目简介

TushareArchiverPublic 是一个全面的金融数据归档系统，基于 Tushare API 和 MySQL 数据库，支持股票、可转债、基础市场等多类金融数据的自动化采集、分批入库和智能更新。系统设计用于量化投资和金融数据分析，提供完整的数据生态系统支持。

## 核心特性

- 🔄 **智能更新机制**：支持全量/增量更新，避免重复拉取
- 📊 **全面数据覆盖**：股票基本面、技术面、资金面、情绪面、主题面数据
- ⚡ **高效性能**：分批拉取、empty_dates保护、最近日期刷新机制
- 🛡️ **数据质量保证**：字段完整性检查、数据类型转换、错误处理
- 🏗️ **模块化架构**：清晰的类继承结构，易于扩展和维护
- 📝 **完整日志**：详细的更新日志和错误追踪

## 目录结构

```
TushareArchiverPublic/
├── main.py                         # 🚀 主入口程序（一键更新所有数据）
├── config.py                       # 全局数据库与Tushare配置
├── update_mode.py                  # 各子系统数据更新模式配置
├── utils.py                        # 工具函数（empty_dates管理、日期处理等）
├── empty_dates.json               # 统一管理所有Archiver的empty_dates
├── .gitignore                      # Git忽略文件配置
├── README.md                       # 项目说明文档
├── scripts/                        # 🔧 批处理脚本
│   ├── run_tushare_archiver.bat   # Windows自动化执行脚本
│   └── run_simple.bat             # 简化版执行脚本
├── logs/                           # 📝 日志文件目录
├── StockInfoArchiver/
│   └── StockInfoDailyArchiver.py  # 股票信息数据采集（18个Updater类）
├── CBArchiver/
│   └── CBDailyArchiver.py         # 可转债数据采集（7个Updater类）
├── BasicArchiver/
│   └── BasicDailyArchiver.py      # 基础市场数据采集（1个Updater类）
└── __init__.py
```

## 依赖安装

```bash
pip install tushare pymysql pandas numpy loguru rich
```

## 安装配置指南

### 1. 环境配置

#### Python 环境
建议使用 conda 创建独立环境：
```bash
# 创建环境（可自定义环境名称）
conda create -n tushare python=3.8
conda activate tushare

# 安装依赖
pip install tushare pymysql pandas numpy loguru rich
```

**注意**：如果您使用不同的环境名称，请修改以下脚本中的环境名：
- `scripts/run_simple.bat`
- `scripts/run_tushare_archiver.bat`

将 `conda activate tushare` 中的 `tushare` 改为您的环境名称。

#### 数据库配置
1. 安装并启动 MySQL 服务
2. 创建数据库（可自定义名称）：
```sql
CREATE DATABASE TushareArchiverPublic DEFAULT CHARSET=utf8mb4;
```

### 2. 配置文件设置

编辑 `config.py` 文件，填入您的实际配置：

```python
class Config:
    MYSQL_HOST = "localhost"        # 您的MySQL服务器地址
    MYSQL_PORT = 3306              # 您的MySQL端口
    MYSQL_USER = "your_username"   # 您的MySQL用户名
    MYSQL_PASSWORD = "your_password"  # 您的MySQL密码
    MYSQL_DATABASE = "TushareArchiverPublic"  # 您的数据库名
    TUSHARE_TOKEN = "your_tushare_token"  # 您的Tushare Token
```

### 3. Tushare Token 获取

1. 访问 [Tushare官网](https://tushare.pro/)
2. 注册账号并登录
3. 在个人中心获取您的 Token
4. 将 Token 填入 `config.py` 中的 `TUSHARE_TOKEN`

### 4. 脚本路径配置

**重要**：使用批处理脚本前，请先修改脚本中的路径：

编辑以下文件，将 `PATH_TO_YOUR_PROJECT_ROOT` 替换为您的实际项目路径：
- `scripts/run_simple.bat`
- `scripts/run_tushare_archiver.bat`

例如：将 `PATH_TO_YOUR_PROJECT_ROOT` 改为 `C:\Users\YourName\TushareArchiverPublic`

### 5. 验证配置

运行以下命令验证配置是否正确：
```bash
python main.py
```

如果配置正确，程序会：
1. 测试数据库连接
2. 测试 Tushare API 连接
3. 开始数据更新流程

### 更新模式配置

在 `update_mode.py` 中集中管理各数据源的更新模式：

```python
# 股票信息数据更新模式
STOCK_INFO_ARCHIVER_UPDATE_MODE = {
    'stock_basic': 'full',           # 股票基本信息
    'stock_namechange': 'full',      # 股票曾用名
    'stock_daily': 'full',           # A股日线行情
    'stock_income': 'full',          # 股票利润表
    'stock_cashflow': 'full',        # 股票现金流量表
    'stock_balancesheet': 'full',    # 股票资产负债表
    'stock_forecast': 'full',        # 股票业绩预告
    'stock_express': 'full',         # 股票业绩快报
    'stock_fina_indicator': 'full',  # 股票财务指标
    'stock_fina_mainbz': 'full',     # 股票主营业务构成
    'stock_dividend': 'full',        # 股票分红送股
    'stock_fina_audit': 'full',      # 股票财务审计意见
    'stock_block_trade': 'full',     # 股票大宗交易
    'stock_margin': 'full',          # 融资融券交易汇总
    'stock_kpl_concept': 'full',     # 开盘啦题材库
    'stock_kpl_concept_cons': 'full', # 开盘啦题材成分
    'stock_kpl_list': 'full',        # 开盘啦榜单数据
    'stock_holder_trade': 'full',    # 股东增减持
}

# 可转债数据更新模式
CB_ARCHIVER_UPDATE_MODE = {
    'cb_basic': 'full',      # 可转债基本信息
    'cb_issue': 'full',      # 可转债发行数据
    'cb_call': 'full',       # 可转债赎回信息
    'cb_daily': 'full',      # 可转债行情
    'cb_share': 'full',      # 可转债转股结果
    'repo_daily': 'full',    # 债券回购日行情
    'bond_blk': 'full',      # 债券大宗交易
}

# 基础数据更新模式
BASIC_ARCHIVER_UPDATE_MODE = {
    'trade_cal': 'full',     # 交易日历
}
```

**更新模式说明**：
- `full`：全量更新（清空表后重新插入所有数据）
- `increment`：增量更新（只插入新数据，支持最近日期刷新机制）

## 快速开始

### 🚀 一键执行（推荐）

使用主入口程序，一次性更新所有数据模块：

```bash
# Python 方式
python main.py

# Windows 批处理（需要在scripts目录下运行）
scripts\run_tushare_archiver.bat

# 或使用简化版本
scripts\run_simple.bat
```

### 📊 数据更新流程

主入口程序会按以下顺序执行：

1. **配置检查**：验证数据库连接和Tushare API配置
2. **日期计算**：生成交易日期和日历日期列表（默认30天）
3. **模块更新**：
   - 基础数据更新（交易日历等）
   - 可转债数据更新（7个数据源）
   - 股票信息数据更新（18个数据源）
4. **结果汇总**：显示更新状态和耗时统计

### 🔧 Windows 自动化

#### 方法一：直接运行批处理文件
```cmd
# 确保已按照上面的说明修改脚本中的PATH_TO_YOUR_PROJECT_ROOT
# 然后执行自动化脚本
scripts\run_tushare_archiver.bat
```

#### 方法二：设置定时任务
```cmd
# 以管理员权限运行，创建每日凌晨2点的定时任务
# 请将路径替换为您的实际项目路径
schtasks /create /tn "TushareArchiverPublic_Daily" /tr "C:\path\to\your\TushareArchiverPublic\scripts\run_tushare_archiver.bat" /sc daily /st 02:00 /ru SYSTEM /rl HIGHEST /f
```

### 📝 日志管理

- **日志目录**：`logs/`
- **文件格式**：`tushare_archiver_YYYYMMDD.log`
- **日志级别**：DEBUG、INFO、WARNING、ERROR
- **自动轮转**：单文件最大500MB，保留30天

### ⚡ 性能优化

1. **分批处理**：按日期分批拉取，避免单次请求过大
2. **Empty Dates保护**：记录无数据日期，避免重复API调用
3. **增量更新**：支持断点续传，提高更新效率
4. **最近日期刷新**：确保最新数据的及时性和准确性

## 数据覆盖范围

### StockInfoArchiver - 股票信息数据（18个数据源）

#### 1. 基础信息数据
- **股票基本信息** (`stock_basic`)：股票列表、上市状态、行业分类等
- **股票曾用名** (`stock_namechange`)：历史名称变更记录

#### 2. 行情交易数据
- **A股日线行情** (`stock_daily`)：开高低收、成交量价等基础行情数据
- **大宗交易** (`stock_block_trade`)：大额交易记录、买卖双方营业部信息
- **融资融券汇总** (`stock_margin`)：市场整体杠杆水平和做空情绪

#### 3. 财务基本面数据
- **利润表** (`stock_income`)：78个字段，完整的盈利能力数据
- **现金流量表** (`stock_cashflow`)：92个字段，现金流健康状况
- **资产负债表** (`stock_balancesheet`)：131个字段，财务结构分析
- **财务指标** (`stock_fina_indicator`)：181个字段，综合财务分析指标
- **主营业务构成** (`stock_fina_mainbz`)：业务结构和收入来源分析
- **财务审计意见** (`stock_fina_audit`)：财务数据质量保证

#### 4. 业绩预期数据
- **业绩预告** (`stock_forecast`)：管理层业绩指引和预期
- **业绩快报** (`stock_express`)：快速业绩披露数据

#### 5. 价值分配数据
- **分红送股** (`stock_dividend`)：股息政策和股本变动

#### 6. 公司治理数据
- **股东增减持** (`stock_holder_trade`)：内部人士交易行为和持股变化

#### 7. 主题投资数据（开盘啦数据源）
- **题材库** (`stock_kpl_concept`)：市场热点题材和涨停数量
- **题材成分** (`stock_kpl_concept_cons`)：题材与个股的映射关系
- **榜单数据** (`stock_kpl_list`)：涨停、炸板、跌停等短线交易数据

### CBArchiver - 可转债数据（7个数据源）
- **可转债基本信息** (`cb_basic`)：转债基础资料
- **可转债发行数据** (`cb_issue`)：发行条款和发行信息
- **可转债赎回信息** (`cb_call`)：赎回公告和赎回条件
- **可转债行情** (`cb_daily`)：转债日线交易数据
- **可转债转股结果** (`cb_share`)：转股统计数据
- **债券回购日行情** (`repo_daily`)：回购市场行情
- **债券大宗交易** (`bond_blk`)：债券大额交易记录

### BasicArchiver - 基础数据（1个数据源）
- **交易日历** (`trade_cal`)：交易日、休市日信息

## 智能更新机制

### 1. Empty Dates 管理
- **智能过滤**：自动记录无数据的日期，避免重复请求
- **最近日期保护**：最近5个交易日/日历日不进入empty_dates，防止数据延迟
- **自动恢复**：当之前empty的日期重新有数据时，自动清理

### 2. 增量更新策略
- **3日数据刷新**：增量模式下自动删除并重新拉取最近3天数据
- **5日保护机制**：最近5天数据不会被标记为empty
- **VIP接口优化**：使用bulk VIP接口提高数据拉取效率

### 3. 日期类型处理
- **交易日期** (`trade_dates`)：用于行情数据、大宗交易等
- **日历日期** (`calendar_dates`)：用于公告数据、财务数据等
- **报告期** (`report_periods`)：用于财务报表数据

## 使用方法

### 1. 环境准备
```bash
# 克隆项目
git clone <repository_url>
cd TushareArchiverPublic

# 安装依赖
pip install -r requirements.txt

# 配置数据库和Token
vim config.py
```

### 2. 运行数据更新

#### 股票数据更新
```bash
python StockInfoArchiver/StockInfoDailyArchiver.py
```

#### 可转债数据更新
```bash
python CBArchiver/CBDailyArchiver.py
```

#### 基础数据更新
```bash
python BasicArchiver/BasicDailyArchiver.py
```

### 3. 自定义更新
```python
# 单独更新某个数据源
from StockInfoArchiver.StockInfoDailyArchiver import Stock_IncomeUpdater

updater = Stock_IncomeUpdater()
updater.update(mode='increment', dates=['20240101', '20240102'])
updater.close()
```
## 数据质量保证

### 1. 字段完整性
- 所有字段严格按照Tushare官方文档定义(受MySQL关键字限制leading -> leader)
- 自动处理NaN、NaT等缺失值
- 数据类型自动转换和验证

### 2. 错误处理
- 网络异常重试机制
- API限流保护
- 详细错误日志记录

### 3. 数据一致性
- 主键约束确保数据唯一性
- 外键关联保证数据完整性
- 定期数据一致性检查

## 性能优化

### 1. 分批处理
- 大表强制分批拉取，避免超时
- 合理的批次大小控制内存使用
- 进度条显示处理进度

### 2. 数据库优化
- 合理的索引设计
- 批量插入优化
- 连接池管理

### 3. API调用优化
- 优先使用VIP接口减少调用次数
- 智能重试和限流控制
- 缓存机制减少重复请求

## 常见配置问题

### 数据库连接问题
- 确保 MySQL 服务已启动
- 检查用户名、密码、主机地址是否正确
- 确保用户有足够权限创建表和插入数据

### Tushare API 问题
- 确认 Token 是否有效
- 检查账户积分是否足够
- 验证网络连接是否正常

### 环境问题
- 确保所有依赖包已正确安装
- 验证 Python 环境是否激活
- 检查路径配置是否正确

### 脚本路径问题
- 确保已将 `PATH_TO_YOUR_PROJECT_ROOT` 替换为实际项目路径
- 检查路径中是否包含特殊字符或空格
- 确保路径格式正确（Windows 使用反斜杠）

## 自定义配置

### 数据库名称
如需使用自定义数据库名称，请修改：
- `config.py` 中的 `MYSQL_DATABASE`
- 确保数据库已创建

### conda 环境名称
如需使用自定义 conda 环境名称，请修改：
- `scripts/run_simple.bat` 中的环境名
- `scripts/run_tushare_archiver.bat` 中的环境名

### 日志配置
日志文件默认保存在 `logs/` 目录，可在 `main.py` 中自定义：
- 日志级别
- 文件大小限制
- 保留天数

## 系统要求

### 硬件要求
- **内存**：建议8GB以上（处理大量数据时）
- **磁盘空间**：建议预留10GB以上空间存储数据
- **网络**：稳定的互联网连接

### 版本要求
- **Python**：3.12+
- **MySQL** 8.0+

## 扩展开发

### 1. 添加新数据源
```python
class New_DataUpdater(StockInfoBaseUpdater):
    def __init__(self):
        super().__init__()
        self.table_name = 'new_data'
        self.columns = ['field1', 'field2', 'field3']
        self.create_table(self._get_create_sql())
    
    def update(self, mode='full', dates=None):
        # 实现数据更新逻辑
        pass
```

### 2. 自定义更新策略
- 修改update_mode.py配置文件
- 实现自定义的日期生成逻辑
- 添加特定的数据处理逻辑

### 3. 集成其他数据源
- 扩展BaseUpdater类支持其他API
- 实现统一的数据接口
- 添加数据源间的关联分析

## 免责声明

本项目仅用于学习和研究目的，请勿使用本系统进行投资决策。

## License
All the codes in this project are under the MIT License.

## 参考文档

- [Tushare官方文档](https://tushare.pro/document/2)
- [开盘啦APP](https://www.kaipanla.com/)
- [MySQL官方文档](https://dev.mysql.com/doc/)

---

如有问题或建议，欢迎提交 Issue 或 Pull Request。
