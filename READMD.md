电商数据仓库分层项目
基于 PySpark 构建电商数仓，按 ODS → DWD → DWS → ADS 四层分层架构组织数据，从原始 CSV 到最终业务报表的完整 ETL 流程，使用 Parquet 列式存储。

数仓架构
数据源 (CSV)  →  ODS 贴源层  →  DWD 明细层  →  DWS 汇总层  →  ADS 应用层
   5个文件        3张Parquet     1张订单宽表     3张主题汇总表    5张业务报表
                  (原样导入)     (JOIN+维度退化)  (groupBy聚合)   (spark.sql查询)
层级	职责	数据量	核心操作
ODS 贴源层	原始数据原样落库	users 50行 / products 30行 / orders 198行	read.csv + toDF + write.parquet
DWD 明细层	清洗+维度退化，生成宽表	198行 × 13列	JOIN users + JOIN products + 计算total_amount
DWS 汇总层	按主题+时间粒度轻度聚合	user_daily / city_daily / product_daily	groupBy + agg(sum, count)
ADS 应用层	最终业务报表	5张报表	spark.sql 二次查询（含窗口函数）
技术栈
技术	版本	说明
Spark	3.5.9	分布式计算引擎
PySpark	3.5.9	Python API
Java	8	运行时环境
Python	3.12.13	miniconda 环境
Parquet	-	列式存储格式（数仓标准）
WSL2 + Ubuntu	-	Linux 开发环境
项目结构
practices/
├── README.md                    # 项目说明（本文件）
├── f1_ods.py                     # ODS贴源层 + DWD明细层（数据导入+JOIN生成宽表）
├── f3_dws.py                     # DWS汇总层（3张主题汇总表）
├── f4_ADS.py                     # ADS应用层（5张业务报表）
├── data/                         # 原始数据集
│   ├── users.csv                 # 用户表（50条）
│   ├── products.csv              # 商品表（30条）
│   ├── orders_20240701.csv       # 订单表-7月1日（61条）
│   ├── orders_20240702.csv       # 订单表-7月2日（68条）
│   └── orders_20240703.csv       # 订单表-7月3日（69条）
└── warehouse/                    # 数仓分层存储（Parquet）
    ├── ods/                      # 贴源层
    │   ├── users/
    │   ├── products/
    │   └── orders/
    ├── dwd/                      # 明细层
    │   └── dwd_orders_detail/    # 订单明细宽表（13列）
    ├── dws/                      # 汇总层
    │   ├── user_daily/           # 用户×日期 消费汇总
    │   ├── city_daily/           # 城市×日期 消费汇总
    │   └── product_daily/        # 商品×日期 销量汇总
    └── ads/                      # 应用层
        ├── each_day_sales/       # 每日销售额趋势
        ├── city_sales_top10/     # 城市消费力 TOP10
        ├── users_sales_top10/    # 用户消费榜 TOP10
        ├── product_sales_top10/  # 商品销量 TOP10
        └── category_top1/        # 各分类销量冠军
数据集说明
users（用户表）
字段	类型	说明
user_id	INT	用户ID
user_name	STRING	用户名
gender	STRING	性别
age	INT	年龄
city	STRING	城市
user_date	STRING	注册日期
products（商品表）
字段	类型	说明
product_id	INT	商品ID
product_name	STRING	商品名称
category	STRING	品类
product_price	DOUBLE	商品单价
orders（订单表，3天合并）
字段	类型	说明
order_id	INT	订单ID
user_id	INT	用户ID
product_id	INT	商品ID
quantity	INT	购买数量
order_price	DOUBLE	订单单价
order_date	STRING	订单日期（lit添加）
各层实现
ODS 贴源层（f1_ods.py 上半部分）
读取 5 个 CSV 文件，inferSchema=True 推断类型
3 天 orders 用 lit() 添加日期列 + union() 合并
toDF() 设置规范列名（orders 用 order_price、products 用 product_price 避免冲突）
写入 Parquet 到 warehouse/ods/{users,products,orders}/
DWD 明细层（f1_ods.py 下半部分）
orders JOIN users（left）拿到用户维度（user_name, gender, age, city）
再 JOIN products（left）拿到商品维度（product_name, category）
withColumn('total_amount', quantity * order_price) 计算订单金额
select 13 列组成订单明细宽表
写入 Parquet 到 warehouse/dwd/dwd_orders_detail/
DWS 汇总层（f3_dws.py）
从 DWD 宽表按主题+日期聚合，生成 3 张汇总表：

表名	分组键	度量
dws_user_daily	user_id, user_name, city, order_date	total_orders, total_amount
dws_city_daily	city, order_date	total_orders, total_amount
dws_product_daily	product_id, product_name, category, order_date	total_qty, total_amount
使用 groupBy().agg(count().alias(), round(sum(), 2).alias()) 组合聚合。

ADS 应用层（f4_ADS.py）
从 DWS 二次查询出业务报表，全部用 spark.sql() + 临时视图：

报表	数据源	核心SQL
每日销售额趋势	dws_city_daily	GROUP BY order_date, SUM(total_amount)
城市消费力 TOP10	dws_city_daily	GROUP BY city, SUM DESC LIMIT 10
用户消费榜 TOP10	dws_user_daily	GROUP BY user_id, SUM DESC LIMIT 10
商品销量 TOP10	dws_product_daily	GROUP BY product_id, SUM(total_qty) DESC
各分类销量冠军	dws_product_daily	ROW_NUMBER() OVER (PARTITION BY category ORDER BY SUM(total_qty) DESC) + WHERE rn=1
Spark 技术点覆盖
技术点	位置
SparkSession 创建	所有脚本
read.csv + inferSchema	f1_ods.py
lit() 添加常量列	f1_ods.py
union() 合并 DataFrame	f1_ods.py
toDF() 重命名列	f1_ods.py
withColumn 计算派生列	f1_ods.py
join(left) 左连接	f1_ods.py
select 选列	f1_ods.py
write.parquet 列式存储	所有脚本
read.parquet 读取列式存储	f3_dws.py, f4_ADS.py
groupBy + agg 聚合	f3_dws.py
alias() 列别名	f3_dws.py
round() 精度处理	f3_dws.py, f4_ADS.py
orderBy 多列排序	f3_dws.py
createOrReplaceTempView 临时视图	f4_ADS.py
spark.sql() 执行SQL	f4_ADS.py
ROW_NUMBER() OVER 窗口函数	f4_ADS.py
PARTITION BY 分区排名	f4_ADS.py
运行方式
bash
# 1. 启动 Spark（确保 WSL 下 Spark 环境已配置）
# 2. 按顺序执行（后一层依赖前一层输出）

spark-submit f1_ods.py    # 生成 ODS + DWD 层数据
spark-submit f3_dws.py    # 生成 DWS 层数据
spark-submit f4_ADS.py    # 生成 ADS 层报表
数仓分层的价值
复用：DWS 汇总表可被多个 ADS 报表复用，避免重复计算
解耦：原始数据变更只影响 ODS，维度表变更只影响 DWD，下游自动适配
性能：每层预聚合后数据量递减，ADS 查询只扫几十行而非原始 198 行订单
溯源：任何报表数据都能逆向追溯到 DWS → DWD → ODS → 原始 CSV
