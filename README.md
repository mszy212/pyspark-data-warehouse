## 业务问题

模拟一家电商平台的日常经营分析场景：

- 运营总监每周要问：**这个月哪些城市卖得最好？哪些商品该补货？哪些用户在流失？**
- 分析师临时跑 SQL 费时，且原始数据散落 5 个 CSV 文件，跨日订单还得分文件 union
- 目标：把 5 张原始表 → 整理成 4 层数仓 → 一键产出 5 张可直接发给老板看的业务报表


## 业务结论（最近一次运行，基于 3 天 198 条订单）

### 每日销售趋势
- 7/1：¥742,723
- 7/2：¥1,210,630（峰值，比 7/1 高 63%）
- 7/3：¥893,506（比 7/2 回落 26%）
- 三日累计 **¥2,846,859**，**7/2 是明显的销售高峰**

### 头部集中度
- **城市 TOP3**（3 日累计）：上海 ~¥516K、温州 ~¥459K、北京 ~¥393K，合计约 **48%** 的全平台销售额
- **商品 TOP10**（按销量）：笔记本/计算器/得力笔套装/李宁背包/百草味零食/安踏卫衣/九阳豆浆机/Nike运动鞋/Python编程/Hive权威指南，**10 个 SKU 占全站一半以上销量**

### 异常发现
- **温州 7/2 单日 ¥418,477**，远超其他城市单点，是当日的"销冠城市"
- **北京 7/1 仅 ¥5,661**，单日明显异常低（仅 9 单），建议核查当日数据/支付/物流
- **用户黄霞**（id=17）单人贡献 ¥223,089，是用户 TOP10 里**唯一突破 20 万**的，占头部 10 人合计的 **14.7%**

### 分类冠军（销量第一的 SKU）
- 图书文具：笔记本 261 件（学生刚需）
- 服装鞋包：李宁背包 180 件
- 食品生鲜：百草味零食 176 件
- 家用电器：九阳豆浆机 150 件
- 电脑办公：机械键盘 112 件
- 手机数码：AirPods Pro 40 件



## 给业务的建议

1. **温州深挖**：温州 7/2 单日 ¥418K 异常高峰，可能有大客户或促销活动。建议复盘当日渠道来源，复制经验到其他城市
2. **北京补漏**：北京 7/1 仅 ¥5,661 远低于均值，**核查当日数据完整性**或排查配送/支付故障
3. **VIP 客户运营**：用户黄霞（id=17）一人贡献 ¥223K，TOP10 用户合计 ¥1,512K（占 53%）。**这 10 个用户的留存直接决定业绩**，建议进入 VIP 通道、专属优惠
4. **文具囤货**：图书文具的"笔记本" 261 件是销冠 SKU，**安全库存优先保障**；办公品类（机械键盘/罗技鼠标）可作"凑单爆款"组合营销
5. **数据复用**：本套数仓分层可作为新业务（直播/会员体系）上线时的标准模板，**ADS 层 5 张报表可复用**到后续业务线





## 项目结构

```
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
```

## 数据集说明

### users（用户表）

| 字段 | 类型 | 说明 |
|------|------|------|
| user_id | INT | 用户ID |
| user_name | STRING | 用户名 |
| gender | STRING | 性别 |
| age | INT | 年龄 |
| city | STRING | 城市 |
| user_date | STRING | 注册日期 |

### products（商品表）

| 字段 | 类型 | 说明 |
|------|------|------|
| product_id | INT | 商品ID |
| product_name | STRING | 商品名称 |
| category | STRING | 品类 |
| product_price | DOUBLE | 商品单价 |

### orders（订单表，3天合并）

| 字段 | 类型 | 说明 |
|------|------|------|
| order_id | INT | 订单ID |
| user_id | INT | 用户ID |
| product_id | INT | 商品ID |
| quantity | INT | 购买数量 |
| order_price | DOUBLE | 订单单价 |
| order_date | STRING | 订单日期（lit添加）|

## 各层实现

### ODS 贴源层（f1_ods.py 上半部分）

- 读取 5 个 CSV 文件，`inferSchema=True` 推断类型
- 3 天 orders 用 `lit()` 添加日期列 + `union()` 合并
- `toDF()` 设置规范列名（orders 用 `order_price`、products 用 `product_price` 避免冲突）
- 写入 Parquet 到 `warehouse/ods/{users,products,orders}/`

### DWD 明细层（f1_ods.py 下半部分）

- orders JOIN users（left）拿到用户维度（user_name, gender, age, city）
- 再 JOIN products（left）拿到商品维度（product_name, category）
- `withColumn('total_amount', quantity * order_price)` 计算订单金额
- `select` 13 列组成订单明细宽表
- 写入 Parquet 到 `warehouse/dwd/dwd_orders_detail/`

### DWS 汇总层（f3_dws.py）

从 DWD 宽表按主题+日期聚合，生成 3 张汇总表：

| 表名 | 分组键 | 度量 |
|------|--------|------|
| dws_user_daily | user_id, user_name, city, order_date | total_orders, total_amount |
| dws_city_daily | city, order_date | total_orders, total_amount |
| dws_product_daily | product_id, product_name, category, order_date | total_qty, total_amount |

使用 `groupBy().agg(count().alias(), round(sum(), 2).alias())` 组合聚合。

### ADS 应用层（f4_ADS.py）

从 DWS 二次查询出业务报表，全部用 `spark.sql()` + 临时视图：

| 报表 | 数据源 | 核心SQL |
|------|--------|---------|
| 每日销售额趋势 | dws_city_daily | `GROUP BY order_date, SUM(total_amount)` |
| 城市消费力 TOP10 | dws_city_daily | `GROUP BY city, SUM DESC LIMIT 10` |
| 用户消费榜 TOP10 | dws_user_daily | `GROUP BY user_id, SUM DESC LIMIT 10` |
| 商品销量 TOP10 | dws_product_daily | `GROUP BY product_id, SUM(total_qty) DESC` |
| 各分类销量冠军 | dws_product_daily | `ROW_NUMBER() OVER (PARTITION BY category ORDER BY SUM(total_qty) DESC)` + `WHERE rn=1` |

## Spark 技术点覆盖

| 技术点 | 位置 |
|--------|------|
| SparkSession 创建 | 所有脚本 |
| read.csv + inferSchema | f1_ods.py |
| lit() 添加常量列 | f1_ods.py |
| union() 合并 DataFrame | f1_ods.py |
| toDF() 重命名列 | f1_ods.py |
| withColumn 计算派生列 | f1_ods.py |
| join(left) 左连接 | f1_ods.py |
| select 选列 | f1_ods.py |
| write.parquet 列式存储 | 所有脚本 |
| read.parquet 读取列式存储 | f3_dws.py, f4_ADS.py |
| groupBy + agg 聚合 | f3_dws.py |
| alias() 列别名 | f3_dws.py |
| round() 精度处理 | f3_dws.py, f4_ADS.py |
| orderBy 多列排序 | f3_dws.py |
| createOrReplaceTempView 临时视图 | f4_ADS.py |
| spark.sql() 执行SQL | f4_ADS.py |
| ROW_NUMBER() OVER 窗口函数 | f4_ADS.py |
| PARTITION BY 分区排名 | f4_ADS.py |

## 运行方式

```bash
# 1. 启动 Spark（确保 WSL 下 Spark 环境已配置）
# 2. 按顺序执行（后一层依赖前一层输出）

spark-submit f1_ods.py    # 生成 ODS + DWD 层数据
spark-submit f3_dws.py    # 生成 DWS 层数据
spark-submit f4_ADS.py    # 生成 ADS 层报表
```

## 数仓分层的价值

1. **复用**：DWS 汇总表可被多个 ADS 报表复用，避免重复计算
2. **解耦**：原始数据变更只影响 ODS，维度表变更只影响 DWD，下游自动适配
3. **性能**：每层预聚合后数据量递减，ADS 查询只扫几十行而非原始 198 行订单
4. **溯源**：任何报表数据都能逆向追溯到 DWS → DWD → ODS → 原始 CSV