# 运行脚本 
# spark-submit f4_ADS.py
# 导入模块
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.functions import col
from pyspark.sql.functions import *

# 创建SparkSession
spark = SparkSession.builder.appName("f4_ADS").enableHiveSupport().getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# 读取dws三个数据，并且创建视图
df_city = spark.read.parquet("file:///home/mszy/practices/warehouse/dws/city_daily")
df_city.createOrReplaceTempView("v_city")

df_product = spark.read.parquet("file:///home/mszy/practices/warehouse/dws/product_daily")
df_product.createOrReplaceTempView("v_product")

df_user_daily = spark.read.parquet("file:///home/mszy/practices/warehouse/dws/user_daily")
df_user_daily.createOrReplaceTempView("v_user_daily")
# 创建DWS层数据

# 任务一 ：统计每日销售额
each_day_sales = spark.sql('''
select order_date, round(sum(total_amount), 2) as sales_amount
from v_city
group by order_date
order by order_date


''')
# 任务二 ：统计每日城市销售额
city_sales_top10 = spark.sql('''
select order_date,city, round(sum(total_amount), 2) as sales_amount
from v_city
group by order_date, city
order by sales_amount desc
limit 10
''')

# 任务三 ：统计用户总销售额
users_sales_top10 = spark.sql('''
select user_id , user_name, round(sum(total_amount), 2) as sales_amount
from v_user_daily
group by user_id, user_name
order by sales_amount desc
limit 10 
''')

# 任务四 ：统计商品总销售额
product_sales_top10 = spark.sql('''
select product_id,product_name, round(sum(total_qty), 2) as sales_qty
from v_product
group by product_id, product_name
order by sales_qty desc
limit 10
''')

# 任务五 ：各种产品的销量冠军
category_top1 = spark.sql('''
select * from (
    select product_name, category, sum(total_qty) as sales_qty,
    row_number() over(partition by category order by sum(total_qty) desc) as rn
    from v_product
    group by product_name, category
) t
where rn = 1
order by sales_qty desc
''')




# 导出数据
each_day_sales.write.mode("overwrite").parquet("file:///home/mszy/practices/warehouse/ads/each_day_sales")
city_sales_top10.write.mode("overwrite").parquet("file:///home/mszy/practices/warehouse/ads/city_sales_top10")
users_sales_top10.write.mode("overwrite").parquet("file:///home/mszy/practices/warehouse/ads/users_sales_top10")
product_sales_top10.write.mode("overwrite").parquet("file:///home/mszy/practices/warehouse/ads/product_sales_top10")
category_top1.write.mode("overwrite").parquet("file:///home/mszy/practices/warehouse/ads/category_top1")
# 显示数据
df_city.show()
df_product.show()
df_user_daily.show()
each_day_sales.show()
city_sales_top10.show()
users_sales_top10.show()
product_sales_top10.show()
category_top1.show()
