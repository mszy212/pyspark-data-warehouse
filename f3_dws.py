# 运行脚本 
# spark-submit f3_dws.py
# 导入模块  还有sum，count，avg，max，min
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.functions import col
from pyspark.sql.functions import sum,count,avg,max,min,round

# 创建SparkSession对象
spark = SparkSession.builder.appName("F3 DWS").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# 创建数据源
df = spark.read.parquet("file:///home/mszy/practices/warehouse/dwd/dwd_orders_detail")
# df.show()
#  创建DWS层数据 某个用户在某个日期下的订单信息
dws_user_daily = df.groupBy("user_id", "user_name", "city", "order_date").agg(
    count("order_id").alias("total_orders"),
    round(sum("total_amount"), 2).alias("total_amount")
)
dws_user_daily = dws_user_daily.orderBy( "order_date","total_amount", ascending=[True, False])
dws_user_daily.show()
dws_user_daily.write.mode("overwrite").parquet("file:///home/mszy/practices/warehouse/dws/user_daily")


# 创建DWS层数据 某个城市在某个日期下的订单信息
dws_city_daily = df.groupBy('city', 'order_date').agg(
    count('order_id').alias('total_orders'),
    round(sum('total_amount'), 2).alias('total_amount')
)
dws_city_daily = dws_city_daily.orderBy( "order_date","total_amount", ascending=[True, False])
dws_city_daily.show()
dws_city_daily.write.mode("overwrite").parquet("file:///home/mszy/practices/warehouse/dws/city_daily")

#  创建DWS层数据 某个商品在某个日期下的订单信息
dws_product_daily = df.groupBy('product_id', 'product_name', 'category', 'order_date').agg(
    sum('product_id').alias('total_qty'),
    round(sum('total_amount'), 2).alias('total_amount')
)
dws_product_daily = dws_product_daily.orderBy( "order_date","total_amount", ascending=[True, False])
dws_product_daily.show()
dws_product_daily.write.mode("overwrite").parquet("file:///home/mszy/practices/warehouse/dws/product_daily")