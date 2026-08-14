# 运行脚本  spark-submit f1_ods.py

#导入模块,lit
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.functions import col
# 创建SparkSession对象
spark = SparkSession.builder.appName("F1 ODS").getOrCreate()
# 读取CSV文件
df_order1 = spark.read.csv("file:///home/mszy/practices/data/orders_20240701.csv", header=False, inferSchema=True)
df_order2 = spark.read.csv("file:///home/mszy/practices/data/orders_20240702.csv", header=False, inferSchema=True)
df_order3 = spark.read.csv("file:///home/mszy/practices/data/orders_20240703.csv", header=False, inferSchema=True)
df_users = spark.read.csv("file:///home/mszy/practices/data/users.csv", header=False, inferSchema=True)
df_products = spark.read.csv("file:///home/mszy/practices/data/products.csv", header=False, inferSchema=True)
# 添加列名
df_order1 = df_order1.withColumn("order_date", lit("2024-07-01"))
df_order2 = df_order2.withColumn("order_date", lit("2024-07-02"))
df_order3 = df_order3.withColumn("order_date", lit("2024-07-03"))

# 合并订单数据
df_orders = df_order1.union(df_order2).union(df_order3)
# 添加字段名
df_orders = df_orders.toDF("order_id", "user_id", "product_id", "quantity", "order_price", 'order_date')
df_users = df_users.toDF("user_id", "user_name", "gender", "age", "city", 'user_date')
df_products = df_products.toDF("product_id", "product_name", "category", "product_price")
# 显示数据
df_users.show()
df_products.show()
df_orders.show()
# 写parquet文件
df_orders.write.mode("overwrite").parquet("file:///home/mszy/practices/warehouse/ods/orders")
df_users.write.mode("overwrite").parquet("file:///home/mszy/practices/warehouse/ods/users")
df_products.write.mode("overwrite").parquet("file:///home/mszy/practices/warehouse/ods/products")

# orders JOIN users（on user_id）拿到用户维度
df_orders_users = df_orders.join(df_users, on="user_id", how="left")
df_orders_users = df_orders_users.select("order_id", "user_id", "product_id", "user_name", "gender", "age", "city", "quantity", "order_price", "user_date", "order_date")
df_orders_users.show()

# orders JOIN users JOIN products（on product_id）拿到产品维度
df_orders_users_products = df_orders_users.join(df_products, on="product_id", how="left")
df_orders_users_products.show()

# 计算总价
df_orders_users_products = df_orders_users_products.withColumn(
    "total_amount",
    col("quantity") * col("order_price")
)

df_orders_users_products = df_orders_users_products.select(
    "order_id", "user_id", "product_id", "quantity", "order_price",
    "order_date", "user_name", "gender", "age", "city", "product_name",
    "category", "total_amount"
)

df_orders_users_products.show()


# 写parquet文件
df_orders_users_products.write.mode("overwrite").parquet("file:///home/mszy/practices/warehouse/dwd/dwd_orders_detail") 
