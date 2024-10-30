# Databricks notebook source
# MAGIC %md 
# MAGIC # Library Import

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *

# COMMAND ----------

pip install pymongo

# COMMAND ----------

# MAGIC %md 
# MAGIC # Establish Connection

# COMMAND ----------

from pymongo import MongoClient

client = MongoClient("mongodb+srv://llmate-assignment-orders-read-only:cl89aTfXGb4J@order-data.sco2q.mongodb.net/?tls=true")

# Test connection
try:
    client.admin.command('ping')
    print("Connected to MongoDB!")
except Exception as e:
    print(f"Connection failed: {e}")


# COMMAND ----------

connectionString='mongodb+srv://poojaggunagi19:rthlXogUzrxSIuiK@cluster0.e6d2i.mongodb.net/'


# COMMAND ----------

# MAGIC %md 
# MAGIC # Bronze Layer

# COMMAND ----------

# MAGIC %md ## Ingest Raw Data - Orders

# COMMAND ----------


order_df = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("database", 'Sample').option("collection", 'Orders').option("spark.mongodb.input.uri", connectionString).load()
display(order_df)

# COMMAND ----------

# MAGIC %md ## Ingest Raw data - Customers

# COMMAND ----------

cust_df = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("database", 'Sample').option("collection", 'Customers').option("spark.mongodb.input.uri", connectionString).load()
display(cust_df)

# COMMAND ----------

# MAGIC %md ## Store Raw Data - Orders

# COMMAND ----------

order_df.write.format("delta").mode("overwrite").save("/FileStore/tables/Bronze_Load/bronze_orders/")

# COMMAND ----------

# MAGIC %md ## Store Raw Data - Customers

# COMMAND ----------

cust_df.write.mode("overwrite").format("delta").save("/FileStore/tables/Bronze_Load/bronze_customers/")

# COMMAND ----------

# MAGIC %md 
# MAGIC # Silver Layer

# COMMAND ----------

order_slvr_df=spark.read.format("delta").load("/FileStore/tables/Bronze_Load/bronze_orders/")
order_slvr_df.display()

# COMMAND ----------

# MAGIC %md ## Data Enrichment - Orders

# COMMAND ----------

order_slvr_df2=order_slvr_df.withColumn("items",explode("items"))
order_slvr_df2.display()

# COMMAND ----------


order_slvr_df3=order_slvr_df2.select("order_id","customer_id",col("items.product_id").alias("Product_id"),col("items.quantity").alias("quantity"),col("items.unit_price").alias("unit_price"),"order_date","shipping_date","status","total_amount")


# COMMAND ----------

order_slvr_df3.display()

# COMMAND ----------

# MAGIC %md ## Data Cleaning - Orders

# COMMAND ----------

order_slvr_df4=order_slvr_df3.withColumns({"order_date":col("order_date").cast(DateType()), "shipping_date":col("shipping_date").cast(DateType()), "item_count":sum("quantity").over(Window.orderBy(lit("None")))})
order_slvr_df4=order_slvr_df4.fillna(0)
order_slvr_df5=order_slvr_df4.dropDuplicates()

order_slvr_df5.display()


# COMMAND ----------

# MAGIC %md ## Data Enrichment - customers

# COMMAND ----------

sil_cust_df=spark.read.format("delta").load("/FileStore/tables/Bronze_Load/bronze_customers")
sil_cust_df.display()

# COMMAND ----------


sil_cust_df2=sil_cust_df.select("customer_id","email","first_name","last_name","phone",col("address.street").alias("street"),col("address.city").alias("city"),col("address.state").alias("state"),col("address.zip").alias("zip"),"registration_date")
sil_cust_df2.display()

# COMMAND ----------

# MAGIC %md ## Data Cleaning - Customers

# COMMAND ----------

sil_cust_df3=sil_cust_df2.withColumn("registration_date", col("registration_date").cast(DateType()))
sil_cust_df3=sil_cust_df3.fillna(0)
sil_cust_df4=sil_cust_df3.dropDuplicates()
sil_cust_df4.display()

# COMMAND ----------

# MAGIC %md ## Data Integration

# COMMAND ----------

joined_df=order_slvr_df5.join(sil_cust_df4, "customer_id", "inner").select("")

joined_df.display()

# COMMAND ----------

joined_df2=joined_df.withColumn("Customer_name", concat_ws(" ", col("first_name"), col("last_name")))
joined_df2.display()

# COMMAND ----------

# MAGIC %md ## Silver Layer Data Load - Joined Table

# COMMAND ----------

joined_df2.write.format("delta").mode("overwrite").save("/FileStore/tables/Silver_Load/Customer_Order/")

# COMMAND ----------

# MAGIC %md ## Silver Layer Data Load - Silver_Orders

# COMMAND ----------

order_slvr_df5.write.format("delta").mode("overwrite").save("/FileStore/tables/Silver_Load/silver_orders/")

# COMMAND ----------

# MAGIC %md ## Silver Layer Data Load - silver_customers

# COMMAND ----------

sil_cust_df4.write.format("delta").mode("overwrite").save("/FileStore/tables/Silver_Load/silver_customers/")

# COMMAND ----------

# MAGIC %md # Gold Layer

# COMMAND ----------

gold_order_df=spark.read.format("delta").load("/FileStore/tables/Silver_Load/silver_orders/")
gold_order_df.display()

# COMMAND ----------

gold_order_df2=gold_order_df.withColumn("total_sales", sum(col("total_amount")).over(Window.orderBy(lit("None"))))
gold_order_df2.display()

# COMMAND ----------

# MAGIC %md ## Order Analytic table creation

# COMMAND ----------

total_revenue = gold_order_df2.agg(sum("total_amount").alias("total_revenue"))
total_quantity_sold = gold_order_df2.agg(sum("quantity").alias("total_quantity_sold"))
average_order_value = gold_order_df2.groupBy("order_id").agg(sum("total_amount").alias("order_total")) \
                               .agg(avg("order_total").alias("average_order_value"))
average_quantity_per_order = gold_order_df2.groupBy("order_id").agg(sum("quantity").alias("order_quantity")) \
                                      .agg(avg("order_quantity").alias("average_quantity_per_order"))
total_orders = gold_order_df2.select("order_id").distinct().count()
total_items_per_order = gold_order_df2.groupBy("order_id").agg(sum("item_count").alias("total_items")) \
                                 .agg(sum("total_items").alias("total_items_all_orders"))
average_fulfillment_time = gold_order_df2.withColumn("fulfillment_time", datediff(col("shipping_date"), col("order_date"))) \
                                    .agg(avg("fulfillment_time").alias("average_fulfillment_time"))
order_status_analysis = gold_order_df2.groupBy("status").agg(count("*").alias("shipment_status_count"))

# COMMAND ----------

order_kpi_df = total_revenue \
    .crossJoin(total_quantity_sold) \
    .crossJoin(average_order_value) \
    .crossJoin(average_quantity_per_order) \
    .withColumn("total_orders", lit(total_orders)) \
    .crossJoin(total_items_per_order) \
    .crossJoin(average_fulfillment_time)

# Join with order status analysis to add order status counts
final_kpi_df = order_kpi_df.crossJoin(order_status_analysis)

# COMMAND ----------

final_kpi_df.display()

# COMMAND ----------

# MAGIC %md ## Gold Layer Table -Customer

# COMMAND ----------

gold_cust_df=spark.read.format("delta").load("/FileStore/tables/Silver_Load/silver_customers/")
gold_cust_df.display()

# COMMAND ----------

total_customers = gold_cust_df.agg(countDistinct("customer_id").alias("total_customers"))

# Calculate customer tenure (in days) and get average tenure
tenure_df = gold_cust_df.withColumn("tenure", datediff(current_date(), col("registration_date")))
average_tenure = tenure_df.agg(avg("tenure").alias("average_tenure"))

# Earliest and Latest Registration Date
registration_period = gold_cust_df.agg(min("registration_date").alias("first_registration_date"),
                                       max("registration_date").alias("latest_registration_date"))

# Joining all KPI calculations into a single DataFrame
customer_kpi_df = total_customers \
    .crossJoin(average_tenure) \
    .crossJoin(registration_period)

# COMMAND ----------

customer_kpi_df.display()

# COMMAND ----------

# MAGIC %md ## gold Layer Sales  metrics
# MAGIC

# COMMAND ----------

total_sales = gold_order_df2.agg(sum("total_sales").alias("total_sales"))

total_orders = gold_order_df2.agg(countDistinct("order_id").alias("total_orders"))

avg_order_value = gold_order_df2.agg((sum("total_sales") / countDistinct("order_id")).alias("average_order_value"))

total_quantity_sold = gold_order_df2.agg(sum("quantity").alias("total_quantity_sold"))

avg_items_per_order = gold_order_df2.agg((sum("item_count") / countDistinct("order_id")).alias("average_items_per_order"))

total_revenue_by_product = gold_order_df2.groupBy("product_id").agg(sum("total_sales").alias("total_revenue_by_product"))

order_fulfillment_rate = gold_order_df2.agg(
    (count(when(col("status").isin("Shipped", "Completed"), 1)) / count("*") * 100).alias("order_fulfillment_rate")
)

avg_shipping_time = gold_order_df2.filter(col("status") == "Shipped").agg(
    avg(datediff(col("shipping_date"), col("order_date"))).alias("average_shipping_time")
)

# Collect all KPIs into a single DataFrame
sales_metrics_df = total_sales \
    .crossJoin(total_orders) \
    .crossJoin(avg_order_value) \
    .crossJoin(total_quantity_sold) \
    .crossJoin(avg_items_per_order) \
    .crossJoin(order_fulfillment_rate) \
    .crossJoin(avg_shipping_time)

# Join with total revenue by product (optional: save separately if needed)
sales_metrics_df = sales_metrics_df.join(total_revenue_by_product, how="left")



# COMMAND ----------

sales_metrics_df.display()

# COMMAND ----------

final_kpi_df.display()

# COMMAND ----------

# MAGIC %md ## Gold Layer Table Load

# COMMAND ----------

sales_metrics_df.write.format("delta").mode("overwrite").save("/FileStore/tables/Gold_Load/gold_sales_metrics/")

# COMMAND ----------

final_kpi_df.write.format("delta").mode("overwrite").save("/FileStore/tables/Gold_Load/gold_orders_analytics/")
