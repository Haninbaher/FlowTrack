from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("flowtrack_batch").getOrCreate()

df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://postgres:5432/flowtrack") \
    .option("dbtable", "raw_supply_chain") \
    .option("user", "flowtrack") \
    .option("password", "flowtrack") \
    .option("driver", "org.postgresql.Driver") \
    .load()

dim_customers = df.select(
    df["Customer Id"].alias("customer_id"),
    df["Customer Fname"].alias("customer_fname"),
    df["Customer Lname"].alias("customer_lname"),
    df["Customer City"].alias("customer_city"),
    df["Customer Country"].alias("customer_country"),
    df["Customer Segment"].alias("customer_segment")
).dropDuplicates(["customer_id"])

dim_products = df.select(
    df["Product Card Id"].alias("product_id"),
    df["Product Name"].alias("product_name"),
    df["Product Category Id"].alias("product_category_id"),
    df["Product Price"].alias("product_price")
).dropDuplicates(["product_id"])

fct_orders = df.select(
    df["Order Id"].alias("order_id"),
    df["Order Customer Id"].alias("customer_id"),
    df["order date (DateOrders)"].alias("order_date"),
    df["Order Item Product Price"].alias("order_item_product_price"),
    df["Order Item Quantity"].alias("order_item_quantity"),
    df["Sales"].alias("sales"),
    df["Delivery Status"].alias("delivery_status"),
    df["Late_delivery_risk"].alias("late_delivery_risk"),
    df["Days for shipping (real)"].alias("days_for_shipping_real"),
    df["Days for shipment (scheduled)"].alias("days_for_shipment_scheduled"),
    df["Shipping Mode"].alias("shipping_mode"),
    df["Order Region"].alias("order_region"),
    df["Order Country"].alias("order_country")
)

for table_name, dataframe in [
    ("dim_customers", dim_customers),
    ("dim_products", dim_products),
    ("fct_orders", fct_orders),
]:
    dataframe.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/flowtrack") \
        .option("dbtable", table_name) \
        .option("user", "flowtrack") \
        .option("password", "flowtrack") \
        .option("driver", "org.postgresql.Driver") \
        .option("truncate", "true") \
        .mode("overwrite") \
        .save()

spark.stop()
