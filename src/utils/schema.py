from pyspark.sql.types import *

dim_product_Schema = StructType(
    [
        StructField("row_number", LongType(), False),
        StructField("product_key", IntegerType(), False),
        StructField("product_id", StringType(), False),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("brand", StringType(), True),
        StructField("unit_price", DecimalType(10, 2), True),
        StructField("last_updt_col", TimestampType(), False),
    ]
)

dim_customer_schema = StructType(
    [
        StructField("row_number", LongType(), False),
        StructField("customer_key", IntegerType(), False),
        StructField("customer_id", StringType(), False),
        StructField("full_name", StringType(), False),
        StructField("city", StringType(), False),
        StructField("is_loyalty_member", BooleanType(), False),
        StructField("last_updt_col", TimestampType(), False),
    ]
)

dim_store_schema = StructType(
    [
        StructField("row_number", LongType(), False),
        StructField("store_key", IntegerType(), False),
        StructField("store_id", StringType(), False),
        StructField("store_name", StringType(), False),
        StructField("region", StringType(), False),
        StructField("store_manager", BooleanType(), False),
        StructField("last_updt_col", TimestampType(), False),
    ]
)


fact_sales_schema = StructType(
    [
        StructField("row_number", LongType(), False),
        StructField("sales_key", IntegerType(), False),
        StructField("quantity_sold", IntegerType(), False),
        StructField("sale_amount_usd", DoubleType(), False),
        StructField("cost_amount_usd", DoubleType(), False),
        StructField("product_key", IntegerType(), False),
        StructField("sale_datetime", TimestampType(), False),
        StructField("customer_key", IntegerType(), False),
        StructField("store_key", IntegerType(), False),
    ]
)


application_schema = {
    "dim_product_schema": dim_product_Schema,
    "fact_sales_schema": fact_sales_schema,
    "dim_store_schema": dim_store_schema,
    "dim_customer_schema": dim_customer_schema,
}
