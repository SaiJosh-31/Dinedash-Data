import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


ORDERS_PATH = spark.conf.get(
    "dinedash.orders_path",
    "/Volumes/main/dinedash_bronze/landing/stream_data",
)
DIMENSIONS_PATH = spark.conf.get(
    "dinedash.dimensions_path",
    "/Volumes/main/dinedash_bronze/landing/dimensions",
)
UPDATES_PATH = spark.conf.get(
    "dinedash.updates_path",
    "/Volumes/main/dinedash_bronze/landing/user_updates.csv",
)

ORDER_SCHEMA = StructType(
    [
        StructField("order_id", StringType()),
        StructField("timestamp", StringType()),
        StructField("customer_id", StringType()),
        StructField("restaurant_id", StringType()),
        StructField("agent_id", StringType()),
        StructField("delivery_location_id", StringType()),
        StructField(
            "items_ordered",
            ArrayType(
                StructType(
                    [
                        StructField("item_id", StringType()),
                        StructField("item_name", StringType()),
                        StructField("price", StringType()),
                        StructField("quantity", StringType()),
                    ]
                )
            ),
        ),
        StructField("total_amount", StringType()),
        StructField("tip", StringType()),
        StructField("payment_method", StringType()),
        StructField("order_status", StringType()),
    ]
)


def _json_stream(path):
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{path}/_schemas")
        .option("cloudFiles.inferColumnTypes", "false")
        .option("cloudFiles.rescuedDataColumn", "_rescued_data")
        .schema(ORDER_SCHEMA)
        .load(path)
    )


def _csv_stream(filename):
    path = f"{DIMENSIONS_PATH}/{filename}"
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", f"{path}/_schemas")
        .option("header", "true")
        .option("multiLine", "true")
        .option("cloudFiles.rescuedDataColumn", "_rescued_data")
        .load(path)
    )


@dlt.table(name="bronze_orders", comment="Raw order JSON with audit metadata.")
def bronze_orders():
    return (
        _json_stream(ORDERS_PATH)
        .withColumn("ingestion_time", F.current_timestamp())
        .withColumn("source_file_name", F.input_file_name())
    )


def _bronze_dimension(name, filename):
    @dlt.table(name=name, comment=f"Raw {filename} dimension records.")
    def table():
        return (
            _csv_stream(filename)
            .withColumn("ingestion_time", F.current_timestamp())
            .withColumn("source_file_name", F.input_file_name())
        )

    return table


_bronze_dimension("bronze_customers", "dim_customers.csv")
_bronze_dimension("bronze_restaurants", "dim_restaurants.csv")
_bronze_dimension("bronze_menu_items", "dim_menu_items.csv")
_bronze_dimension("bronze_locations", "dim_locations.csv")
_bronze_dimension("bronze_delivery_agents", "dim_delivery_agents.csv")


@dlt.table(name="bronze_user_updates", comment="Raw customer CDC updates.")
def bronze_user_updates():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", f"{UPDATES_PATH}/_schemas")
        .option("header", "true")
        .option("multiLine", "true")
        .option("cloudFiles.rescuedDataColumn", "_rescued_data")
        .load(UPDATES_PATH)
        .withColumn("_commit_timestamp", F.current_timestamp())
        .withColumn("source_file_name", F.input_file_name())
    )


@dlt.expect_or_drop("valid_order_id", "order_id IS NOT NULL AND trim(order_id) <> ''")
@dlt.expect_or_drop("valid_customer_id", "customer_id RLIKE '^C[0-9]+$'")
@dlt.expect_or_drop("valid_restaurant_id", "restaurant_id RLIKE '^R[0-9]+$'")
@dlt.expect_or_drop("valid_timestamp", "order_timestamp IS NOT NULL")
@dlt.expect("non_negative_total", "total_amount >= 0")
@dlt.expect("non_negative_tip", "tip_amount >= 0")
@dlt.expect("known_status", "order_status IN ('delivered', 'cancelled', 'preparing', 'out_for_delivery', 'pending')")
@dlt.table(name="silver_orders", comment="Typed and deduplicated order headers.")
def silver_orders():
    parsed = (
        dlt.read_stream("bronze_orders")
        .withColumn("order_timestamp", F.to_timestamp("timestamp"))
        .withColumn("total_amount", F.col("total_amount").cast(DecimalType(18, 2)))
        .withColumn("tip_amount", F.col("tip").cast(DecimalType(18, 2)))
        .withColumn("order_status", F.lower(F.trim("order_status")))
    )
    return parsed.dropDuplicates(["order_id"]).drop("timestamp", "tip")


@dlt.expect_or_drop("valid_line_order_id", "order_id IS NOT NULL")
@dlt.expect_or_drop("valid_item_id", "item_id RLIKE '^I[0-9]+$'")
@dlt.expect_or_drop("positive_quantity", "quantity > 0")
@dlt.expect("non_negative_price", "unit_price >= 0")
@dlt.table(name="silver_order_items", comment="One row per item in an order.")
def silver_order_items():
    return (
        dlt.read_stream("silver_orders")
        .select("order_id", F.explode_outer("items_ordered").alias("item"), "order_timestamp")
        .select(
            "order_id",
            "order_timestamp",
            F.col("item.item_id").alias("item_id"),
            F.col("item.item_name").alias("item_name"),
            F.col("item.price").cast(DecimalType(18, 2)).alias("unit_price"),
            F.col("item.quantity").cast(IntegerType()).alias("quantity"),
        )
        .withColumn("line_amount", F.col("unit_price") * F.col("quantity"))
    )


@dlt.table(name="silver_customers", comment="Current customer dimension after CDC.")
def silver_customers():
    base = dlt.read_stream("bronze_customers").select(
        "customer_id", "name", "email", "dob", "signup_date", "location_id"
    )
    updates = dlt.read_stream("bronze_user_updates").select(
        "customer_id", "name", "email", "dob", "signup_date", "location_id", "action", "_commit_timestamp"
    )
    return base.unionByName(updates.drop("action", "_commit_timestamp"), allowMissingColumns=True)


def _silver_dimension(source, target, key, casts):
    @dlt.table(name=target, comment=f"Typed {source} dimension.")
    def table():
        result = dlt.read_stream(source)
        for column, data_type in casts.items():
            result = result.withColumn(column, F.col(column).cast(data_type))
        return result.dropDuplicates([key])

    return table


_silver_dimension("bronze_restaurants", "silver_restaurants", "restaurant_id", {"rating": DecimalType(3, 2), "delivery_fee": DecimalType(10, 2)})
_silver_dimension("bronze_menu_items", "silver_menu_items", "item_id", {"price": DecimalType(10, 2)})
_silver_dimension("bronze_locations", "silver_locations", "location_id", {"latitude": DecimalType(10, 6), "longitude": DecimalType(10, 6)})
_silver_dimension("bronze_delivery_agents", "silver_delivery_agents", "agent_id", {"rating": DecimalType(3, 2)})


@dlt.table(name="gold_fact_orders", comment="Order-level fact table.")
def gold_fact_orders():
    return dlt.read("silver_orders").select(
        "order_id", "order_timestamp", "customer_id", "restaurant_id", "agent_id",
        "delivery_location_id", "total_amount", "tip_amount", "payment_method", "order_status"
    )


@dlt.table(name="gold_fact_order_items", comment="Order-item transaction fact table.")
def gold_fact_order_items():
    return dlt.read("silver_order_items")


@dlt.table(name="gold_kpi_restaurant_daily", comment="Daily revenue and fulfillment KPIs.")
def gold_kpi_restaurant_daily():
    orders = dlt.read("gold_fact_orders")
    restaurants = dlt.read("silver_restaurants")
    locations = dlt.read("silver_locations")
    return (
        orders.join(restaurants, "restaurant_id", "left")
        .join(locations, F.col("location_id") == F.col("delivery_location_id"), "left")
        .groupBy(F.to_date("order_timestamp").alias("order_date"), "restaurant_id", "name", "city", "state")
        .agg(
            F.countDistinct("order_id").alias("order_count"),
            F.sum("total_amount").alias("revenue"),
            F.sum("tip_amount").alias("tips"),
            F.avg("total_amount").alias("average_order_value"),
            F.sum(F.expr("CASE WHEN order_status = 'delivered' THEN 1 ELSE 0 END")).alias("delivered_orders"),
            F.sum(F.expr("CASE WHEN order_status = 'cancelled' THEN 1 ELSE 0 END")).alias("cancelled_orders"),
        )
    )


@dlt.table(name="gold_kpi_popular_items", comment="Menu item demand and sales KPIs.")
def gold_kpi_popular_items():
    items = dlt.read("gold_fact_order_items")
    menu = dlt.read("silver_menu_items")
    return (
        items.join(menu.select("item_id", "category"), "item_id", "left")
        .groupBy("item_id", "item_name", "category")
        .agg(
            F.sum("quantity").alias("units_sold"),
            F.sum("line_amount").alias("item_revenue"),
            F.countDistinct("order_id").alias("order_count"),
        )
    )
