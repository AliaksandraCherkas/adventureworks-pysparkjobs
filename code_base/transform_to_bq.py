# transform_to_bq_persist_only.py

from pyspark.sql import SparkSession, StorageLevel
from pyspark.sql.functions import col, sum as _sum, to_date, date_format, year, month, dayofweek, dayofmonth
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, DateType,
    DecimalType, LongType, ShortType
)

# --- Configuration (No changes) ---
GCP_PROJECT_ID = "adventureworks-project-466602"
GCS_BUCKET = "bct-base-adventureworks"
BQ_DATASET = "adventureworks_dw"
GCS_INPUT_PATH = f"gs://{GCS_BUCKET}/parquet"
GCS_TEMP_BUCKET = GCS_BUCKET
SOURCE_TABLES = [
    "person.person", "sales.customer", "sales.salesterritory", "production.product",
    "production.productsubcategory", "production.productcategory",
    "sales.salesorderheader", "sales.salesorderdetail"
]
BQ_SCHEMAS = {
    "dim_customer": StructType([
        StructField("customer_key", IntegerType(), False),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True)
    ]),
    "dim_product": StructType([
        StructField("product_key", IntegerType(), False),
        StructField("product_name", StringType(), True),
        StructField("subcategory_name", StringType(), True),
        StructField("category_name", StringType(), True),
        StructField("standard_cost", DecimalType(19, 4), True),
        StructField("list_price", DecimalType(19, 4), True)
    ]),
    "dim_territory": StructType([
        StructField("territory_key", IntegerType(), False),
        StructField("territory_name", StringType(), True),
        StructField("country_region_code", StringType(), True),
        StructField("territory_group", StringType(), True)
    ]),
    "dim_date": StructType([
        StructField("date_key", IntegerType(), False),
        StructField("date", DateType(), True),
        StructField("year", IntegerType(), True),
        StructField("month", IntegerType(), True),
        StructField("day", IntegerType(), True),
        StructField("day_of_week", IntegerType(), True)
    ]),
    "fact_sales_detail": StructType([
        StructField("sales_order_id", IntegerType(), False),
        StructField("sales_order_detail_id", IntegerType(), False),
        StructField("date_key", IntegerType(), True),
        StructField("customer_key", IntegerType(), False),
        StructField("product_key", IntegerType(), False),
        StructField("territory_key", IntegerType(), True),
        StructField("order_quantity", ShortType(), True),
        StructField("unit_price", DecimalType(19, 4), True),
        StructField("unit_price_discount", DecimalType(19, 4), True),
        StructField("line_total", DecimalType(38, 6), True)
    ]),
    "fact_sales_agg_daily_product": StructType([
        StructField("date_key", IntegerType(), False),
        StructField("product_key", IntegerType(), False),
        StructField("total_quantity_sold", LongType(), True),
        StructField("total_revenue", DecimalType(38, 6), True)
    ])
}

# --- Helper Functions (No changes) ---
def read_source_tables(spark, gcs_path, tables):
    dfs = {}
    for table_path in tables:
        schema, table_name = table_path.split('.')
        full_path = f"{gcs_path}/{schema}/{table_name}"
        dfs[table_name] = spark.read.parquet(full_path)
    return dfs

def write_to_bigquery(df, table_name, schema):
    final_df = df.select([col(field.name) for field in schema.fields])
    final_df.write.format("bigquery") \
      .option("table", f"{GCP_PROJECT_ID}:{BQ_DATASET}.{table_name}") \
      .option("temporaryGcsBucket", GCS_TEMP_BUCKET) \
      .mode("overwrite") \
      .save()

# --- Main ETL Logic ---
def main():
    spark = SparkSession.builder \
        .appName("AdventureWorks_Transformation_PersistOnly") \
        .getOrCreate()
        
    source_dfs = read_source_tables(spark, GCS_INPUT_PATH, SOURCE_TABLES)

    # --- Create and Write Dimension Tables ---
    
    # dim_customer
    dim_customer = source_dfs["customer"].alias("c") \
        .join(source_dfs["person"].alias("p"), col("c.personid") == col("p.businessentityid"), "left") \
        .select(
            col("c.customerid").alias("customer_key"),
            col("p.firstname").alias("first_name"),
            col("p.lastname").alias("last_name")
        )
    write_to_bigquery(dim_customer, "dim_customer", BQ_SCHEMAS["dim_customer"])

    # dim_product
    dim_product = source_dfs["product"].alias("p") \
        .join(source_dfs["productsubcategory"].alias("ps"), col("p.productsubcategoryid") == col("ps.productsubcategoryid"), "left") \
        .join(source_dfs["productcategory"].alias("pc"), col("ps.productcategoryid") == col("pc.productcategoryid"), "left") \
        .select(
            col("p.productid").alias("product_key"),
            col("p.name").alias("product_name"),
            col("ps.name").alias("subcategory_name"),
            col("pc.name").alias("category_name"),
            col("p.standardcost").alias("standard_cost"),
            col("p.listprice").alias("list_price")
        )
    write_to_bigquery(dim_product, "dim_product", BQ_SCHEMAS["dim_product"])
    
    # dim_territory
    dim_territory = source_dfs["salesterritory"].select(
        col("territoryid").alias("territory_key"),
        col("name").alias("territory_name"),
        col("countryregioncode").alias("country_region_code"),
        col("group").alias("territory_group")
    )
    write_to_bigquery(dim_territory, "dim_territory", BQ_SCHEMAS["dim_territory"])

    # OPTIMIZATION: Cache salesorderheader as it's used for dim_date and fact_sales_detail
    sales_order_header_df = source_dfs["salesorderheader"].persist(StorageLevel.MEMORY_AND_DISK)

    # dim_date (reads from cached DataFrame)
    dim_date = sales_order_header_df \
        .select(to_date(col("orderdate")).alias("date")) \
        .distinct() \
        .select(
            date_format(col("date"), "yyyyMMdd").cast("int").alias("date_key"),
            col("date"), year(col("date")).alias("year"), month(col("date")).alias("month"),
            dayofmonth(col("date")).alias("day"), dayofweek(col("date")).alias("day_of_week")
        )
    write_to_bigquery(dim_date, "dim_date", BQ_SCHEMAS["dim_date"])

    # --- Create and Write Fact Tables ---
    
    # fact_sales_detail (reads from cached sales_order_header_df)
    fact_sales_detail = source_dfs["salesorderdetail"].alias("sod") \
        .join(sales_order_header_df.alias("soh"), col("sod.salesorderid") == col("soh.salesorderid"), "inner") \
        .select(
            col("soh.salesorderid").alias("sales_order_id"),
            col("sod.salesorderdetailid").alias("sales_order_detail_id"),
            date_format(to_date(col("soh.orderdate")), "yyyyMMdd").cast("int").alias("date_key"),
            col("soh.customerid").alias("customer_key"),
            col("sod.productid").alias("product_key"),
            col("soh.territoryid").alias("territory_key"),
            col("sod.orderqty").alias("order_quantity"),
            col("sod.unitprice").alias("unit_price"),
            col("sod.unitpricediscount").alias("unit_price_discount"),
            (col("sod.orderqty") * col("sod.unitprice")).alias("line_total")
        )
    
    # OPTIMIZATION: Persist the detailed fact table as it's written AND used for aggregation.
    fact_sales_detail.persist(StorageLevel.MEMORY_AND_DISK)
    
    # Action 1: Write the detailed fact table. This triggers the computation and caching.
    write_to_bigquery(fact_sales_detail, "fact_sales_detail", BQ_SCHEMAS["fact_sales_detail"])

    # fact_sales_agg_daily_product (reads from cached fact_sales_detail)
    fact_sales_agg = fact_sales_detail \
        .groupBy("date_key", "product_key") \
        .agg(
            _sum("order_quantity").alias("total_quantity_sold"),
            _sum("line_total").alias("total_revenue")
        )

    # Action 2: Write the aggregated fact table.
    write_to_bigquery(fact_sales_agg, "fact_sales_agg_daily_product", BQ_SCHEMAS["fact_sales_agg_daily_product"])

    # OPTIMIZATION: Clean up cached data from memory/disk once done.
    sales_order_header_df.unpersist()
    fact_sales_detail.unpersist()

    spark.stop()

if __name__ == "__main__":
    main()