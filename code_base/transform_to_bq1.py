import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, to_date, date_format, year, month, dayofweek, dayofmonth
from pyspark.sql.types import (
    IntegerType,
    StringType,
    DoubleType,
    TimestampType,
    StructType,
    StructField,
)

# --- Logging Configuration ---
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# --- Configuration ---
GCP_PROJECT_ID = "adventureworks-project-466602"
GCS_BUCKET = "bct-base-adventureworks"
BQ_DATASET = "adventureworks_dw"
GCS_INPUT_PATH = f"gs://{GCS_BUCKET}/parquet"
GCS_TEMP_PATH = f"gs://{GCS_BUCKET}/temp"

SOURCE_TABLES = [
    "person.person",
    "sales.customer",
    "sales.salesterritory",
    "production.product",
    "production.productsubcategory",
    "production.productcategory",
    "sales.salesorderheader",
    "sales.salesorderdetail"
]

schemas = {
    "person": StructType([
        StructField("businessentityid", IntegerType(), True),
        StructField("firstname", StringType(), True),
        StructField("lastname", StringType(), True),
    ]),
    "customer": StructType([
        StructField("customerid", IntegerType(), True),
        StructField("personid", IntegerType(), True),
    ]),
    "product": StructType([
        StructField("productid", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("productsubcategoryid", IntegerType(), True),
        StructField("standardcost", DoubleType(), True),
        StructField("listprice", DoubleType(), True),
    ]),
    "productsubcategory": StructType([
        StructField("productsubcategoryid", IntegerType(), True),
        StructField("productcategoryid", IntegerType(), True),
        StructField("name", StringType(), True),
    ]),
    "productcategory": StructType([
        StructField("productcategoryid", IntegerType(), True),
        StructField("name", StringType(), True),
    ]),
    "salesterritory": StructType([
        StructField("territoryid", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("countryregioncode", StringType(), True),
        StructField("group", StringType(), True),
    ]),
    "salesorderheader": StructType([
        StructField("salesorderid", IntegerType(), True),
        StructField("orderdate", TimestampType(), True),
        StructField("customerid", IntegerType(), True),
        StructField("territoryid", IntegerType(), True),
    ]),
    "salesorderdetail": StructType([
        StructField("salesorderid", IntegerType(), True),
        StructField("salesorderdetailid", IntegerType(), True),
        StructField("productid", IntegerType(), True),
        StructField("orderqty", IntegerType(), True),
        StructField("unitprice", DoubleType(), True),
        StructField("unitpricediscount", DoubleType(), True),
    ])
}

def read_source_tables(spark, gcs_path, tables):
    logger.info("--> Reading source tables from GCS...")
    dfs = {}
    for table_path in tables:
        schema_name, table_name = table_path.split('.')
        full_path = f"{gcs_path}/{schema_name}/{table_name}"
        schema = schemas.get(table_name)
        if schema:
            df = spark.read.schema(schema).parquet(full_path)
        else:
            logger.warning(f"No schema defined for {table_name}, using inferred schema.")
            df = spark.read.parquet(full_path)
        dfs[table_name] = df
        logger.info(f"    - Loaded {table_path}")
    return dfs

def write_to_bigquery(df, table_name):
    logger.info(f"--> Writing table '{table_name}' to BigQuery...")
    df.write.format("bigquery") \
        .option("table", f"{GCP_PROJECT_ID}:{BQ_DATASET}.{table_name}") \
        .option("temporaryGcsBucket", GCS_BUCKET) \
        .mode("overwrite") \
        .save()
    logger.info(f"    - ✅ Wrote {table_name} to BigQuery.")

def main():
    spark = SparkSession.builder \
        .appName("AdventureWorks_Transformation_to_BQ") \
        .getOrCreate()

    try:
        source_dfs = read_source_tables(spark, GCS_INPUT_PATH, SOURCE_TABLES)

        logger.info("--- Creating Dimension Tables ---")

        dim_customer = source_dfs["customer"].alias("c") \
            .join(source_dfs["person"].alias("p"), col("c.personid") == col("p.businessentityid"), "left") \
            .select(
                col("c.customerid").alias("customer_key"),
                col("p.firstname").alias("first_name"),
                col("p.lastname").alias("last_name")
            )
        write_to_bigquery(dim_customer, "dim_customer")

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
        write_to_bigquery(dim_product, "dim_product")

        dim_territory = source_dfs["salesterritory"] \
            .select(
                col("territoryid").alias("territory_key"),
                col("name").alias("territory_name"),
                col("countryregioncode").alias("country_region_code"),
                col("group").alias("territory_group")
            )
        write_to_bigquery(dim_territory, "dim_territory")

        dim_date = source_dfs["salesorderheader"] \
            .select(to_date(col("orderdate")).alias("date")) \
            .distinct() \
            .select(
                date_format(col("date"), "yyyyMMdd").cast("int").alias("date_key"),
                col("date"),
                year(col("date")).alias("year"),
                month(col("date")).alias("month"),
                dayofmonth(col("date")).alias("day"),
                dayofweek(col("date")).alias("day_of_week")
            )
        write_to_bigquery(dim_date, "dim_date")

        logger.info("--- Creating Fact Tables ---")

        fact_sales_detail = source_dfs["salesorderdetail"].alias("sod") \
            .join(source_dfs["salesorderheader"].alias("soh"), col("sod.salesorderid") == col("soh.salesorderid"), "inner") \
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
        write_to_bigquery(fact_sales_detail, "fact_sales_detail")

        fact_sales_agg = fact_sales_detail \
            .groupBy("date_key", "product_key") \
            .agg(
                _sum("order_quantity").alias("total_quantity_sold"),
                _sum("line_total").alias("total_revenue")
            )
        write_to_bigquery(fact_sales_agg, "fact_sales_agg_daily_product")

        logger.info("🎉 Data Warehouse ETL job finished successfully!")

    except Exception as e:
        logger.error(f"❌ ETL job failed: {e}", exc_info=True)
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
