# transform_to_bq.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, to_date, date_format, year, month, dayofweek, dayofmonth

# --- Configuration ---
GCP_PROJECT_ID = "adventureworks-project-466602"
GCS_BUCKET = "bct-base-adventureworks"
BQ_DATASET = "adventureworks_dw" # The new Data Warehouse dataset we will create

# GCS path where the raw Parquet files are stored
GCS_INPUT_PATH = f"gs://{GCS_BUCKET}/parquet"
# GCS path for temporary files used by the BigQuery connector
GCS_TEMP_PATH = f"gs://{GCS_BUCKET}/temp"

# List of source tables needed for our transformations
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

# --- Helper Functions ---
def read_source_tables(spark, gcs_path, tables):
    """Reads all necessary source tables from GCS Parquet into Spark DataFrames."""
    print("--> Reading source tables from GCS...")
    dfs = {}
    for table_path in tables:
        schema, table_name = table_path.split('.')
        full_path = f"{gcs_path}/{schema}/{table_name}"
        dfs[table_name] = spark.read.parquet(full_path)
        print(f"    - Read {table_path}")
    return dfs

def write_to_bigquery(df, table_name):
    """Writes a DataFrame to a BigQuery table."""
    print(f"--> Writing table '{table_name}' to BigQuery dataset '{BQ_DATASET}'...")
    df.write.format("bigquery") \
      .option("table", f"{GCP_PROJECT_ID}:{BQ_DATASET}.{table_name}") \
      .option("temporaryGcsBucket", GCS_BUCKET) \
      .mode("overwrite") \
      .save()
    print(f"    - ✅ Success: Wrote {table_name} to BigQuery.")

# --- Main ETL Logic ---
def main():
    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("AdventureWorks_Transformation_to_BQ") \
        .getOrCreate()
        
    # 1. Read all source data from GCS
    source_dfs = read_source_tables(spark, GCS_INPUT_PATH, SOURCE_TABLES)

    # 2. Create Dimension Tables
    print("\n--- Starting Dimension Table Creation ---")

    # dim_customer
    dim_customer = source_dfs["customer"].alias("c") \
        .join(source_dfs["person"].alias("p"), col("c.personid") == col("p.businessentityid"), "left") \
        .select(
            col("c.customerid").alias("customer_key"),
            col("p.firstname").alias("first_name"),
            col("p.lastname").alias("last_name")
        )
    write_to_bigquery(dim_customer, "dim_customer")

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
    write_to_bigquery(dim_product, "dim_product")
    
    # dim_territory
    dim_territory = source_dfs["salesterritory"] \
        .select(
            col("territoryid").alias("territory_key"),
            col("name").alias("territory_name"),
            col("countryregioncode").alias("country_region_code"),
            col("group").alias("territory_group")
        )
    write_to_bigquery(dim_territory, "dim_territory")

    # dim_date (dynamically created from order dates)
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

    # 3. Create Detailed Fact Table
    print("\n--- Starting Fact Table Creation ---")

    # fact_sales_detail
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
            # Calculate the line_total since it doesn't exist in the source
            (col("sod.orderqty") * col("sod.unitprice")).alias("line_total")
        )
    write_to_bigquery(fact_sales_detail, "fact_sales_detail")

    # 4. Create Aggregated Fact Table
    print("\n--- Starting Aggregated Fact Table Creation ---")
    
    # fact_sales_agg_daily_product
    fact_sales_agg = fact_sales_detail \
        .groupBy("date_key", "product_key") \
        .agg(
            _sum("order_quantity").alias("total_quantity_sold"),
            _sum("line_total").alias("total_revenue")
        )
    write_to_bigquery(fact_sales_agg, "fact_sales_agg_daily_product")

    print("\n🎉 Data Warehouse ETL job finished successfully!")
    spark.stop()

if __name__ == "__main__":
    main()