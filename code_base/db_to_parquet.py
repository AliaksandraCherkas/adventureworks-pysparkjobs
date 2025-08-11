import pyspark.sql
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, TimestampType,
    BooleanType, DecimalType, ShortType, ByteType
)
from google.cloud import secretmanager
import google.auth
import time

# --- Configuration ---
GCS_OUTPUT_BUCKET = "gs://bct-base-adventureworks"
SECRET_DB_USER = "username"
SECRET_DB_PASS = "password"
SECRET_DB_NAME = "db_name"


TABLES_TO_INGEST = [
    ("sales", "salesorderheader"),
    ("sales", "salesorderdetail"),
    ("production", "product"),
    ("production", "productsubcategory"), 
    ("production", "productcategory"),    
    ("sales", "customer"),
    ("person", "person"),
    ("sales", "salesterritory"),
]

# --- Explicit Schema Definitions ---
# This version is corrected to match YOUR database schema from the screenshots.
TABLE_SCHEMAS = {
    # This schema now has 25 columns, matching the screenshot
    "sales.salesorderheader": StructType([
        StructField("salesorderid", IntegerType(), False),
        StructField("revisionnumber", ByteType(), False),
        StructField("orderdate", TimestampType(), False),
        StructField("duedate", TimestampType(), False),
        StructField("shipdate", TimestampType(), True),
        StructField("status", ByteType(), False),
        StructField("onlineorderflag", BooleanType(), False),
        StructField("purchaseordernumber", StringType(), True),
        # NOTE: accountnumber IS present in your salesorderheader table, but NOT in your customer table. This is kept here.
        StructField("accountnumber", StringType(), True),
        StructField("customerid", IntegerType(), False),
        StructField("salespersonid", IntegerType(), True),
        StructField("territoryid", IntegerType(), True),
        StructField("billtoaddressid", IntegerType(), False),
        StructField("shiptoaddressid", IntegerType(), False),
        StructField("shipmethodid", IntegerType(), False),
        StructField("creditcardid", IntegerType(), True),
        StructField("creditcardapprovalcode", StringType(), True),
        StructField("currencyrateid", IntegerType(), True),
        StructField("subtotal", DecimalType(19, 4), False),
        StructField("taxamt", DecimalType(19, 4), False),
        StructField("freight", DecimalType(19, 4), False),
        StructField("totaldue", DecimalType(19, 4), False),
        StructField("comment", StringType(), True),
        StructField("rowguid", StringType(), False),
        StructField("modifieddate", TimestampType(), False),
    ]),
    # This schema now has 10 columns, matching the screenshot (linetotal removed).
    "sales.salesorderdetail": StructType([
        StructField("salesorderid", IntegerType(), False),
        StructField("salesorderdetailid", IntegerType(), False),
        StructField("carriertrackingnumber", StringType(), True),
        StructField("orderqty", ShortType(), False),
        StructField("productid", IntegerType(), False),
        StructField("specialofferid", IntegerType(), False),
        StructField("unitprice", DecimalType(19, 4), False),
        StructField("unitpricediscount", DecimalType(19, 4), False),
        StructField("rowguid", StringType(), False),
        StructField("modifieddate", TimestampType(), False),
    ]),
    # This schema now has 6 columns, matching the screenshot (accountnumber removed).
    "sales.customer": StructType([
        StructField("customerid", IntegerType(), False),
        StructField("personid", IntegerType(), True),
        StructField("storeid", IntegerType(), True),
        StructField("territoryid", IntegerType(), True),
        StructField("rowguid", StringType(), False),
        StructField("modifieddate", TimestampType(), False),
    ]),
    "person.person": StructType([
        StructField("businessentityid", IntegerType(), False),
        StructField("persontype", StringType(), False),
        StructField("namestyle", BooleanType(), False),
        StructField("title", StringType(), True),
        StructField("firstname", StringType(), False),
        StructField("middlename", StringType(), True),
        StructField("lastname", StringType(), False),
        StructField("suffix", StringType(), True),
        StructField("emailpromotion", IntegerType(), False),
        StructField("additionalcontactinfo", StringType(), True),
        StructField("demographics", StringType(), True),
        StructField("rowguid", StringType(), False),
        StructField("modifieddate", TimestampType(), False),
    ]),
    "sales.salesterritory": StructType([
        StructField("territoryid", IntegerType(), False),
        StructField("name", StringType(), False),
        StructField("countryregioncode", StringType(), False),
        StructField("group", StringType(), False),
        StructField("salesytd", DecimalType(19, 4), False),
        StructField("saleslastyear", DecimalType(19, 4), False),
        StructField("costytd", DecimalType(19, 4), False),
        StructField("costlastyear", DecimalType(19, 4), False),
        StructField("rowguid", StringType(), False),
        StructField("modifieddate", TimestampType(), False),
    ]),
    "production.product": StructType([
        StructField("productid", IntegerType(), False),
        StructField("name", StringType(), False),
        StructField("productnumber", StringType(), False),
        StructField("makeflag", BooleanType(), False),
        StructField("finishedgoodsflag", BooleanType(), False),
        StructField("color", StringType(), True),
        StructField("safetystocklevel", ShortType(), False),
        StructField("reorderpoint", ShortType(), False),
        StructField("standardcost", DecimalType(19, 4), False),
        StructField("listprice", DecimalType(19, 4), False),
        StructField("size", StringType(), True),
        StructField("sizeunitmeasurecode", StringType(), True),
        StructField("weightunitmeasurecode", StringType(), True),
        StructField("weight", DecimalType(8, 2), True),
        StructField("daystomanufacture", IntegerType(), False),
        StructField("productline", StringType(), True),
        StructField("class", StringType(), True),
        StructField("style", StringType(), True),
        StructField("productsubcategoryid", IntegerType(), True),
        StructField("productmodelid", IntegerType(), True),
        StructField("sellstartdate", TimestampType(), False),
        StructField("sellenddate", TimestampType(), True),
        StructField("discontinueddate", TimestampType(), True),
        StructField("rowguid", StringType(), False),
        StructField("modifieddate", TimestampType(), False),
    ]),
    "production.productsubcategory": StructType([
        StructField("productsubcategoryid", IntegerType(), False),
        StructField("productcategoryid", IntegerType(), False),
        StructField("name", StringType(), False),
        StructField("rowguid", StringType(), False),
        StructField("modifieddate", TimestampType(), False),
    ]),
    "production.productcategory": StructType([
        StructField("productcategoryid", IntegerType(), False),
        StructField("name", StringType(), False),
        StructField("rowguid", StringType(), False),
        StructField("modifieddate", TimestampType(), False),
    ]),
}

# --- Helper Functions (No change needed) ---
def access_secret_version(secret_id: str, project_id: str, version_id: str = "latest") -> str:
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")

def ingest_table(spark, jdbc_url, jdbc_properties, dbtable_name, table_schema, output_path):
    df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", dbtable_name)
        .option("user", jdbc_properties["user"])
        .option("password", jdbc_properties["password"])
        .option("driver", jdbc_properties["driver"])
        .schema(table_schema)
        .load()
    )
    df.write.mode("overwrite").parquet(output_path)

# --- Main Execution Logic (No change needed) ---
def main():
    time.sleep(10)
    try:
        _, project_id = google.auth.default()
        if not project_id:
            raise RuntimeError("Could not determine GCP project ID.")
        db_user = access_secret_version(SECRET_DB_USER, project_id)
        db_pass = access_secret_version(SECRET_DB_PASS, project_id)
        db_name = access_secret_version(SECRET_DB_NAME, project_id)
        jdbc_url = f"jdbc:postgresql://127.0.0.1:5432/{db_name}?sslmode=disable"
        jdbc_properties = {
            "user": db_user,
            "password": db_pass,
            "driver": "org.postgresql.Driver",
        }
        spark = SparkSession.builder.appName("AdventureWorks_Core_Sales_Ingestion").getOrCreate()
        for schema, table_name in TABLES_TO_INGEST:
            dbtable_name = f"{schema}.{table_name}"
            output_path = f"{GCS_OUTPUT_BUCKET}/parquet/{schema}/{table_name}"
            table_schema = TABLE_SCHEMAS[dbtable_name]
            ingest_table(spark, jdbc_url, jdbc_properties, dbtable_name, table_schema, output_path)
        spark.stop()
    except Exception as e:
        raise

if __name__ == "__main__":
    main()