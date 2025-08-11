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
# This version is corrected based on the Dataproc error log.
TABLE_SCHEMAS = {
    "sales.salesorderheader": StructType([
        # NOTE: All columns are now nullable (True) to match the actual schema from the DB.
        # This is the safest approach as NOT NULL constraints are not always propagated.
        StructField("salesorderid", IntegerType(), True),
        # FIX 1: revisionnumber is SMALLINT -> ShortType
        StructField("revisionnumber", ShortType(), True),
        StructField("orderdate", TimestampType(), True),
        StructField("duedate", TimestampType(), True),
        StructField("shipdate", TimestampType(), True),
        # FIX 2: status is SMALLINT -> ShortType
        StructField("status", ShortType(), True),
        StructField("onlineorderflag", BooleanType(), True),
        StructField("purchaseordernumber", StringType(), True),
        StructField("accountnumber", StringType(), True),
        StructField("customerid", IntegerType(), True),
        StructField("salespersonid", IntegerType(), True),
        StructField("territoryid", IntegerType(), True),
        StructField("billtoaddressid", IntegerType(), True),
        StructField("shiptoaddressid", IntegerType(), True),
        StructField("shipmethodid", IntegerType(), True),
        StructField("creditcardid", IntegerType(), True),
        StructField("creditcardapprovalcode", StringType(), True),
        StructField("currencyrateid", IntegerType(), True),
        # FIX 3: All decimal types updated to match the actual precision (38, 18)
        StructField("subtotal", DecimalType(38, 18), True),
        StructField("taxamt", DecimalType(38, 18), True),
        StructField("freight", DecimalType(38, 18), True),
        StructField("totaldue", DecimalType(38, 18), True),
        StructField("comment", StringType(), True),
        StructField("rowguid", StringType(), True),
        StructField("modifieddate", TimestampType(), True),
    ]),
    # Assuming similar corrections are needed for other tables, making them all nullable for safety.
    "sales.salesorderdetail": StructType([
        StructField("salesorderid", IntegerType(), True),
        StructField("salesorderdetailid", IntegerType(), True),
        StructField("carriertrackingnumber", StringType(), True),
        StructField("orderqty", ShortType(), True),
        StructField("productid", IntegerType(), True),
        StructField("specialofferid", IntegerType(), True),
        StructField("unitprice", DecimalType(38, 18), True),
        StructField("unitpricediscount", DecimalType(38, 18), True),
        StructField("rowguid", StringType(), True),
        StructField("modifieddate", TimestampType(), True),
    ]),
    "sales.customer": StructType([
        StructField("customerid", IntegerType(), True),
        StructField("personid", IntegerType(), True),
        StructField("storeid", IntegerType(), True),
        StructField("territoryid", IntegerType(), True),
        StructField("rowguid", StringType(), True),
        StructField("modifieddate", TimestampType(), True),
    ]),
    "person.person": StructType([
        StructField("businessentityid", IntegerType(), True),
        StructField("persontype", StringType(), True),
        StructField("namestyle", BooleanType(), True),
        StructField("title", StringType(), True),
        StructField("firstname", StringType(), True),
        StructField("middlename", StringType(), True),
        StructField("lastname", StringType(), True),
        StructField("suffix", StringType(), True),
        StructField("emailpromotion", IntegerType(), True),
        StructField("additionalcontactinfo", StringType(), True),
        StructField("demographics", StringType(), True),
        StructField("rowguid", StringType(), True),
        StructField("modifieddate", TimestampType(), True),
    ]),
    "sales.salesterritory": StructType([
        StructField("territoryid", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("countryregioncode", StringType(), True),
        StructField("group", StringType(), True),
        StructField("salesytd", DecimalType(38, 18), True),
        StructField("saleslastyear", DecimalType(38, 18), True),
        StructField("costytd", DecimalType(38, 18), True),
        StructField("costlastyear", DecimalType(38, 18), True),
        StructField("rowguid", StringType(), True),
        StructField("modifieddate", TimestampType(), True),
    ]),
    "production.product": StructType([
        StructField("productid", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("productnumber", StringType(), True),
        StructField("makeflag", BooleanType(), True),
        StructField("finishedgoodsflag", BooleanType(), True),
        StructField("color", StringType(), True),
        StructField("safetystocklevel", ShortType(), True),
        StructField("reorderpoint", ShortType(), True),
        StructField("standardcost", DecimalType(38, 18), True),
        StructField("listprice", DecimalType(38, 18), True),
        StructField("size", StringType(), True),
        StructField("sizeunitmeasurecode", StringType(), True),
        StructField("weightunitmeasurecode", StringType(), True),
        StructField("weight", DecimalType(8, 2), True),
        StructField("daystomanufacture", IntegerType(), True),
        StructField("productline", StringType(), True),
        StructField("class", StringType(), True),
        StructField("style", StringType(), True),
        StructField("productsubcategoryid", IntegerType(), True),
        StructField("productmodelid", IntegerType(), True),
        StructField("sellstartdate", TimestampType(), True),
        StructField("sellenddate", TimestampType(), True),
        StructField("discontinueddate", TimestampType(), True),
        StructField("rowguid", StringType(), True),
        StructField("modifieddate", TimestampType(), True),
    ]),
    "production.productsubcategory": StructType([
        StructField("productsubcategoryid", IntegerType(), True),
        StructField("productcategoryid", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("rowguid", StringType(), True),
        StructField("modifieddate", TimestampType(), True),
    ]),
    "production.productcategory": StructType([
        StructField("productcategoryid", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("rowguid", StringType(), True),
        StructField("modifieddate", TimestampType(), True),
    ]),
}

# --- Helper Functions (No change) ---
# ... (rest of your script is fine) ...
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