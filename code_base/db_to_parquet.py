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

# Added productcategory and productsubcategory for more detailed product analysis.
TABLES_TO_INGEST = [
    ("sales", "salesorderheader"),
    ("sales", "salesorderdetail"),
    ("production", "product"),
    ("production", "productsubcategory"), # <-- Added
    ("production", "productcategory"),    # <-- Added for completeness
    ("sales", "customer"),
    ("person", "person"),
    ("sales", "salesterritory"),
]

# --- Explicit Schema Definitions ---
TABLE_SCHEMAS = {
    # ... (sales, person schemas remain the same)
    "sales.salesorderheader": StructType([
        StructField("salesorderid", IntegerType(), False),
        StructField("revisionnumber", ByteType(), False),
        StructField("orderdate", TimestampType(), False),
        StructField("duedate", TimestampType(), False),
        StructField("shipdate", TimestampType(), True),
        StructField("status", ByteType(), False),
        StructField("onlineorderflag", BooleanType(), False),
        StructField("salesordernumber", StringType(), False),
        StructField("purchaseordernumber", StringType(), True),
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
    "sales.salesorderdetail": StructType([
        StructField("salesorderid", IntegerType(), False),
        StructField("salesorderdetailid", IntegerType(), False),
        StructField("carriertrackingnumber", StringType(), True),
        StructField("orderqty", ShortType(), False),
        StructField("productid", IntegerType(), False),
        StructField("specialofferid", IntegerType(), False),
        StructField("unitprice", DecimalType(19, 4), False),
        StructField("unitpricediscount", DecimalType(19, 4), False),
        StructField("linetotal", DecimalType(38, 6), False),
        StructField("rowguid", StringType(), False),
        StructField("modifieddate", TimestampType(), False),
    ]),
    "sales.customer": StructType([
        StructField("customerid", IntegerType(), False),
        StructField("personid", IntegerType(), True),
        StructField("storeid", IntegerType(), True),
        StructField("territoryid", IntegerType(), True),
        StructField("accountnumber", StringType(), False),
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
    
    # Schemas for production tables
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
    
    # Schema for the newly added tables
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


# --- Helper Functions ---
def access_secret_version(secret_id: str, project_id: str, version_id: str = "latest") -> str:
    """Retrieves a secret's value from Google Cloud Secret Manager."""
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")

def ingest_table(
    spark: pyspark.sql.SparkSession,
    jdbc_url: str,
    jdbc_properties: dict,
    dbtable_name: str,
    table_schema: pyspark.sql.types.StructType,
    output_path: str,
):
    """Reads a single table from the database and writes it to GCS."""
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


# --- Main Execution Logic ---
def main():
    """Main function to orchestrate the ETL process."""
    time.sleep(20)
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
            
            ingest_table(
                spark, jdbc_url, jdbc_properties, dbtable_name, table_schema, output_path
            )

        spark.stop()

    except Exception as e:
        raise

if __name__ == "__main__":
    main()