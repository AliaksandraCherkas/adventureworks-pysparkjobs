import logging
import os
import time
from typing import Dict, List

import google.auth
import pyspark.sql
from google.cloud import secretmanager
from pyspark.sql import SparkSession
from pyspark.sql.types import (DoubleType, IntegerType, StringType, StructField,
                               StructType, TimestampType)

# --- Logging Configuration ---
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] {%(levelname)s} - %(message)s'
)
logger = logging.getLogger(__name__)


# --- Configuration from Environment Variables ---
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
GCS_OUTPUT_BUCKET_NAME = os.getenv("GCS_OUTPUT_BUCKET")
SECRET_DB_USER = os.getenv("SECRET_ID_DB_USER")
SECRET_DB_PASS = os.getenv("SECRET_ID_DB_PASS")
SECRET_DB_NAME = os.getenv("SECRET_ID_DB_NAME")


# --- Validate Configuration ---
if not GCP_PROJECT_ID:
    try:
        _, GCP_PROJECT_ID = google.auth.default()
    except google.auth.exceptions.DefaultCredentialsError:
        logger.error("GCP_PROJECT_ID is not set and could not be determined from the environment.")
        raise

if not all([GCS_OUTPUT_BUCKET_NAME, SECRET_DB_USER, SECRET_DB_PASS, SECRET_DB_NAME]):
    raise ValueError("One or more required environment variables are not set.")

GCS_OUTPUT_BUCKET = f"gs://{GCS_OUTPUT_BUCKET_NAME}"


# --- Static Definitions ---

# Define the specific list of tables to ingest.
TABLES_TO_INGEST: List[str] = [
    "person.person",
    "sales.customer",
    "sales.salesterritory",
    "production.product",
    "production.productsubcategory",
    "production.productcategory",
    "sales.salesorderheader",
    "sales.salesorderdetail"
]

# Explicitly define the schemas for each table to ensure type safety and consistency.
schemas: Dict[str, StructType] = {
    "person.person": StructType([
        StructField("businessentityid", IntegerType(), False),
        StructField("firstname", StringType(), True),
        StructField("lastname", StringType(), True),
    ]),
    "sales.customer": StructType([
        StructField("customerid", IntegerType(), False),
        StructField("personid", IntegerType(), True),
    ]),
    "production.product": StructType([
        StructField("productid", IntegerType(), False),
        StructField("name", StringType(), True),
        StructField("productsubcategoryid", IntegerType(), True),
        StructField("standardcost", DoubleType(), True),
        StructField("listprice", DoubleType(), True),
    ]),
    "production.productsubcategory": StructType([
        StructField("productsubcategoryid", IntegerType(), False),
        StructField("productcategoryid", IntegerType(), True),
        StructField("name", StringType(), True),
    ]),
    "production.productcategory": StructType([
        StructField("productcategoryid", IntegerType(), False),
        StructField("name", StringType(), True),
    ]),
    "sales.salesterritory": StructType([
        StructField("territoryid", IntegerType(), False),
        StructField("name", StringType(), True),
        StructField("countryregioncode", StringType(), True),
        StructField("group", StringType(), True),
    ]),
    "sales.salesorderheader": StructType([
        StructField("salesorderid", IntegerType(), False),
        StructField("orderdate", TimestampType(), True),
        StructField("customerid", IntegerType(), True),
        StructField("territoryid", IntegerType(), True),
    ]),
    "sales.salesorderdetail": StructType([
        StructField("salesorderid", IntegerType(), False),
        StructField("salesorderdetailid", IntegerType(), False),
        StructField("productid", IntegerType(), True),
        StructField("orderqty", IntegerType(), True),
        StructField("unitprice", DoubleType(), True),
        StructField("unitpricediscount", DoubleType(), True),
    ])
}


# --- Helper Functions ---

def access_secret_version(secret_id: str, project_id: str, version_id: str = "latest") -> str:
    """Retrieves a secret's value from Google Cloud Secret Manager."""
    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    except Exception as e:
        logger.error(f"Failed to access secret: {secret_id} in project {project_id}. Error: {e}")
        raise

def ingest_table(
    spark: SparkSession,
    jdbc_url: str,
    jdbc_properties: Dict[str, str],
    dbtable_name: str,
    schema: StructType,
    gcs_bucket_path: str,
):
    """Reads a single table from the database using a predefined schema and writes it to GCS."""
    schema_name, table_name = dbtable_name.split('.')
    output_path = f"{gcs_bucket_path}/parquet/{schema_name}/{table_name}"

    logger.info(f"--> Starting ingestion for table: {dbtable_name}")

    try:
        # Read data from JDBC using the provided custom schema.
        # This is more efficient and safer than schema inference.
        df_reader = (
            spark.read.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", dbtable_name)
            .option("user", jdbc_properties["user"])
            .option("password", jdbc_properties["password"])
            .option("driver", jdbc_properties["driver"])
            .option("customSchema", schema.json()) # Enforce the schema on read
        )

        df = df_reader.load()

        # Write data to GCS in Parquet format, overwriting any existing data
        df.write.mode("overwrite").parquet(output_path)
        logger.info(f"✅ Successfully saved {df.count()} rows for {dbtable_name} to: {output_path}")

    except Exception as e:
        logger.error(f"❌ Failed to ingest table {dbtable_name}. Error: {e}")
        raise


# --- Main Execution Logic ---

def main():
    """Main function to orchestrate the database to GCS ingestion process."""
    logger.info("Starting AdventureWorks database ingestion job...")
    time.sleep(20) # Delay to allow Cloud SQL Proxy to initialize

    try:
        logger.info(f"Running in project: {GCP_PROJECT_ID}")
        logger.info(f"Output GCS Bucket: {GCS_OUTPUT_BUCKET}")

        # Retrieve database credentials from Secret Manager
        logger.info("Retrieving database credentials from Secret Manager...")
        db_user = access_secret_version(SECRET_DB_USER, GCP_PROJECT_ID)
        db_pass = access_secret_version(SECRET_DB_PASS, GCP_PROJECT_ID)
        db_name = access_secret_version(SECRET_DB_NAME, GCP_PROJECT_ID)
        logger.info("Credentials retrieved successfully.")

        # JDBC connection details
        jdbc_url = f"jdbc:postgresql://127.0.0.1:5432/{db_name}?sslmode=disable"
        jdbc_properties = {
            "user": db_user,
            "password": db_pass,
            "driver": "org.postgresql.Driver",
        }

        # Initialize Spark Session
        spark = SparkSession.builder.appName("AdventureWorks_DB_Ingestion_Targeted").getOrCreate()

        logger.info(f"Starting ingestion of {len(TABLES_TO_INGEST)} specified tables...")

        # Loop through the defined tables and ingest them
        for table_identifier in TABLES_TO_INGEST:
            table_schema = schemas.get(table_identifier)
            if not table_schema:
                logger.warning(f"No schema defined for table {table_identifier}. Skipping.")
                continue
            
            ingest_table(
                spark, jdbc_url, jdbc_properties, table_identifier, table_schema, GCS_OUTPUT_BUCKET
            )

        logger.info("🎉 All specified tables have been ingested successfully!")

    except Exception as e:
        logger.error(f"❌ Ingestion job failed: {e}", exc_info=True)
        raise
    finally:
        if 'spark' in locals() and not spark._sc.isFinalizing:
            logger.info("Stopping Spark session.")
            spark.stop()

if __name__ == "__main__":
    main()