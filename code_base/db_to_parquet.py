# main_ingest.py

import pyspark.sql
from pyspark.sql import SparkSession
from google.cloud import secretmanager
import google.auth
import time

# --- Configuration ---

# The GCS bucket where the Parquet files will be stored.
GCS_OUTPUT_BUCKET = "gs://bct-base-adventureworks"

# The names of the secrets stored in Google Cloud Secret Manager.
SECRET_DB_USER = "username"
SECRET_DB_PASS = "password"
SECRET_DB_NAME = "db_name"

# List of all tables to be ingested, formatted as (schema, table_name).
TABLES_TO_INGEST = [
    ("humanresources", "department"),
    ("humanresources", "employee"),
    ("humanresources", "employeedepartmenthistory"),
    ("humanresources", "employeepayhistory"),
    ("humanresources", "jobcandidate"),
    ("humanresources", "shift"),
    ("person", "address"),
    ("person", "addresstype"),
    ("person", "businessentity"),
    ("person", "businessentityaddress"),
    ("person", "businessentitycontact"),
    ("person", "contacttype"),
    ("person", "countryregion"),
    ("person", "emailaddress"),
    ("person", "password"),
    ("person", "person"),
    ("person", "personphone"),
    ("person", "phonenumbertype"),
    ("person", "stateprovince"),
    ("production", "billofmaterials"),
    ("production", "culture"),
    ("production", "document"),
    ("production", "illustration"),
    ("production", "location"),
    ("production", "product"),
    ("production", "productcategory"),
    ("production", "productcosthistory"),
    ("production", "productdescription"),
    ("production", "productdocument"),
    ("production", "productinventory"),
    ("production", "productlistpricehistory"),
    ("production", "productmodel"),
    ("production", "productmodelillustration"),
    ("production", "productmodelproductdescriptionculture"),
    ("production", "productphoto"),
    ("production", "productproductphoto"),
    ("production", "productreview"),
    ("production", "productsubcategory"),
    ("production", "scrapreason"),
    ("production", "transactionhistory"),
    ("production", "transactionhistoryarchive"),
    ("production", "unitmeasure"),
    ("production", "workorder"),
    ("production", "workorderrouting"),
    ("purchasing", "productvendor"),
    ("purchasing", "purchaseorderdetail"),
    ("purchasing", "purchaseorderheader"),
    ("purchasing", "shipmethod"),
    ("purchasing", "vendor"),
    ("sales", "countryregioncurrency"),
    ("sales", "creditcard"),
    ("sales", "currency"),
    ("sales", "currencyrate"),
    ("sales", "customer"),
    ("sales", "personcreditcard"),
    ("sales", "salesorderdetail"),
    ("sales", "salesorderheader"),
    ("sales", "salesorderheadersalesreason"),
    ("sales", "salesperson"),
    ("sales", "salespersonquotahistory"),
    ("sales", "salesreason"),
    ("sales", "salestaxrate"),
    ("sales", "salesterritory"),
    ("sales", "salesterritoryhistory"),
    ("sales", "shoppingcartitem"),
    ("sales", "specialoffer"),
    ("sales", "specialofferproduct"),
    ("sales", "store"),
]

# --- Helper Functions ---

def access_secret_version(secret_id: str, project_id: str, version_id: str = "latest") -> str:
    """
    Retrieves a secret's value from Google Cloud Secret Manager.
    """
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")

def ingest_table(
    spark: pyspark.sql.SparkSession,
    jdbc_url: str,
    jdbc_properties: dict,
    schema: str,
    table: str,
    gcs_bucket: str,
):
    """
    Reads a single table from the database and writes it to GCS as Parquet.
    """
    dbtable_name = f"{schema}.{table}"
    output_path = f"{gcs_bucket}/parquet/{schema}/{table}"

    print(f"--> Starting ingestion for table: {dbtable_name}")

    # Read data from the database via JDBC
    df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", dbtable_name)
        .option("user", jdbc_properties["user"])
        .option("password", jdbc_properties["password"])
        .option("driver", jdbc_properties["driver"])
        .load()
    )

    # Write data to GCS in Parquet format
    df.write.mode("overwrite").parquet(output_path)
    print(f"✅ Data for {dbtable_name} saved to: {output_path}")


# --- Main Execution Logic ---

def main():
    """
    Main function to orchestrate the ETL process.
    """
    time.sleep(20)
    try:
        # Automatically detect project ID from the environment
        _, project_id = google.auth.default()
        if not project_id:
            raise RuntimeError("Could not determine GCP project ID.")
        print(f"Running in project: {project_id}")

        # Retrieve database credentials from Secret Manager
        db_user = access_secret_version(SECRET_DB_USER, project_id)
        db_pass = access_secret_version(SECRET_DB_PASS, project_id)
        db_name = access_secret_version(SECRET_DB_NAME, project_id)

        # The Cloud SQL Auth Proxy runs on each Dataproc node,
        # so we connect to localhost (127.0.0.1).
        # The proxy securely handles the connection to the actual Cloud SQL instance.
        # jdbc_url = f"jdbc:postgresql://127.0.0.1:5432/{db_name}"
        # NEW, CORRECTED LINE
        jdbc_url = f"jdbc:postgresql://127.0.0.1:5432/{db_name}?sslmode=disable"
        jdbc_properties = {
            "user": db_user,
            "password": db_pass,
            "driver": "org.postgresql.Driver",
        }

        # Initialize Spark Session
        spark = SparkSession.builder.appName("AdventureWorks_Ingestion").getOrCreate()

        print(f"Starting ingestion of {len(TABLES_TO_INGEST)} tables...")

        # Loop through all tables and ingest them
        for schema, table_name in TABLES_TO_INGEST:
            ingest_table(
                spark, jdbc_url, jdbc_properties, schema, table_name, GCS_OUTPUT_BUCKET
            )

        print("\n🎉 All tables have been ingested successfully!")

        # Stop the Spark session
        spark.stop()

    except Exception as e:
        print(f"An error occurred: {e}")
        # Consider adding more robust error handling/logging here
        raise

if __name__ == "__main__":
    main()