import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, current_timestamp, trim, 
    lower, initcap, regexp_replace
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session():
    return SparkSession.builder \
        .appName("EcommerceCustomersETL") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
        .getOrCreate()

def extract(spark, path):
    logger.info("Extracting customers data from CSV...")
    df = spark.read.csv(path, header=True, inferSchema=True)
    logger.info(f"Extracted {df.count()} raw records")
    return df

def transform(df):
    logger.info("Starting transformation...")

    # Step 1: Drop rows where customer_id or email is null
    df = df.dropna(subset=["customer_id", "email"])
    logger.info(f"After dropping nulls: {df.count()} records")

    # Step 2: Remove duplicate customer_ids (keep first occurrence)
    df = df.dropDuplicates(["customer_id"])
    logger.info(f"After deduplication: {df.count()} records")

    # Step 3: Standardize email to lowercase
    df = df.withColumn("email", lower(trim(col("email"))))

    # Step 4: Standardize name to title case
    df = df.withColumn("name", initcap(trim(col("name"))))

    # Step 5: Standardize city and state to title case
    df = df.withColumn("city", initcap(trim(col("city"))))
    df = df.withColumn("state", initcap(trim(col("state"))))

    # Step 6: Standardize segment to uppercase
    df = df.withColumn("segment", trim(col("segment")))

    # Step 7: Validate email format (basic check)
    df = df.filter(col("email").contains("@"))

    # Step 8: Convert signup_date to proper date type
    df = df.withColumn("signup_date", to_date(col("signup_date"), "yyyy-MM-dd"))

    # Step 9: Add ingestion timestamp
    df = df.withColumn("ingested_at", current_timestamp())

    logger.info(f"Final clean record count: {df.count()}")
    return df

def load(df, db_url, db_properties):
    logger.info("Loading customers into PostgreSQL staging...")
    df.write \
        .jdbc(
            url=db_url,
            table="staging.customers",
            mode="overwrite",
            properties=db_properties
        )
    logger.info("Customers loaded successfully!")

def run():
    spark = create_spark_session()

    db_url = "jdbc:postgresql://postgres:5432/airflow"
    db_properties = {
        "user": "airflow",
        "password": "airflow",
        "driver": "org.postgresql.Driver"
    }

    raw_df = extract(spark, "/opt/airflow/data/customers.csv")
    clean_df = transform(raw_df)
    load(clean_df, db_url, db_properties)

    spark.stop()
    logger.info("Customers ETL complete!")

if __name__ == "__main__":
    run()