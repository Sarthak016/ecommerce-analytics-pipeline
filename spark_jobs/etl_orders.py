import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, round as spark_round,
    to_date, current_timestamp, trim, upper
)
from pyspark.sql.types import IntegerType, DoubleType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session():
    return SparkSession.builder \
        .appName("EcommerceOrdersETL") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
        .getOrCreate()

def extract(spark, path):
    logger.info("Extracting orders data from CSV...")
    df = spark.read.csv(path, header=True, inferSchema=True)
    logger.info(f"Extracted {df.count()} raw records")
    return df

def transform(df):
    logger.info("Starting transformation...")

    # Step 1: Drop rows where critical fields are null
    df = df.dropna(subset=["order_id", "customer_id", "product_id"])
    logger.info(f"After dropping nulls: {df.count()} records")

    # Step 2: Drop duplicate order_ids
    df = df.dropDuplicates(["order_id"])
    logger.info(f"After deduplication: {df.count()} records")

    # Step 3: Fill missing quantity with 1 as default
    df = df.fillna({"quantity": 1})

    # Step 4: Cast columns to correct types
    df = df.withColumn("quantity", col("quantity").cast(IntegerType()))
    df = df.withColumn("unit_price", col("unit_price").cast(DoubleType()))

    # Step 5: Calculate total_amount
    df = df.withColumn(
        "total_amount",
        spark_round(col("quantity") * col("unit_price"), 2)
    )

    # Step 6: Standardize status column to uppercase
    df = df.withColumn("status", upper(trim(col("status"))))

    # Step 7: Convert order_date to proper date type
    df = df.withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd"))

    # Step 8: Add ingestion timestamp
    df = df.withColumn("ingested_at", current_timestamp())

    # Step 9: Filter out invalid records (negative quantity or price)
    df = df.filter((col("quantity") > 0) & (col("unit_price") > 0))
    logger.info(f"Final clean record count: {df.count()}")

    return df

def load(df, db_url, db_properties):
    logger.info("Loading orders into PostgreSQL staging...")
    df.write \
        .jdbc(
            url=db_url,
            table="staging.orders",
            mode="overwrite",
            properties=db_properties
        )
    logger.info("Orders loaded successfully!")

def run():
    spark = create_spark_session()

    db_url = "jdbc:postgresql://postgres:5432/airflow"
    db_properties = {
        "user": "airflow",
        "password": "airflow",
        "driver": "org.postgresql.Driver"
    }

    raw_df = extract(spark, "/opt/airflow/data/orders.csv")
    clean_df = transform(raw_df)
    load(clean_df, db_url, db_properties)

    spark.stop()
    logger.info("Orders ETL complete!")

if __name__ == "__main__":
    run()