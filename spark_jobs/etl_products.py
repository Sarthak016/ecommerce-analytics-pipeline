import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, trim,
    initcap, round as spark_round
)
from pyspark.sql.types import DoubleType, IntegerType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session():
    return SparkSession.builder \
        .appName("EcommerceProductsETL") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
        .getOrCreate()

def extract(spark, path):
    logger.info("Extracting products data from CSV...")
    df = spark.read.csv(path, header=True, inferSchema=True)
    logger.info(f"Extracted {df.count()} raw records")
    return df

def transform(df):
    logger.info("Starting transformation...")

    # Step 1: Drop rows where product_id or product_name is null
    df = df.dropna(subset=["product_id", "product_name"])
    logger.info(f"After dropping nulls: {df.count()} records")

    # Step 2: Remove duplicate product_ids
    df = df.dropDuplicates(["product_id"])
    logger.info(f"After deduplication: {df.count()} records")

    # Step 3: Standardize product_name and category to title case
    df = df.withColumn("product_name", initcap(trim(col("product_name"))))
    df = df.withColumn("category", initcap(trim(col("category"))))
    df = df.withColumn("brand", initcap(trim(col("brand"))))

    # Step 4: Cast price columns to correct types
    df = df.withColumn("cost_price", col("cost_price").cast(DoubleType()))
    df = df.withColumn("selling_price", col("selling_price").cast(DoubleType()))
    df = df.withColumn("stock_quantity", col("stock_quantity").cast(IntegerType()))

    # Step 5: Calculate profit margin percentage
    df = df.withColumn(
        "profit_margin_pct",
        spark_round(
            ((col("selling_price") - col("cost_price")) / col("selling_price")) * 100,
            2
        )
    )

    # Step 6: Filter out products with invalid prices
    df = df.filter(
        (col("cost_price") > 0) &
        (col("selling_price") > 0) &
        (col("selling_price") >= col("cost_price"))
    )

    # Step 7: Filter out negative stock
    df = df.filter(col("stock_quantity") >= 0)

    # Step 8: Add ingestion timestamp
    df = df.withColumn("ingested_at", current_timestamp())

    logger.info(f"Final clean record count: {df.count()}")
    return df

def load(df, db_url, db_properties):
    logger.info("Loading products into PostgreSQL staging...")
    df.write \
        .jdbc(
            url=db_url,
            table="staging.products",
            mode="overwrite",
            properties=db_properties
        )
    logger.info("Products loaded successfully!")

def run():
    spark = create_spark_session()

    db_url = "jdbc:postgresql://postgres:5432/airflow"
    db_properties = {
        "user": "airflow",
        "password": "airflow",
        "driver": "org.postgresql.Driver"
    }

    raw_df = extract(spark, "/opt/airflow/data/products.csv")
    clean_df = transform(raw_df)
    load(clean_df, db_url, db_properties)

    spark.stop()
    logger.info("Products ETL complete!")

if __name__ == "__main__":
    run()