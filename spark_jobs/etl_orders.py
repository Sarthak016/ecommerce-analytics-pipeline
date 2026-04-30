import logging
import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round as spark_round, to_date, current_timestamp, trim, upper
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
    df = df.dropna(subset=["order_id", "customer_id", "product_id"])
    logger.info(f"After dropping nulls: {df.count()} records")
    df = df.dropDuplicates(["order_id"])
    logger.info(f"After deduplication: {df.count()} records")
    df = df.fillna({"quantity": 1})
    df = df.withColumn("quantity", col("quantity").cast(IntegerType()))
    df = df.withColumn("unit_price", col("unit_price").cast(DoubleType()))
    df = df.withColumn("total_amount", spark_round(col("quantity") * col("unit_price"), 2))
    df = df.withColumn("status", upper(trim(col("status"))))
    df = df.withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd"))
    df = df.withColumn("ingested_at", current_timestamp())
    df = df.filter((col("quantity") > 0) & (col("unit_price") > 0))
    logger.info(f"Final clean record count: {df.count()}")
    return df

def load(df, db_url, db_properties):
    logger.info("Loading orders into PostgreSQL staging...")
    conn = psycopg2.connect(host="postgres", database="airflow", user="airflow", password="airflow")
    cursor = conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS staging.orders CASCADE;")
    conn.commit()
    cursor.close()
    conn.close()
    df.write.jdbc(url=db_url, table="staging.orders", mode="overwrite", properties=db_properties)
    logger.info("Orders loaded successfully!")

def run():
    spark = create_spark_session()
    db_url = "jdbc:postgresql://postgres:5432/airflow"
    db_properties = {"user": "airflow", "password": "airflow", "driver": "org.postgresql.Driver"}
    raw_df = extract(spark, "/opt/airflow/data/orders.csv")
    clean_df = transform(raw_df)
    load(clean_df, db_url, db_properties)
    spark.stop()
    logger.info("Orders ETL complete!")

if __name__ == "__main__":
    run()
