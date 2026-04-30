import logging
import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, current_timestamp, trim, lower, initcap

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
    df = df.dropna(subset=["customer_id", "email"])
    logger.info(f"After dropping nulls: {df.count()} records")
    df = df.dropDuplicates(["customer_id"])
    logger.info(f"After deduplication: {df.count()} records")
    df = df.withColumn("email", lower(trim(col("email"))))
    df = df.withColumn("name", initcap(trim(col("name"))))
    df = df.withColumn("city", initcap(trim(col("city"))))
    df = df.withColumn("state", initcap(trim(col("state"))))
    df = df.withColumn("segment", trim(col("segment")))
    df = df.filter(col("email").contains("@"))
    df = df.withColumn("signup_date", to_date(col("signup_date"), "yyyy-MM-dd"))
    df = df.withColumn("ingested_at", current_timestamp())
    logger.info(f"Final clean record count: {df.count()}")
    return df

def load(df, db_url, db_properties):
    logger.info("Loading customers into PostgreSQL staging...")
    conn = psycopg2.connect(host="postgres", database="airflow", user="airflow", password="airflow")
    cursor = conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS staging.customers CASCADE;")
    conn.commit()
    cursor.close()
    conn.close()
    df.write.jdbc(url=db_url, table="staging.customers", mode="overwrite", properties=db_properties)
    logger.info("Customers loaded successfully!")

def run():
    spark = create_spark_session()
    db_url = "jdbc:postgresql://postgres:5432/airflow"
    db_properties = {"user": "airflow", "password": "airflow", "driver": "org.postgresql.Driver"}
    raw_df = extract(spark, "/opt/airflow/data/customers.csv")
    clean_df = transform(raw_df)
    load(clean_df, db_url, db_properties)
    spark.stop()
    logger.info("Customers ETL complete!")

if __name__ == "__main__":
    run()
