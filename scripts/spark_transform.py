"""Read raw JSON from MinIO, cleanse/transform with PySpark, write Parquet back to MinIO."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from scripts.config import (
    MINIO_ACCESS_KEY,
    MINIO_BUCKET,
    MINIO_ENDPOINT,
    MINIO_SECRET_KEY,
)

logger = logging.getLogger(__name__)


def _get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("lakehouse-transform")
        .master("local[*]")
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262")
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ENDPOINT}")
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )


def _raw_path(entity: str, dt: datetime) -> str:
    return f"s3a://{MINIO_BUCKET}/raw/{entity}/year={dt.year}/month={dt.month:02d}/day={dt.day:02d}/{entity}.json"


def _cleaned_path(entity: str, dt: datetime) -> str:
    return f"s3a://{MINIO_BUCKET}/cleaned/{entity}/year={dt.year}/month={dt.month:02d}/day={dt.day:02d}/"


def _transform_products(spark: SparkSession, dt: datetime) -> None:
    df = spark.read.option("multiline", "true").json(_raw_path("products", dt))
    cleaned = (
        df.select(
            F.col("id").cast("int").alias("product_id"),
            F.col("title").alias("product_name"),
            F.col("category"),
            F.coalesce(F.col("brand"), F.lit("Unknown")).alias("brand"),
            F.col("price").cast("double"),
            F.col("discountPercentage").cast("double").alias("discount_pct"),
            F.col("rating").cast("double"),
            F.col("stock").cast("int"),
        )
        .dropDuplicates(["product_id"])
        .withColumn("_extracted_date", F.lit(dt.strftime("%Y-%m-%d")))
    )
    cleaned.write.mode("overwrite").parquet(_cleaned_path("products", dt))
    logger.info("Wrote %d products to cleaned layer", cleaned.count())


def _transform_carts(spark: SparkSession, dt: datetime) -> None:
    df = spark.read.option("multiline", "true").json(_raw_path("carts", dt))

    # Order headers
    orders = (
        df.select(
            F.col("id").cast("int").alias("order_id"),
            F.col("userId").cast("int").alias("user_id"),
            F.col("total").cast("double").alias("order_total"),
            F.col("totalProducts").cast("int").alias("total_products"),
            F.col("totalQuantity").cast("int").alias("total_quantity"),
        )
        .dropDuplicates(["order_id"])
        .withColumn("_extracted_date", F.lit(dt.strftime("%Y-%m-%d")))
    )
    orders.write.mode("overwrite").parquet(_cleaned_path("orders", dt))
    logger.info("Wrote %d orders to cleaned layer", orders.count())

    # Order items (explode products array)
    items = (
        df.select(
            F.col("id").cast("int").alias("order_id"),
            F.explode("products").alias("product"),
        )
        .select(
            "order_id",
            F.col("product.id").cast("int").alias("product_id"),
            F.col("product.title").alias("product_name"),
            F.col("product.price").cast("double").alias("unit_price"),
            F.col("product.quantity").cast("int").alias("quantity"),
            F.col("product.total").cast("double").alias("line_total"),
            F.col("product.discountPercentage").cast("double").alias("discount_pct"),
            F.col("product.discountedTotal").cast("double").alias("discounted_total"),
        )
        .withColumn("_extracted_date", F.lit(dt.strftime("%Y-%m-%d")))
    )
    items.write.mode("overwrite").parquet(_cleaned_path("order_items", dt))
    logger.info("Wrote %d order items to cleaned layer", items.count())


def _transform_users(spark: SparkSession, dt: datetime) -> None:
    df = spark.read.option("multiline", "true").json(_raw_path("users", dt))
    cleaned = (
        df.select(
            F.col("id").cast("int").alias("user_id"),
            F.col("firstName").alias("first_name"),
            F.col("lastName").alias("last_name"),
            F.col("email"),
            F.col("age").cast("int"),
            F.col("gender"),
            F.col("phone"),
            F.col("address.city").alias("city"),
            F.col("address.state").alias("state"),
            F.col("address.country").alias("country"),
            F.col("company.name").alias("company_name"),
            F.col("company.department").alias("department"),
            F.col("company.title").alias("job_title"),
        )
        .dropDuplicates(["user_id"])
        .withColumn("_extracted_date", F.lit(dt.strftime("%Y-%m-%d")))
    )
    cleaned.write.mode("overwrite").parquet(_cleaned_path("users", dt))
    logger.info("Wrote %d users to cleaned layer", cleaned.count())


def run_spark_transform(execution_date: Optional[datetime] = None) -> None:
    """Entry point called by Airflow or manual invocation."""
    dt = execution_date or datetime.now()
    logger.info("Starting Spark transform for %s", dt.date())
    spark = _get_spark()
    try:
        _transform_products(spark, dt)
        _transform_carts(spark, dt)
        _transform_users(spark, dt)
    finally:
        spark.stop()
    logger.info("Spark transform complete for %s", dt.date())


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_spark_transform()
