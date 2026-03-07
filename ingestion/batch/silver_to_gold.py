import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, year, month
from utils.config_loader import load_config


config = load_config()

silver_path = config["paths"]["silver_data"]
gold_path = config["paths"]["gold_data"]


def main():

    spark = (
        SparkSession.builder
        .appName("SilverToGoldSales")
        .master("local[*]")
        .getOrCreate()
    )

    

    df = spark.read.parquet(silver_path)

    print("Schema Silver:")
    df.printSchema()

    # --------------------------
    # 1️⃣ Ventas por región
    # --------------------------

    sales_by_region = (
        df.groupBy("region")
        .agg(sum("sales").alias("total_sales"))
        .orderBy(col("total_sales").desc())
    )

    sales_by_region.write.mode("overwrite").parquet(
        f"{gold_path}/sales_by_region"
    )

    print("Tabla GOLD creada: sales_by_region")

    # --------------------------
    # 2️⃣ Ventas por categoría
    # --------------------------

    sales_by_category = (
        df.groupBy("category")
        .agg(sum("sales").alias("total_sales"))
        .orderBy(col("total_sales").desc())
    )

    sales_by_category.write.mode("overwrite").parquet(
        f"{gold_path}/sales_by_category"
    )

    print("Tabla GOLD creada: sales_by_category")

    # --------------------------
    # 3️⃣ Ventas por mes
    # --------------------------

    sales_by_month = (
        df.withColumn("year", year("order_date"))
        .withColumn("month", month("order_date"))
        .groupBy("year", "month")
        .agg(sum("sales").alias("total_sales"))
        .orderBy("year", "month")
    )

    sales_by_month.write.mode("overwrite").parquet(
        f"{gold_path}/sales_by_month"
    )

    print("Tabla GOLD creada: sales_by_month")

    spark.stop()


if __name__ == "__main__":
    main()