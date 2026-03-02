import os
from pyspark.sql import SparkSession

os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] += os.pathsep + "C:\\hadoop\\bin"

def main():
    spark = (
        SparkSession.builder
        .appName("RawToBronzeSales")
        .master("local[*]")
        .config("spark.hadoop.home.dir", "C:\\hadoop")
        .config("spark.hadoop.io.native.lib", "false")
        .getOrCreate()
    )

    raw_path = "data/raw/sales.csv"
    bronze_path = "data/bronze/sales"

    df_raw = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(raw_path)
    )

    print("RAW schema:")
    df_raw.printSchema()
    print(f"RAW rows: {df_raw.count()}")

    (
        df_raw.write
        .mode("overwrite")
        .parquet(bronze_path)
    )

    print("Datos escritos en BRONZE correctamente")
    spark.stop()

if __name__ == "__main__":
    main()