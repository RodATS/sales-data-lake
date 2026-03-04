from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, trim, upper
from pyspark.sql.functions import count
from pyspark.sql.functions import coalesce

def main():

    spark = (
        SparkSession.builder
        .appName("BronzeToSilverSales")
        .master("local[*]")
        .getOrCreate()
    )

    bronze_path = "data/bronze/sales"
    silver_path = "data/silver/sales"

    # Leer Bronze
    df = spark.read.parquet(bronze_path)

    print("Schema Bronze:")
    df.printSchema()

    # Transformaciones Silver
    df_silver = (
        df
        # eliminar columna técnica
        .drop("Row_ID")

        # convertir fechas
        .withColumn(
            "order_date",
            coalesce(
                to_date(col("Order_Date"), "M/d/yyyy"),
                to_date(col("Order_Date"), "d/M/yyyy")
            )
        )
    
        .withColumn(
            "ship_date",
            coalesce(
                to_date(col("Ship_Date"), "M/d/yyyy"),
                to_date(col("Ship_Date"), "d/M/yyyy")
            )
        )

        # normalizar texto
        .withColumn("customer_name", trim(col("Customer_Name")))
        .withColumn("segment", upper(col("Segment")))

        # manejar postal_code nulo
        .withColumn(
            "postal_code",
            col("Postal_Code").cast("string")
        )

        # renombrar columnas principales
        .withColumnRenamed("Order_ID", "order_id")
        .withColumnRenamed("Customer_ID", "customer_id")
        .withColumnRenamed("Product_ID", "product_id")
        .withColumnRenamed("Category", "category")
        .withColumnRenamed("Sub_Category", "sub_category")
        .withColumnRenamed("Product_Name", "product_name")
        .withColumnRenamed("Sales", "sales")
        .withColumnRenamed("City", "city")
        .withColumnRenamed("State", "state")
        .withColumnRenamed("Region", "region")
        .withColumnRenamed("Country", "country")
    )

    print("Schema Silver:")
    df_silver.printSchema()

    print("Validaciones de calidad...")

    total_rows = df_silver.count()

    null_sales = df_silver.filter(col("sales").isNull()).count()
    negative_sales = df_silver.filter(col("sales") < 0).count()
    null_dates = df_silver.filter(col("order_date").isNull()).count()

    print(f"Total rows: {total_rows}")
    print(f"Null sales: {null_sales}")
    print(f"Negative sales: {negative_sales}")
    print(f"Null order_date: {null_dates}")

    df.select("Order_Date").show(20, False)

    # Escribir Silver particionado por año
    (
        df_silver
        .withColumn("year", col("order_date").substr(1,4))
        .write
        .mode("overwrite")
        .partitionBy("year")
        .parquet(silver_path)
    )

    print("Datos escritos en SILVER correctamente")

    spark.stop()

if __name__ == "__main__":
    main()